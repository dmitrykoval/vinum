from itertools import chain

from typing import (
    Iterable,
    Any,
    Tuple,
    Optional,
)

import numpy as np

import pyarrow as pa

import vinum_lib

from vinum._typing import AnyArrayLike, OperatorArgument
from vinum.arrow.arrow_table import ArrowTable
from vinum.arrow.record_batch import RecordBatch
from vinum.errors import OperatorError
from vinum.parser.query import Column, SortOrder
from vinum.core.base import (
    Operator,
    VectorizedExpression
)
from vinum.util.util import is_array_type


class ProjectOperator(Operator):
    """
    Project operator.

    Parameters
    ----------
    arguments : Iterable[OperatorArgument]
        Columns or Expressions.
    parent_operator : Operator
        Parent operator.
    col_names : Optional[Iterable[str]]
        Column names to use in the output batch.
    keep_input_table: bool
        Keep input record batch or return only expressions.
    """
    def __init__(self,
                 arguments: Iterable[OperatorArgument],
                 parent_operator: Operator,
                 col_names: Optional[Iterable[str]] = None,
                 keep_input_table=False) -> None:
        super().__init__(parent_operator, arguments)
        self._col_names = col_names
        self._keep_input_table = keep_input_table

    def _kernel(self, batch: RecordBatch, arguments: Tuple) -> RecordBatch:
        arrays = self._repeat_scalars(arguments)
        self._ensure_equal_arrays_size(arrays)

        col_names = self._get_column_names()

        if self._keep_input_table:
            return RecordBatch.from_arrays(
                tuple(chain(batch.columns, arrays)),
                tuple(chain(batch.column_names, col_names))
            )
        else:
            return RecordBatch.from_arrays(arrays, col_names)

    def _get_column_names(self) -> Iterable[str]:
        if self._col_names:
            return self._col_names

        return tuple(
            arg.get_column_name()
            for arg
            in self._arguments
        )

    @staticmethod
    def _repeat_scalars(arguments: Iterable) -> Iterable:
        arr_lens = list(len(arr) for arr in arguments if is_array_type(arr))
        col_max_len = max(arr_lens) if arr_lens else 1

        arrays = []
        for arg in arguments:
            if is_array_type(arg):
                arrays.append(arg)
            else:
                arrays.append(np.repeat(arg, col_max_len))
        return tuple(arrays)

    @staticmethod
    def _ensure_equal_arrays_size(arrays: Iterable[AnyArrayLike]) -> None:
        size = None
        size_index = None
        for idx, array in enumerate(arrays):
            if size is None:
                size = len(array)
                size_index = idx
            else:
                if len(array) != size:
                    err_msg = (
                        f'Select expressions have unequal sizes. '
                        f'This is not permitted. '
                        f'Expression one index: {size_index}, size: {size}. '
                        f'Expression two index: {idx}, size: {len(array)}.'
                    )
                    raise OperatorError(err_msg)


class FilterOperator(Operator):
    """
    Boolean Filter Operator.

    Apply boolean filter to the input RecordBatch.
    """
    def __init__(self,
                 predicate: VectorizedExpression,
                 parent_operator: Operator) -> None:
        super().__init__(parent_operator, [predicate])

    def _kernel(self,
                batch: RecordBatch,
                arguments: Tuple[AnyArrayLike]) -> RecordBatch:
        assert len(arguments) == 1
        return batch.filter(arguments[0])


class SortOperator(Operator):
    """
    Sort Operator.

    Sort the table by a list of provided columns or expressions.

    Parameters
    ----------
    arguments : Tuple[OperatorBaseType, ...]
        Columns or expressions to sort by.
    sort_order : Tuple[SortOrder, ...]
        List of SortOrders (ASC, DESC) corresponding to columns
        in the `arguments` parameter.
    """
    def __init__(self,
                 arguments: Tuple[OperatorArgument, ...],
                 sort_order: Tuple[SortOrder, ...],
                 parent_operator: Operator) -> None:
        super().__init__(parent_operator, arguments)

        assert len(arguments)
        assert len(sort_order)

        self._col_names = [c.get_column_name() for c in arguments]
        self._sort_order = [
            vinum_lib.SortOrder.ASC if o == SortOrder.ASC
            else vinum_lib.SortOrder.DESC
            for o in sort_order
        ]
        self._expressions = []

        self._sort_op = vinum_lib.Sort(self._col_names, self._sort_order)

    def next(self) -> Iterable[RecordBatch]:
        for batch in self._parent_operator.next():
            self._process_arguments(self._arguments, batch=batch)
            col_names = tuple(i[0] for i in self._expressions)
            exprs = tuple(i[1] for i in self._expressions)
            batch = RecordBatch.from_arrays(
                tuple(chain(batch.columns, exprs)),
                tuple(chain(batch.column_names, col_names))
            )

            # Remove, once sorting by boolean columns is supported by Arrow
            self._verify_bool_columns(batch.get_schema())

            self._sort_op.next(batch.get_batch())
            self._expressions.clear()

        yield RecordBatch(self._sort_op.sorted())

        del self._sort_op

    def _get_column(self, column: Column, batch: RecordBatch) -> pa.Array:
        return None

    def _eval_expression(self, expression: 'VectorizedExpression',
                         batch: RecordBatch) -> Any:
        result = super()._eval_expression(expression, batch)
        self._expressions.append(
            (expression.get_column_name(),
             result)
        )
        return result

    def _verify_bool_columns(self, schema: pa.Schema):
        """
        This a temporary check needed because Arrow, currently (3.0.0) does not
        support sorting by Boolean columns. Remove once bools are supported.
        """
        for name, field_type in zip(schema.names, schema.types):
            if name in self._col_names and pa.types.is_boolean(field_type):
                raise OperatorError(
                    "Sorting by boolean column is not supported yet. "
                    "Please use float(bool_column) as a workaround."
                )


class SliceOperator(Operator):
    """
    Slice Operator.

    Return a slice [offset: offset + limit].

    Parameters
    ----------
    limit : int
        Number of rows to retain.
    limit : int
        Number of rows to retain.
    """
    def __init__(self,
                 limit: int,
                 offset: int,
                 parent_operator: Operator) -> None:
        super().__init__(parent_operator)

        self._limit = limit
        self._offset = offset

        self._num_returned = 0
        self._curr_offset = 0

    def next(self) -> ArrowTable:
        for batch in self._parent_operator.next():
            if self._num_returned >= self._limit:
                break
            if self._offset >= self._curr_offset + batch.num_rows:
                self._curr_offset += batch.num_rows
                continue

            offset = max(self._offset - self._curr_offset, 0)
            slice_size = min(self._limit - self._num_returned,
                             batch.num_rows - offset)

            if slice_size < batch.num_rows:
                yield batch.slice(slice_size, offset)
            else:
                yield batch

            self._num_returned += slice_size
            self._curr_offset += offset + slice_size


class TableReaderOperator(Operator):
    def __init__(self,
                 table: ArrowTable) -> None:
        super().__init__(None)
        self._reader: vinum_lib.TableBatchReader = vinum_lib.TableBatchReader(
            table.get_table())

        from vinum import get_batch_size
        self._reader.set_batch_size(get_batch_size())

    def next(self) -> RecordBatch:
        while True:
            batch = self._reader.next()
            if batch is None:
                break
            yield RecordBatch(batch)


class FileReaderOperator(Operator):
    def __init__(self, reader: pa.csv.CSVStreamingReader) -> None:
        super().__init__(None)
        self._reader: reader = reader

    def next(self) -> RecordBatch:
        while True:
            try:
                batch = self._reader.read_next_batch()
            except StopIteration:
                break
            yield RecordBatch(batch)


class EmptyTableReaderOperator(Operator):
    def __init__(self) -> None:
        super().__init__(None)

    def next(self) -> RecordBatch:
        yield RecordBatch.empty_batch()


class MaterializeTableOperator(Operator):
    def next(self) -> ArrowTable:
        batches = []
        for batch in self._parent_operator.next():
            batches.append(batch.get_batch())
        yield ArrowTable.from_batches(batches)
