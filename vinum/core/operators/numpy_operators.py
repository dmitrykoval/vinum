import re
from collections import defaultdict
from typing import (
    Iterable,
    Union,
    Set,
    Any,
    Tuple,
    Dict,
    no_type_check,
)

import numpy as np
import pyarrow as pa

from vinum._typing import AnyArrayLike
from vinum.arrow.arrow_table import ArrowTable
from vinum.errors import OperatorError
from vinum.parser.query import Column, Literal, SortOrder
from vinum.core.operators.generic_operators import (
    Operator,
    AggregateOperator,
    AbstractCombineGroupByGroupsOperator,
    OperatorBaseType
)
from vinum.util.util import ensure_is_array, is_column, is_operator


class NumpyOperator(Operator):
    """
    Base Numpy Operator class.

    Base class for all Operators using numpy arrays as columns.
    """
    def _resolve_column(self, column: Column) -> np.ndarray:
        return self.table.get_np_column(column)


class AggregateNumpyOperator(AggregateOperator, NumpyOperator):
    """
    Base Numpy Aggregate Operator.

    Base class for all numpy based aggregate functions.
    """
    pass


class BooleanFilterOperator(NumpyOperator):
    """
    Boolean Filter Operator.

    Apply boolean filter to the shared Arrow table in-place.
    """
    def _run(self) -> Any:
        self._apply_bitmask(*self._processed_args)

    def _apply_bitmask(self, bitmask: AnyArrayLike) -> None:
        # Do nothing if there's no bitmask
        if bitmask is None:
            return
        self.table.apply_bitmask(bitmask)


class ComputeSelectExpressionsOperator(NumpyOperator):
    """
    Compute Expressions Operator.

    Resolve all the operator arguments: evaluate expressions,
    pull in required columns.
    """
    def _run(self) -> Any:
        select_columns = self._wrap_scalars_in_iterable(self._processed_args)
        self._ensure_equal_arrays_size(select_columns)
        return select_columns

    @staticmethod
    def _wrap_scalars_in_iterable(select_columns: Tuple) -> Tuple:
        return tuple(ensure_is_array(col) for col in select_columns)

    @staticmethod
    def _ensure_equal_arrays_size(arrays: Tuple) -> None:
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

    def _resolve_value(self, value: Literal) -> Tuple:
        return tuple((super()._resolve_value(value),))

    def _resolve_column(self, column: Column) -> pa.ChunkedArray:
        return self.table.get_pa_column(column)


class DistinctOperator(NumpyOperator):
    """
    Distinct Operator.

    Implements SQL's SELECT DISTINCT modifier.
    Makes sure all the rows are unique for all the columns, passed as
    arguments.
    """
    def _run(self) -> Any:
        return self._distinct(*self._processed_args)

    @staticmethod
    def _distinct(select_expressions: Tuple) -> Tuple:
        distinct_cols = []

        # TODO this creates an extra copy
        distinct_ar = np.unique(np.array(select_expressions), axis=1)

        for col_idx in range(distinct_ar.shape[0]):
            distinct_cols.append(distinct_ar[col_idx, :])

        return tuple(distinct_cols)


class UpdateTableOperator(NumpyOperator):
    """
    Update Table Operator.

    Replace ArrowTable columns with the ones provided in the args.
    """
    def __init__(self,
                 arguments: Iterable[OperatorBaseType],
                 column_names: Tuple[str, ...],
                 table: ArrowTable) -> None:
        super().__init__(arguments, table)
        self.column_names = column_names

    def _run(self) -> None:
        self._update_table(*self._processed_args)

    def _update_table(self, columns) -> None:
        self.table.replace_columns(columns, self.column_names)


class RetainTableColumnsOperator(NumpyOperator):
    """
    Retain columns operator.

    Retain only provided columns in the table and drop the rest.
    """
    def __init__(self,
                 retain_first_columns: int,
                 new_column_names: Tuple[str, ...],
                 table: ArrowTable) -> None:
        super().__init__([], table)
        self._retain_first_columns = retain_first_columns
        self._new_column_names = new_column_names

    def _run(self) -> None:
        self.table.retain_columns(self._retain_first_columns,
                                  self._new_column_names)


class LikeOperator(NumpyOperator):
    """
    SQL LIKE operator.

    Return boolean mask based on string pattern.
    Supports:
        '_' for a single character wildcard,
        '%' for zero or more characters wildcard.

    Parameters
    ----------
    arguments : Tuple[Union[Column, Operator], Literal]
        First argument is column or expression to perform LIKE operation on.
        Second argument is the string pattern.
    invert : bool
        Set to True for inverting the result - 'NOT LIKE'.
    table : ArrowTable
        Data table.
    """
    def __init__(self,
                 arguments: Tuple[Union[Column, Operator], Literal],
                 invert: bool,
                 table: ArrowTable) -> None:
        super().__init__(arguments, table)
        self.invert = invert

    def _run(self) -> Iterable[bool]:
        column, pattern = self._processed_args
        return self._like(column, pattern)

    @staticmethod
    def _compile_regex_pattern(pattern: str) -> re.Pattern:
        assert pattern is not None
        pattern = pattern.replace('_', '.')
        pattern = pattern.replace('%', '.*')
        pattern = f'^{pattern}$'
        return re.compile(pattern)

    def _like(self, column: AnyArrayLike, pattern: str) -> Iterable[bool]:
        def re_lambda(val):
            return bool(compiled_pattern.match(val)) != self.invert

        compiled_pattern = self._compile_regex_pattern(pattern)
        re_func = np.vectorize(re_lambda)

        return re_func(column)


class GroupByArguments:
    """
    Group By Arguments.

    Encapsulates arguments for GROUP BY operation.

    Parameters
    ----------
    group_by :  Tuple['OperatorBaseType', ...]
        List of arguments for SQL GROUP BY clause.
    """
    def __init__(self, group_by: Tuple['OperatorBaseType', ...]) -> None:
        self._group_by: Tuple['OperatorBaseType', ...] = group_by

        self._group_by_col_names: Tuple[str, ...] = tuple()
        self._group_by_expr_names: Tuple[str, ...] = tuple()

    def get_group_by_args(self) -> Tuple['OperatorBaseType', ...]:
        return self._group_by

    @no_type_check
    def get_group_by_operators(self) -> Tuple[Operator, ...]:
        return tuple(
            expr for expr in self._group_by if is_operator(expr)
        )

    def get_group_by_expr_names(self) -> Tuple[str, ...]:
        if not self._group_by_expr_names:
            self._process_column_names()

        return tuple(self._group_by_expr_names)

    def get_group_by_column_names(self) -> Tuple[str, ...]:
        if not self._group_by_col_names:
            self._process_column_names()

        return tuple(self._group_by_col_names)

    def _process_column_names(self) -> None:
        col_names = []
        expr_names = []
        for group_by_arg in self._group_by:   # type: Any
            if is_column(group_by_arg):
                col_names.append(group_by_arg.get_column_name())
            elif is_operator(group_by_arg):
                name = group_by_arg.get_column_name()
                col_names.append(name)
                expr_names.append(name)
            else:
                raise OperatorError(
                    'Group By argument type exception, '
                    'is neither Column nor Operator. '
                    f'Type: {type(group_by_arg)}'
                )
        self._group_by_col_names = tuple(col_names)
        self._group_by_expr_names = tuple(expr_names)


class HashSplitGroupByOperator(NumpyOperator):
    """
    Group By Split Operator.

    Split the table into a list of sub-tables based on criteria,
    provided by GROUP BY clause.

    This implementation is based on a hash-table, storing list of indices
    for each unique key. Requires full pass over all the rows in
    the python space.

    Parameters
    ----------
    select_columns : Tuple[OperatorBaseType, ...]
        Select clause columns and expressions.
    group_by_args : GroupByArguments
        Arguments for GROUP BY clause.
    table : ArrowTable
        Data table.
    """
    def __init__(self,
                 select_columns: Tuple[OperatorBaseType, ...],
                 group_by_args: GroupByArguments,
                 table: ArrowTable) -> None:
        super().__init__(group_by_args.get_group_by_operators(), table)
        self._select_columns = select_columns
        self._group_by_args = group_by_args

    def _run(self) -> None:
        group_by_expressions = self._processed_args
        groups = self._group_by(group_by_expressions)
        self._executor.set_groupby_groups(groups)

    def _add_table_columns(self, expressions: Tuple) -> None:
        self.group_by_expr_columns: Dict[str, AnyArrayLike] = {}

        for column_name, expression in (
                zip(self._group_by_args.get_group_by_expr_names(), expressions)
        ):
            self.table.add_column(column_name, expression)

    def _get_group_key_for_row(self, row_idx: int) -> Tuple:
        """
        Return a hash-table key of GROUP BY columns,
         for a given row index.
        """
        keys = []
        for col_name in self._group_by_args.get_group_by_column_names():
            row_arr = self.table.take_pylist_from_column(col_name, [row_idx])
            row_key = row_arr[0]
            keys.append(row_key)
        return tuple(keys)

    def _group_by(self, group_by_expressions: Tuple) -> Tuple[ArrowTable, ...]:
        """
        Compute groups based on GROUP BY criteria.

        Parameters
        ----------
        group_by_expressions : Tuple
            List of arguments for SQL GROUP BY clause.

        Returns
        -------
        Tuple[ArrowTable]
            All the groups, with a separate ArrowTable per group.
        """
        self._add_table_columns(group_by_expressions)

        group_table_indices = defaultdict(list)
        for idx in range(self.table.num_rows):
            key = self._get_group_key_for_row(idx)
            group_table_indices[key].append(idx)

        groups = []
        for group_indices in group_table_indices.values():
            groups.append(self.table.take(group_indices))

        return tuple(groups)


class TakeGroupByColumnValuesOperator(NumpyOperator):
    """
    Take the first value from the column.

    Used to return values for GROUP BY columns, which are not part of
    aggregate functions.

    Parameters
    ----------
    column : Column
        GROUP BY column to take values for.
    split_operator : Operator
        Wait until Split Operator execution is finished.
    """
    def __init__(self, column: Column, split_operator: Operator) -> None:
        super().__init__(tuple(), None, depends_on=split_operator)
        self._column = column

    def _take_column_value(self) -> Any:
        pylist = self.table.take_pylist_from_column(
            self._column.get_column_name(), [0]
        )
        return pylist[0]

    def _run(self) -> Any:
        return self._take_column_value()


class CombineGroupByGroupsOperator(AbstractCombineGroupByGroupsOperator,
                                   ComputeSelectExpressionsOperator):
    """
    Combine all GROUP BY groups of a single Operator or Columns into
    a single array.

    Operator fetches all the groups from Executor and essentially flattens
    them into a single one-dimensional array.
    """
    def _run(self) -> Any:
        res = super()._run()
        self._executor.clear_groupby_groups()
        return res

    @staticmethod
    def _combine_groups(groups: Tuple) -> np.ndarray:
        return np.array(groups).flatten()

    def _resolve_operator(self, operator: Operator) -> np.ndarray:
        groups = self._executor.collect_operator_result_for_all_groups(
            operator
        )
        return self._combine_groups(groups)


class DropTableColumnsOperator(NumpyOperator):
    """
    Drop given columns in a table.

    Parameters
    ----------
    drop_column_names : Set[str]
        Column names to drop.
    table : ArrowTable
        Table.
    """
    def __init__(self, drop_column_names: Set[str], table: ArrowTable) -> None:
        super().__init__([], table)
        self._drop_column_names = drop_column_names

    def _run(self) -> None:
        self._remove_unused_columns()

    def _remove_unused_columns(self) -> None:
        self.table.drop_columns(self._drop_column_names)


class OrderByOperator(NumpyOperator):
    """
    Order By Operator.

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
                 arguments: Tuple[OperatorBaseType, ...],
                 sort_order: Tuple[SortOrder, ...],
                 table: ArrowTable) -> None:
        super().__init__(arguments, table)

        assert len(arguments)
        assert len(sort_order)

        self._sort_order = sort_order

    def _run(self) -> None:
        self._order_by(self._processed_args)

    def _is_mixed_sort_order(self) -> bool:
        """
        Return True if the order is mixed (ie ASC and DECS are present).
        """
        order_one = self._sort_order[0]
        for order_two in self._sort_order[1:]:
            if order_one != order_two:
                return True
        return False

    def _is_desc(self) -> bool:
        return self._sort_order[0] == SortOrder.DESC

    @staticmethod
    def _reverse_ranks(array: AnyArrayLike) -> np.ndarray:
        groups, unique_indices = np.unique(array, return_inverse=True)
        return len(groups) - unique_indices - 1

    def _order_by(self, columns: Tuple) -> None:
        if self._is_mixed_sort_order():
            cols_in_asc_order = []
            for column, sort_order in zip(columns, self._sort_order):
                if sort_order == SortOrder.DESC:
                    column = self._reverse_ranks(column)
                cols_in_asc_order.append(column)
            ind = np.lexsort(cols_in_asc_order[::-1])
        else:
            ind = np.lexsort(columns[::-1])
            if self._is_desc():
                ind = ind[::-1]

        self.table.take_in_place(ind)


class LimitOperator(NumpyOperator):
    """
    Limit Operator.

    Update the table with a slice [offset: offset + limit].

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
                 table: ArrowTable) -> None:
        super().__init__([], table)

        self._limit = limit
        self._offset = offset

    def _run(self) -> None:
        self.table.replace_with_slice(self._limit, self._offset)
