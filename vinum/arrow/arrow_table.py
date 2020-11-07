from typing import List, Iterable, TYPE_CHECKING, Tuple

import numpy as np

import pyarrow as pa
from pyarrow.lib import ChunkedArray, ArrowInvalid

from vinum._typing import PyArrowArray, AnyArrayLike
from vinum.parser.query import Column

if TYPE_CHECKING:
    try:
        import pandas as pd
    except ModuleNotFoundError:
        pass


class ArrowTable:
    """
    Apache Arrow Table abstraction.

    Arrow Table represents a foundational data structure on top of which
    Pine executes a SQL query.
    Subsequently, the result of the query is stored as an Arrow Table.

    ArrowTable is a shared and mutable object. In case of concurrent
    execution, additional synchronization layer has to be provided.

    During the process of query execution, individual Operators may get
    get the data from a table or modify the data inside of the table.
    For example, BooleanFilterOperator may update the contents of the
    table based on filter predicate.

    Single instance of ArrowTable is shared during the entire lifecycle
    of query execution.

    Where possible, ArrowTable tries to return a zero-copy view of columns.
    When it's not possible to represent an underlying data structure as a
    Numpy array, for example for strings, a copy would be made.

    Parameters
    ----------
    table : pyarrow.Table
        Apache Arrow Table instance.
    """
    def __init__(self, table: pa.Table) -> None:
        assert table is not None
        self._table: pa.Table = table
        self._ensure_non_empty_col_names()

    def get_table(self) -> pa.Table:
        return self._table

    def get_schema(self) -> pa.Schema:
        return self._table.schema

    @property
    def num_columns(self) -> int:
        return self._table.num_columns

    @property
    def num_rows(self) -> int:
        return self._table.num_rows

    @property
    def column_names(self) -> List[str]:
        return self._table.column_names

    def has_column(self, column_name: str) -> bool:
        return column_name in self._table.schema.names

    def get_pa_column(self, column: Column) -> pa.ChunkedArray:
        return self.get_pa_column_by_name(column.get_column_name())

    def get_pa_column_by_index(self, index: int) -> pa.ChunkedArray:
        return self._table.columns[index]

    def get_pa_column_by_name(self, column_name: str) -> pa.ChunkedArray:
        if column_name not in self._table.schema.names:
            raise ValueError(f'Column "{column_name}" is not found.')
        return self.get_pa_column_by_index(
            self._table.schema.names.index(column_name)
        )

    def get_np_column(self, column: Column) -> np.ndarray:
        arr = self.get_pa_column(column)
        return self._arrow_array_to_numpy(arr)

    def get_np_column_by_index(self, index: int) -> np.ndarray:
        arr = self.get_pa_column_by_index(index)
        return self._arrow_array_to_numpy(arr)

    def get_np_column_by_name(self, column_name: str) -> np.ndarray:
        arr = self.get_pa_column_by_name(column_name)
        return self._arrow_array_to_numpy(arr)

    def take(self, indices: List[int]) -> 'ArrowTable':
        """
        Select rows with provided indices and return a new Table.
        """
        return ArrowTable(self._table.take(indices))

    def take_in_place(self, indices: List[int]) -> None:
        """
        Select rows with provided indices and replace current table
        with the result.
        """
        self._table = self._table.take(indices)

    def take_from_column(self,
                         column_name: str,
                         indices: List[int]) -> PyArrowArray:
        """
        Return a column with indices selected.
        """
        col: pa.ChunkedArray = self.get_pa_column_by_name(column_name)
        return col.take(indices)

    def take_pylist_from_column(self,
                                column_name: str,
                                indices: List[int]) -> List:
        return self.take_from_column(column_name, indices).to_pylist()

    def apply_bitmask(self, bitmask: Iterable[bool]) -> None:
        self._table = self._table.filter(bitmask)

    def slice(self, length: int, offset: int = 0) -> pa.Table:
        return self._table.slice(offset, length)

    def replace_with_slice(self, length: int, offset: int = 0) -> None:
        self._table = self.slice(length, offset)

    def add_column(self, name: str, column: AnyArrayLike) -> None:
        if name in self._table.schema.names:
            raise ValueError(f'Column name {name} already exists')
        else:
            self._table = self._table.append_column(name, [column])

    def drop_columns(self, column_names: Iterable[str]) -> None:
        self._table = self._table.drop(column_names)

    def retain_columns(self,
                       n_columns: int,
                       new_column_names: Tuple[str, ...] = None) -> None:
        """
        Retain only n leading columns in the table.
        """
        if n_columns < self._table.num_columns:
            self._table = self._table.drop(
                self._table.schema.names[n_columns:]
            )

        if new_column_names:
            self._table = self._table.rename_columns(new_column_names)

    def replace_columns(self,
                        columns: Tuple[Iterable],
                        column_names: Tuple[str, ...] = None) -> None:
        """
        Replace the table with a new table constructed from columns.
        """
        if column_names is None:
            column_names = self._table.schema.names[:len(columns)]
        self._new_table(columns, column_names)

    def rename_columns(self, column_names: List[str]) -> None:
        self._table = self._table.rename_columns(column_names)

    def to_pandas(self) -> 'pd.DataFrame':
        return self._table.to_pandas()

    def clone(self) -> 'ArrowTable':
        return ArrowTable(self._table.slice())

    def _ensure_non_empty_col_names(self):
        unnamed_count = 0
        new_names = []
        for col_name in self.column_names:
            if not col_name:
                new_names.append(f'unnamed_{unnamed_count}')
                unnamed_count += 1
            else:
                new_names.append(col_name)
        if unnamed_count:
            self.rename_columns(new_names)

    def _new_table(self,
                   columns: Tuple[Iterable],
                   column_names: Tuple[str, ...]) -> None:
        self._table = pa.Table.from_arrays(columns, names=column_names)

    @staticmethod
    def _arrow_array_to_numpy(array: PyArrowArray) -> np.ndarray:
        """
        Convert Arrow Array or ChunkedArray to Numpy array.

        Returns
        -------
        :class:`numpy.ndarray`
        """
        # The implementation below would make a copy of an array
        # for non-numeric types or columns with Nulls.
        # TODO: is there a work-around to avoid making a copy?
        if isinstance(array, ChunkedArray):
            np_arr = array.to_numpy()
        else:
            try:
                np_arr = array.to_numpy(zero_copy_only=True)
            except ArrowInvalid:
                np_arr = array.to_numpy(zero_copy_only=False)

        # This type test is needed because by default, Arrow creates
        # an 'object' typed numpy array for a 'string' column.
        if array.type == 'string' and array.null_count == 0:
            np_arr = np_arr.astype('U')

        return np_arr
