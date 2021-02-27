from typing import List, Iterable, TYPE_CHECKING

import numpy as np

import pyarrow as pa
from pyarrow.lib import ChunkedArray, ArrowInvalid

from vinum._typing import PyArrowArray

if TYPE_CHECKING:
    try:
        import pandas as pd
    except ModuleNotFoundError:
        pass


class ArrowTable:
    """
    Apache Arrow Table abstraction.

    Arrow Table represents a foundational data structure on which
    Vinum executes a SQL query.
    Subsequently, the result of the query is stored as an Arrow Table.

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

    def combine_chunks(self) -> 'ArrowTable':
        return ArrowTable(
            self._table.combine_chunks()
        )

    def get_column_index(self, column_name: str) -> int:
        return self._table.schema.names.index(column_name)

    def get_pa_column_by_index(self, index: int) -> pa.Array:
        chunked = self._table.columns[index]

        assert chunked.num_chunks <= 1

        return chunked.chunk(0) if chunked.num_chunks else pa.array([])

    def get_pa_column_by_name(self, column_name: str) -> pa.Array:
        if column_name not in self._table.schema.names:
            raise ValueError(f'Column "{column_name}" is not found.')
        return self.get_pa_column_by_index(
            self.get_column_index(column_name)
        )

    def get_np_column_by_name(self, column_name: str) -> np.ndarray:
        arr = self.get_pa_column_by_name(column_name)
        return self._arrow_array_to_numpy(arr)

    def slice(self, length: int, offset: int = 0) -> 'ArrowTable':
        return ArrowTable(
            self._table.slice(offset, length)
        )

    def rename_columns(self, column_names: List[str]) -> None:
        self._table = self._table.rename_columns(column_names)

    def to_pandas(self) -> 'pd.DataFrame':
        return self._table.to_pandas()

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

    @staticmethod
    def from_batches(batches: Iterable[pa.RecordBatch]):
        if not batches:
            return ArrowTable(pa.Table.from_arrays([], []))
        return ArrowTable(pa.Table.from_batches(batches))