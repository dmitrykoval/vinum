from typing import List, Iterable, Tuple

import numpy as np

import pyarrow as pa
from pyarrow.lib import ChunkedArray, ArrowInvalid

from vinum._typing import PyArrowArray
from vinum.parser.query import Column
from vinum.util.util import is_numpy_array


class RecordBatch:
    """
    Apache Arrow RecordBatch.

    Parameters
    ----------
    table : pyarrow.RecordBatch
        Apache Arrow RecordBatch instance.
    """
    def __init__(self, batch: pa.RecordBatch) -> None:
        assert batch is not None
        self._batch: pa.RecordBatch = batch
        self._ensure_non_empty_col_names()

    def get_batch(self) -> pa.RecordBatch:
        return self._batch

    def get_schema(self) -> pa.Schema:
        return self._batch.schema

    @property
    def num_columns(self) -> int:
        return self._batch.num_columns

    @property
    def num_rows(self) -> int:
        return self._batch.num_rows

    @property
    def columns(self) -> List[str]:
        return self._batch.columns

    @property
    def column_names(self) -> List[str]:
        return self._batch.schema.names

    @staticmethod
    def from_arrays(arrays: Iterable[Iterable],
                    col_names: Iterable[str]) -> 'RecordBatch':
        return RecordBatch(
            pa.RecordBatch.from_arrays(list(arrays), names=col_names)
        )

    @staticmethod
    def empty_batch() -> 'RecordBatch':
        return RecordBatch(
            pa.RecordBatch.from_arrays([], [])
        )

    def has_column(self, column_name: str) -> bool:
        return column_name in self._batch.schema.names

    def get_column_index(self, column_name: str) -> int:
        return self._batch.schema.names.index(column_name)

    def get_pa_column(self, column: Column) -> pa.Array:
        return self.get_pa_column_by_name(column.get_column_name())

    def get_pa_column_by_index(self, index: int) -> pa.Array:
        return self._batch.columns[index]

    def get_pa_column_by_name(self, column_name: str) -> pa.Array:
        if column_name not in self._batch.schema.names:
            raise ValueError(f'Column "{column_name}" is not found.')
        return self.get_pa_column_by_index(
            self.get_column_index(column_name)
        )

    def get_np_column(self, column: Column) -> np.ndarray:
        arr = self.get_pa_column(column)
        return self._arrow_array_to_numpy(arr)

    def filter(self, bitmask: Iterable[bool]) -> 'RecordBatch':
        if is_numpy_array(bitmask):
            bitmask = pa.array(bitmask)
        return RecordBatch(
            self._batch.filter(bitmask, null_selection_behavior='emit_null')
        )

    def slice(self, length: int, offset: int = 0) -> 'RecordBatch':
        return RecordBatch(
            self._batch.slice(offset, length)
        )

    def rename_columns(self, column_names: List[str]) -> None:
        self._batch = self._batch.rename_columns(column_names)

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

    def _new_record_batch(self,
                   columns: Tuple[Iterable],
                   column_names: Tuple[str, ...]) -> None:
        self._batch = pa.RecordBatch.from_arrays(columns, names=column_names)

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
