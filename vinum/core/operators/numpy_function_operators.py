from typing import Optional, Any

import numpy as np

from vinum.core.operators.generic_operators import AggregateOperator, Operator
from vinum.core.operators.numpy_operators import NumpyOperator
from vinum.errors import OperatorError
from vinum.parser.query import Column
from vinum.util.util import ensure_is_array, is_not_null_mask


class CountOperator(AggregateOperator, NumpyOperator):
    """
    Count built-in function.

    There are two modes of counting rows:
        1. With '*' argument, return count of all the rows in a group.
        2. With expression or column as an argument. In this case
            count would return all non-null rows
            (ie not: None, np.nan, np.datetime64('nat')).
    """
    def _run(self):
        if self.arguments and isinstance(self.arguments[0],
                                         (Operator, Column)):
            col = self._processed_args[0]
            return np.count_nonzero(is_not_null_mask(col))
        else:
            return self.table.num_rows


class DatetimeOperator(NumpyOperator):
    """
    Datetime built-in function.

    Essentially, acts as a mapper from Nympy datetime64
    types to Arrow datetime types.

    Important! Units should be listed, in the increasing resolution order.
    """
    UNITS = ['D', 's', 'ms', 'us', 'ns']
    NUMPY_UNITS = ['Y', 'M', 'W', 'D', 'h', 'm', 's', 'ms', 'us', 'ns']

    unit: Optional[str] = None

    def _ensure_ts_unit(self, unit: str) -> None:
        if unit not in self.UNITS:
            msg = (f"Unsupported {self.__class__.__name__} unit: '{unit}'. "
                   f"Supported units are: [{', '.join(self.UNITS)}]")
            raise OperatorError(msg)

    def _get_unit(self) -> str:
        if len(self._processed_args) > 1:
            unit = self._processed_args[1]
            self._ensure_ts_unit(unit)
        else:
            unit = self.unit
        return unit

    @staticmethod
    def _format_datetime_dtype(unit: str = None) -> str:
        dtype = 'datetime64'
        if unit:
            return f'{dtype}[{unit}]'
        else:
            return dtype

    def _get_dtype(self):
        unit = self._get_unit()
        return self._format_datetime_dtype(unit)

    @staticmethod
    def _parse_numpy_datetime_unit(dtype: np.dtype) -> str:
        assert dtype
        name = dtype.name
        if name and '[' in name:
            return name[name.index('[') + 1:name.index(']')]
        else:
            return ''

    def _find_higher_res_unit(self, numpy_unit: str) -> str:
        """"
        Find the next supported unit which would give higher resolution,
        than provided numpy unit.

        For example, if 'h' (hours) is passed and only seconds are supported,
        the method would return 's'.
        If none found, highest supported resolution is returned.

        Parameters
        ----------
        numpy_unit : str
            Numpy datetime unit ('Y', 'M', 'W', 'D', 'h', ..)
        """
        np_units_idx = self.NUMPY_UNITS.index(numpy_unit)
        for sup_unit in self.NUMPY_UNITS[np_units_idx + 1:]:
            if sup_unit in self.UNITS:
                return sup_unit
        return self.UNITS[-1]

    def _ensure_unit_correctness(self, np_array: np.ndarray) -> np.ndarray:
        """
        Ensure the array unit is supported by the class.

        If the time unit is not passed explicitly, np.datetime64 would try to
        autodect it. The resulting unit time might not be supported by Arrow.
        This method makes sure the resulting unit is supported by Arrow,
        by upscaling it to the highest supported unit.
        """
        unit = self._parse_numpy_datetime_unit(np_array.dtype)
        if unit not in self.UNITS:
            sup_unit = self._find_higher_res_unit(unit)
            return np_array.astype(self._format_datetime_dtype(sup_unit))
        return np_array

    def _run(self) -> Any:
        if not len(self._processed_args):
            msg = f'No arguments provided for {self.__class__} operator.'
            raise OperatorError(msg)

        arg = ensure_is_array(self._processed_args[0])
        dtimes = np.array(arg, dtype=self._get_dtype())
        return self._ensure_unit_correctness(dtimes)


class DateOperator(DatetimeOperator):
    """
    Date built-in function.
    """
    UNITS = ['D']

    unit = 'D'


class TimestampOperator(DatetimeOperator):
    """
    From_timestamp built-in function.
    """
    UNITS = ['s', 'ms', 'us', 'ns']

    unit = 's'


class AbstractCastOperator(NumpyOperator):
    """
    Abstract cast operator.

    Provides basic functionality for type casting, subclasses can
    simply define the type to cast to via `type` class property.
    See BoolCastOperator for an example.
    """
    type: Optional[str] = None

    def _run(self):
        if len(self._processed_args) > 1:
            arr = self._processed_args
        else:
            arr = self._processed_args[0]

        arr = ensure_is_array(arr)
        return np.array(arr, dtype=self.type)


class BoolCastOperator(AbstractCastOperator):
    """
    Bool type cast built-in function.
    """
    type = 'bool'


class FloatCastOperator(AbstractCastOperator):
    """
    Float type cast built-in function.
    """
    type = 'float'


class IntCastOperator(AbstractCastOperator):
    """
    Int type cast built-in function.
    """
    type = 'int'


class StringCastOperator(AbstractCastOperator):
    """
    String type cast built-in function.
    """
    type = 'str'
