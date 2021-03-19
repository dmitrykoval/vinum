import re
from enum import Enum, auto
from typing import Optional, Any, Iterable, Tuple, Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from vinum._typing import AnyArrayLike, OperatorArgument
from vinum.arrow.arrow_table import ArrowTable
from vinum.core.base import Operator, VectorizedExpression

from vinum.errors import OperatorError
from vinum.parser.query import Column, Literal
from vinum.util.util import (
    ensure_is_array,
    is_numpy_array,
    is_numpy_str_array, is_pyarrow_array, is_pyarrow_string, is_array_type,
)


class DatetimeFunction(VectorizedExpression):
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

    def _get_unit(self, arguments: Any) -> str:
        if len(arguments) > 1:
            unit = arguments[1]
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

    def _get_dtype(self, arguments: Any):
        unit = self._get_unit(arguments)
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
        try:
            np_units_idx = self.NUMPY_UNITS.index(numpy_unit)
            for sup_unit in self.NUMPY_UNITS[np_units_idx + 1:]:
                if sup_unit in self.UNITS:
                    return sup_unit
        except ValueError:
            pass
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

    def _expr_kernel(self, arguments: Any, table: ArrowTable) -> Any:
        if not len(arguments):
            msg = f'No arguments provided for {self.__class__} operator.'
            raise OperatorError(msg)

        arg = ensure_is_array(arguments[0])
        dtimes = np.array(arg, dtype=self._get_dtype(arguments))
        return self._ensure_unit_correctness(dtimes)


class DateFunction(DatetimeFunction):
    """
    Date built-in function.
    """
    UNITS = ['D']

    unit = 'D'


class TimestampFunction(DatetimeFunction):
    """
    From_timestamp built-in function.
    """
    UNITS = ['s', 'ms', 'us', 'ns']

    unit = 's'


class AbstractCastFunction(VectorizedExpression):
    """
    Abstract cast operator.

    Provides basic functionality for type casting, subclasses can
    simply define the type to cast to via `type` class property.
    See BoolCastFunction for an example.
    """
    type: Optional[str] = None

    def _expr_kernel(self, arguments: Any, table: ArrowTable) -> Any:
        if len(arguments) > 1:
            arr = arguments
        else:
            arr = arguments[0]

        arr = ensure_is_array(arr)
        return np.array(arr, dtype=self.type)


class BoolCastFunction(AbstractCastFunction):
    """
    Bool type cast built-in function.
    """
    type = 'bool'


class FloatCastFunction(AbstractCastFunction):
    """
    Float type cast built-in function.
    """
    type = 'float'


class IntCastFunction(AbstractCastFunction):
    """
    Int type cast built-in function.
    """
    type = 'int'


class StringCastFunction(AbstractCastFunction):
    """
    String type cast built-in function.
    """
    type = 'str'


class AbstractStringFunction(VectorizedExpression):
    """
    Abstract String Operator.

    Provides basic functionality for string manipulation,
    such as casting non-string arguments to string, etc.
    """

    def _process_arguments(self, arguments: Iterable[OperatorArgument],
                           batch: ArrowTable) -> Tuple:
        args = super()._process_arguments(arguments, batch)
        return self._cast_args_to_str(args)

    @staticmethod
    def _cast_args_to_str(args: Iterable
                          ) -> Tuple[Union[str, np.ndarray], ...]:
        str_args = []

        has_arrays = any(is_array_type(arg) for arg in args)

        for arg in args:
            if is_numpy_array(arg):
                clean_arg = np.array(arg, dtype="U")
            elif is_pyarrow_array(arg):
                if not pa.types.is_string(arg.type):
                    clean_arg = arg.cast(target_type=pa.string())
                else:
                    clean_arg = arg
            else:
                if has_arrays:
                    clean_arg = pa.array((str(arg),), type=pa.string())
                else:
                    clean_arg = str(arg)
            str_args.append(clean_arg)
        return tuple(str_args)

    @staticmethod
    def _post_process_result(result: Any) -> Any:
        """
        In case the result is a single string, wrap it as an array.
        """
        if is_numpy_str_array(result) and result.shape == ():
            return np.array((result,))
        elif is_pyarrow_string(result):
            # TODO conversion to python string is a hack,
            # TODO but otherwise it throws an exception:
            # TODO 'pyarrow.lib.ArrowTypeError: Expected bytes,
            # TODO  got a 'pyarrow.lib.StringScalar' object'
            # TODO need to file a bug report
            return pa.array((result.as_py(),), type=pa.string())
        else:
            return result


class ConcatFunction(AbstractStringFunction):
    """
    String Concat Operator.

    Concatenate all the arguments as strings.

    Parameters
    ----------
    arguments : Iterable[OperatorBaseType, ...]
        Columns or string literals to concatenate.
    table : ArrowTable
        Table.
    """
    def __init__(self,
                 arguments: Iterable[OperatorArgument]) -> None:
        super().__init__(arguments=arguments,
                         function=np.char.add,
                         is_numpy_func=True,
                         is_binary_func=True)

    def _process_arguments(self, arguments: Iterable[OperatorArgument],
                           batch: ArrowTable) -> Tuple:
        args = super()._process_arguments(arguments, batch)

        return tuple(arg if is_numpy_array(arg)
                     else np.array(arg, dtype='U')
                     for arg in args)

class UpperStringFunction(AbstractStringFunction):
    """
    Upper String Operator.

    Convert a string to uppercase.
    """
    def _expr_kernel(self, arguments: Any, table: ArrowTable) -> Any:
        # return np.char.upper(*arguments)
        return pc.utf8_upper(*arguments)


class LowerStringFunction(AbstractStringFunction):
    """
    Lower String function.

    Convert a string to lowercase.
    """
    def _expr_kernel(self, arguments: Any, table: ArrowTable) -> Any:
        return pc.utf8_lower(*arguments)
        # return np.char.lower(*arguments)


class LikeFunction(VectorizedExpression):
    """
    LIKE function.

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
    """
    def __init__(self,
                 arguments: Tuple[Union[Column, VectorizedExpression], Literal],
                 invert: bool) -> None:
        super().__init__(arguments=arguments,
                         is_numpy_func=True)
        self.invert = invert

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

    def _expr_kernel(self, arguments: Any, table: ArrowTable) -> Any:
        column, pattern = arguments
        return self._like(column, pattern)


class FunctionType(Enum):
    ARROW = auto()
    NUMPY = auto()
    CLASS = auto()


_default_functions_registry = {
    # Type conversion
    'to_bool': (BoolCastFunction, FunctionType.CLASS),
    'to_float': (FloatCastFunction, FunctionType.CLASS),
    'to_int': (IntCastFunction, FunctionType.CLASS),
    'to_str': (StringCastFunction, FunctionType.CLASS),

    # Math
    'abs': (np.absolute, FunctionType.NUMPY),
    'sqrt': (np.sqrt, FunctionType.NUMPY),
    'cos': (np.cos, FunctionType.NUMPY),
    'sin': (np.sin, FunctionType.NUMPY),
    'tan': (np.tan, FunctionType.NUMPY),
    'power': (np.power, FunctionType.NUMPY),
    'log': (np.log, FunctionType.NUMPY),
    'log2': (np.log2, FunctionType.NUMPY),
    'log10': (np.log10, FunctionType.NUMPY),

    # Math constants
    'pi': (lambda: np.pi, FunctionType.NUMPY),
    'e': (lambda: np.e, FunctionType.NUMPY),

    # Datetime
    'date': (DateFunction, FunctionType.CLASS),
    'datetime': (DatetimeFunction, FunctionType.CLASS),
    'from_timestamp': (TimestampFunction, FunctionType.CLASS),
    'timedelta': (np.timedelta64, FunctionType.NUMPY),
    'is_busday': (np.is_busday, FunctionType.NUMPY),

    # String
    'concat': (ConcatFunction, FunctionType.CLASS),
    'upper': (UpperStringFunction, FunctionType.CLASS),
    'lower': (LowerStringFunction, FunctionType.CLASS),
}


AGG_FUNCS = {
    'count_star',
    'count',
    'min',
    'max',
    'sum',
    'avg',
    'np.min',
    'np.max',
    'np.sum',
}

NUMPY_AGG_MAPPING = {
    'np.min': 'min',
    'np.max': 'max',
    'np.sum': 'sum',
}


def is_aggregate_func(function_name: Optional[str]) -> bool:
    """
    Return True if function is aggregate.

    Parameters
    ----------
    function_name : str
        Name of the function.

    Returns
    -------
    bool
        True if the function is aggregate.
    """
    return function_name and function_name.lower() in AGG_FUNCS


def ensure_numpy_mapping(function_name):
    assert function_name
    function_name = function_name.lower()
    if function_name in NUMPY_AGG_MAPPING:
        return NUMPY_AGG_MAPPING[function_name]
    else:
        return function_name
