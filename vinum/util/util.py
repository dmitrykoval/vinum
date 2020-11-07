import numpy as np
import pyarrow as pa
from datetime import datetime

from typing import Iterable, Tuple, Any, TYPE_CHECKING, FrozenSet, List

if TYPE_CHECKING:
    from vinum._typing import QueryBaseType, AnyArrayLike
    from vinum.parser.query import Column

TREE_INDENT_SYMBOL = '  '


def find_all_columns_recursively(
        expressions: Tuple['QueryBaseType', ...],
        skip_shared_expressions: bool = False) -> FrozenSet['Column']:
    """
    Traverse Expressions and return a set of all referenced Columns.

    Null type is one of:
    * None python type
    * numpy.nan
    * numpy.datetime64('nat')

    Parameters
    ----------
    expressions : Tuple['QueryBaseType', ...]
        Expressions to traverse.
    skip_shared_expressions : bool
        Expressions to traverse.

    Returns
    -------
    FrozenSet['Column']
        Set of all uniques columns used in Expressions
        (regardless of the recursion depth).
    """
    if not expressions:
        return frozenset()

    columns = set()
    for select_expr in expressions:  # type: Any
        if is_column(select_expr):
            columns.add(select_expr)
        elif is_expression(select_expr) or is_operator(select_expr):
            if skip_shared_expressions and select_expr.is_shared():
                continue
            columns |= find_all_columns_recursively(
                select_expr.arguments,
                skip_shared_expressions=skip_shared_expressions
            )
    return frozenset(columns)


def append_flat(append_to: List, append_what: Any):
    if isinstance(append_what, Iterable):
        append_to.extend(append_what)
    else:
        append_to.append(append_what)


def ensure_is_array(obj: Any) -> Tuple:
    if not is_array_type(obj):
        return tuple((obj,))
    return obj


def is_literal(obj: Any) -> bool:
    from vinum.parser.query import Literal
    return isinstance(obj, Literal)


def is_column(obj: Any) -> bool:
    from vinum.parser.query import Column
    return isinstance(obj, Column)


def is_expression(obj: Any) -> bool:
    from vinum.parser.query import Expression
    return isinstance(obj, Expression)


def is_operator(obj: Any) -> bool:
    from vinum.core.operators.generic_operators import Operator
    return isinstance(obj, Operator)


def is_numpy_type(obj: Any) -> bool:
    return isinstance(obj, (np.ndarray, np.generic))


def is_numpy_number(obj: Any) -> bool:
    return is_numpy_type(obj) and np.issubdtype(obj.dtype, np.number)


def is_numpy_datetime(obj: Any) -> bool:
    return is_numpy_type(obj) and np.issubdtype(obj.dtype, np.datetime64)


def is_numpy_string_type(obj: Any) -> bool:
    return is_numpy_type(obj) and obj.dtype.kind in {'U', 'S'}


def is_numpy_array(array: Any) -> bool:
    return isinstance(array, np.ndarray)


def is_numpy_str_array(array: Any) -> bool:
    return is_numpy_array(array) and np.issubdtype(array.dtype, np.str)


def is_numpy_bool_array(array: Any) -> bool:
    return is_numpy_array(array) and np.issubdtype(array.dtype, np.bool)


def is_numpy_array_dtype_in(array: Any, dtypes: Tuple) -> bool:
    if not is_numpy_array(array):
        return False
    for dtype in dtypes:
        if np.issubdtype(array.dtype, dtype):
            return True
    return False


def is_pyarrow_array(array: Any) -> bool:
    return isinstance(array, pa.Array) or isinstance(array, pa.ChunkedArray)


def is_datetime_array(array: Any) -> bool:
    if is_numpy_array(array) and np.issubdtype(array.dtype,
                                               np.datetime64):
        return True
    elif is_pyarrow_array(array) and pa.types.is_temporal(array.type):
        return True
    elif isinstance(array, Iterable):
        element = next(iter(array))
        is_python_date = isinstance(element, datetime)
        return is_numpy_datetime(element) or is_python_date
    else:
        return False


def is_array_type(obj: Any) -> bool:
    return (isinstance(obj, list)
            or isinstance(obj, tuple)
            or is_numpy_array(obj)
            or is_pyarrow_array(obj)
            )


def is_null_mask(array: np.ndarray) -> np.ndarray:
    """
    Returns an array with boolean mask indicating whether
    the value at the corresponding index is of 'null' type.

    Null type is one of:
    * None python type
    * numpy.nan
    * numpy.datetime64('nat')

    Parameters
    ----------
    array : np.ndarray
        Array to create null mask for.

    Returns
    -------
    np.ndarray
        Boolean mask with True indicating nulls in the input array.
    """
    if is_numpy_number(array):
        mask = np.isnan(array)
    elif is_numpy_datetime(array):
        mask = np.isnat(array)
    else:
        # The equality comparison to None is used here
        # instead of `array is None` because `==` is
        # overriden for numpy array.
        # Hence it results in vectorized operation.
        mask = (array == None)  # noqa: E711

    return mask


def is_not_null_mask(array: np.ndarray) -> np.ndarray:
    """
    Returns an array with boolean mask indicating whether
    the value at the corresponding index is not of 'null' type.

    Inverts ``is_null_mask``.

    Null type is one of:
    * None python type
    * numpy.nan
    * numpy.datetime64('nat')

    Parameters
    ----------
    array : np.ndarray
        Array to create null mask for.

    Returns
    -------
    np.ndarray
        Boolean mask with True indicating non-nulls in the input array.
    """
    return ~is_null_mask(array)


def safe_array_len(array: 'AnyArrayLike') -> int:
    if is_numpy_array(array):
        return array.size   # type: ignore
    else:
        return len(array)
