import numpy as np
from typing import Dict, Callable, Set, Optional

from vinum.core.operators.numpy_function_operators import (
    IntCastOperator,
    BoolCastOperator,
    FloatCastOperator,
    StringCastOperator,
    DatetimeOperator,
    TimestampOperator,
    DateOperator,
    ConcatOperator,
    UpperStringOperator,
    LowerStringOperator,
)
from vinum.core.operators.numpy_operator_mappings import AGGREGATE_OPERATORS
from vinum.errors import FunctionError


_default_functions_registry = {
    # Type conversion
    'bool': BoolCastOperator,
    'float': FloatCastOperator,
    'int': IntCastOperator,
    'str': StringCastOperator,

    # Math
    'abs': np.absolute,
    'sqrt': np.sqrt,
    'cos': np.cos,
    'sin': np.sin,
    'tan': np.tan,
    'power': np.power,
    'log': np.log,
    'log2': np.log2,
    'log10': np.log10,

    # Datetime
    'date': DateOperator,
    'datetime': DatetimeOperator,
    'from_timestamp': TimestampOperator,
    'timedelta': np.timedelta64,
    'is_busday': np.is_busday,

    # String
    'concat': ConcatOperator,
    'upper': UpperStringOperator,
    'lower': LowerStringOperator,
}


AGGREGATE_FUNCTIONS = {
    'count',
    'sum',
    'min',
    'max',
    'mean',
    'median',
    'var',
    'avg',
    'std',
    'stddev',
    'np.min',
    'np.max',
    'np.mean',
    'np.std',
    'np.var',
    'np.median',
}

_udf_registry: Dict[str, Callable] = {}
_udf_aggregate_functions: Set[str] = set()


def _ensure_function_name_correctness(name: str) -> str:
    assert name is not None
    assert type(name) == str
    return name.lower()


def _register_udf(name: str, function, is_aggregate=False) -> None:
    function_name = _ensure_function_name_correctness(name)

    _remove_udf(name)

    _udf_registry[function_name] = function

    if is_aggregate:
        _udf_aggregate_functions.add(name)


def _remove_udf(name: str) -> None:
    if name in _udf_registry:
        del _udf_registry[name]
    if name in _udf_aggregate_functions:
        _udf_aggregate_functions.remove(name)


def is_aggregate_function(function_name: Optional[str]) -> bool:
    """
    Return True if function is either built-in aggregate or user-defined one.

    Parameters
    ----------
    function_name : str
        Name of the function.

    Returns
    -------
    bool
        True if the function is aggregate.
    """
    return (function_name in AGGREGATE_FUNCTIONS
            or function_name in _udf_aggregate_functions)


def lookup_aggregate_function(function_name: str) -> Callable:
    """
    Return aggregate function by function name.

    First, built-in aggregates registry is used, if the function is not found
    then aggregate UDFs registry is checked.

    Parameters
    ----------
    function_name : str
        Name of the function.

    Returns
    -------
    Callable
        Function callable.
    """
    if function_name in AGGREGATE_OPERATORS.keys():
        return AGGREGATE_OPERATORS[function_name]
    elif function_name in _udf_aggregate_functions:
        return lookup_udf(function_name)
    else:
        raise FunctionError(
            f"Aggregate function '{function_name}' is not found."
        )


def lookup_udf(function_name: str) -> Callable:
    """
    Return UDF by name.

    If function is a numpy function, ie it starts with a
    reserved namespace 'np.', its definition is evaluated via `eval`
    and resulting Callable returned.

    Parameters
    ----------
    function_name : str
        Name of the function.

    Returns
    -------
    Callable
        Function callable.
    """
    function_name = _ensure_function_name_correctness(function_name)

    if function_name.startswith('np.'):
        try:
            return eval(function_name)
        except (NameError, AttributeError):
            raise FunctionError(
                f"Numpy function '{function_name}' is not found "
                "in the numpy package."
            )
    elif function_name in _default_functions_registry:
        return _default_functions_registry[function_name]
    elif function_name in _udf_registry:
        return _udf_registry[function_name]
    else:
        raise FunctionError(f"Function '{function_name}' is not found.")


def register_python(function_name: str, function) -> None:
    """
    Register Python function as a User Defined Function (UDF).

    Parameters
    ----------
    function_name : str
        Name of the User Defined Function.
    function: callable, python function
        Function to be used as a UDF.

    See also
    --------
    register_numpy : Register Numpy function as a User Defined Function.

    Notes
    -----
    Python functions are "vectorized" before use, via ``numpy.vectorize``.
    For better performance, please try to use numpy UDFs,
    operating in terms of numpy arrays. See :func:`vinum.register_numpy`.

    Function would be invoked for individual rows of the Table.

    Any python packages used inside of the function should be imported
    before the invocation.

    Function names are case insensitive.

    Examples
    --------
    Using lambda as a UDF:

    >>> import vinum as vn
    >>> vn.register_python('cube', lambda x: x**3)
    >>> tbl = vn.Table.from_pydict({'len': [1, 2, 3], 'size': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT cube(size) from t ORDER BY cube(size) DESC')
       cube
    0  4913
    1  2197
    2   343

    >>> import math
    >>> import vinum as vn
    >>> vn.register_python('distance', lambda x, y: math.sqrt(x**2 + y**2))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('select x, y, distance(x, y) as dist from t')
       x   y       dist
    0  1   7   7.071068
    1  2  13  13.152946
    2  3  17  17.262677

    Using regular python function:

    >>> import vinum as vn
    >>> def sin_taylor(x):
    ...     "Taylor series approximation of the sine trig function around 0."
    ...     return x - x**3/6 + x**5/120 - x**7/5040
    ...
    >>> vn.register_python('sin', sin_taylor)
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('select sin(x) as sin_x, sin(y) as sin_y from t '
    ...            'order by sin_y')
          sin_x     sin_y
    0  0.141120 -0.961397
    1  0.909297  0.420167
    2  0.841471  0.656987
    """
    function = np.vectorize(function)
    _register_udf(function_name, function)


def register_numpy(function_name: str, function, is_aggregate=False) -> None:
    """
    Register Numpy function as a User Defined Function (UDF).
    UDF can perform vectorized operations on arrays passed as arguments.

    Parameters
    ----------
    function_name : str
        Name of the User Defined Function.

    function: callable
        Function to be used as a UDF. Function has to operate on vectorized
        numpy arrays.
        Numpy arrays will be passed as input arguments to the function
        and it should return numpy array.
        (except when ``is_aggregate``=True).

    is_aggregate: bool, optional
        Set to `True` when the function is aggregate - that is taking
        numpy array as an input
        and returning scalar value. Aggregate functions can be used
        in GROUP BY queries.


    See also
    --------
    register_python : Register Python function as a User Defined Function.

    Notes
    -----
    Numpy package is imported under `np` namespace.
    You can invoke any function from the `np.*` namespace.

    Arguments of the function would be numpy arrays of provided columns.
    UDF can perform vectorized operations on arrays passed as arguments.
    The function would be called only once
    (or once per group in the GROUP BY queries).

    Function names are case insensitive.

    Examples
    --------
    Define a function operating with Numpy arrays. Numpy function perform
    vectorized operations on input numpy arrays.

    >>> import numpy as np
    >>> import vinum as vn
    >>> vn.register_numpy('cube', lambda x: np.power(x, 3))
    >>> tbl = vn.Table.from_pydict({'len': [1, 2, 3], 'size': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT cube(size) from t ORDER BY cube(size) DESC')
       cube
    0  4913
    1  2197
    2   343

    >>> import numpy as np
    >>> import vinum as vn
    >>> vn.register_numpy('distance',
    ...                   lambda x, y: np.sqrt(np.square(x) + np.square(y)))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('select x, y, distance(x, y) as dist from t')
       x   y       dist
    0  1   7   7.071068
    1  2  13  13.152946
    2  3  17  17.262677

    Please note that `x` and `y` arguments are of `np.array` type.
    In both of the cases function perform vectorized operations on input
    numpy arrays.


    >>> import numpy as np
    >>> import vinum as vn
    >>> def z_score(x: np.array):
    ...     \"\"\"Compute Standard Score\"\"\"
    ...     mean = np.mean(x)
    ...     std = np.std(x)
    ...     return (x - mean) / std
    ...
    >>> vn.register_numpy('score', z_score)
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('select x, score(x), y, score(y) from t')
       x     score   y   score_1
    0  1 -1.224745   7 -1.297771
    1  2  0.000000  13  0.162221
    2  3  1.224745  17  1.135550

    Please note that `x` argument is of `np.array` type.
    """
    _register_udf(function_name, function, is_aggregate)
