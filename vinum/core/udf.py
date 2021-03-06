import numpy as np
from typing import Dict, Callable, Tuple

from vinum.core.functions import (
    _default_functions_registry, FunctionType, )
from vinum.errors import FunctionError

_udf_registry: Dict[str, Callable] = {}


def _ensure_function_name_correctness(name: str) -> str:
    assert name is not None
    assert type(name) == str
    return name.lower()


def _register_udf(name: str, function) -> None:
    function_name = _ensure_function_name_correctness(name)
    _remove_udf(name)
    _udf_registry[function_name] = function


def _remove_udf(name: str) -> None:
    if name in _udf_registry:
        del _udf_registry[name]


def lookup_udf(function_name: str) -> Tuple[Callable, FunctionType]:
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
            func = eval(function_name)
            func_type = FunctionType.NUMPY
        except (NameError, AttributeError):
            raise FunctionError(
                f"Numpy function '{function_name}' is not found "
                "in the numpy package."
            )
    elif function_name in _default_functions_registry:
        func, func_type = _default_functions_registry[function_name]
    elif function_name in _udf_registry:
        func, func_type = _udf_registry[function_name], FunctionType.NUMPY
    else:
        raise FunctionError(f"Function '{function_name}' is not found.")

    return func, func_type


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


def register_numpy(function_name: str, function) -> None:
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

    See also
    --------
    register_python : Register Python function as a User Defined Function.

    Notes
    -----
    Numpy package is imported under `np` namespace.
    You can invoke any function from the `np.*` namespace.

    Arguments of the function would be numpy arrays of provided columns.
    UDF can perform vectorized operations on arrays passed as arguments.
    The function would be called only once.

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
    _register_udf(function_name, function)
