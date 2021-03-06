import numpy as np
import pyarrow as pa

from typing import (
    Iterable,
    Tuple,
    Any,
    TYPE_CHECKING,
    FrozenSet,
    List,
    Callable,
    Union,
)


if TYPE_CHECKING:
    from vinum._typing import QueryBaseType
    from vinum.parser.query import Column, Expression

TREE_INDENT_SYMBOL = '  '


def find_all_columns_recursively(
        expressions: Tuple['QueryBaseType', ...],
        skip_shared_expressions: bool = False) -> FrozenSet['Column']:
    """
    Traverse Expressions and return a set of all referenced Columns.

    Parameters
    ----------
    expressions : Tuple['QueryBaseType', ...]
        Expressions to traverse.
    skip_shared_expressions : bool
        Whether to skip shared expressions

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
        elif is_expression(select_expr):
            if skip_shared_expressions and select_expr.is_shared():
                continue
            columns |= find_all_columns_recursively(
                select_expr.arguments,
                skip_shared_expressions=skip_shared_expressions
            )
    return frozenset(columns)


def traverse_exprs(
        expressions: Union['QueryBaseType', Tuple['QueryBaseType', ...]]
) -> Tuple['Expression', ...]:
    """
    Traverse Expressions and return a list of all Expression.

    Parameters
    ----------
    expressions : Tuple['QueryBaseType', ...]
        Expressions to traverse.

    Returns
    -------
    Tuple['Expression']
        Returns the traversal of the expressions tree.
    """
    if not expressions:
        return tuple()
    if not is_iterable(expressions):
        expressions = [expressions]

    traversal = []
    for expr in expressions:  # type: Any
        if is_expression(expr):
            traversal.append(expr)
            traversal.extend(traverse_exprs(expr.arguments))
    return tuple(traversal)


def traverse_exprs_tree(expr: 'QueryBaseType', apply_func: Callable):
    if is_expression(expr):
        for arg in expr.arguments:
            traverse_exprs_tree(arg, apply_func)
    apply_func(expr)


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


def is_vect_expression(obj: Any) -> bool:
    from vinum.core.base import VectorizedExpression
    return isinstance(obj, VectorizedExpression)


def is_numpy_array(array: Any) -> bool:
    return isinstance(array, np.ndarray)


def is_numpy_str_array(array: Any) -> bool:
    return is_numpy_array(array) and np.issubdtype(array.dtype, str)


def is_pyarrow_array(array: Any) -> bool:
    return isinstance(array, pa.Array) or isinstance(array, pa.ChunkedArray)


def is_pyarrow_string(obj: Any) -> bool:
    return isinstance(obj, pa.StringScalar)


def is_iterable(obj: Any) -> bool:
    return isinstance(obj, Iterable) and not isinstance(obj, str)

def is_array_type(obj: Any) -> bool:
    return (isinstance(obj, list)
            or isinstance(obj, tuple)
            or is_numpy_array(obj)
            or is_pyarrow_array(obj)
            )
