from functools import partial

import numpy as np

from vinum.core.operators.numpy_function_operators import (
    CountOperator,
    ConcatOperator,
)
from vinum.parser.query import SQLOperator
from vinum.util.util import is_null_mask, is_not_null_mask

SQL_OPERATOR_FUNCTIONS = {
    # Unary operators
    SQLOperator.NEGATION: lambda x: -x,
    SQLOperator.BINARY_NOT: lambda x: ~x,

    # Math operators
    SQLOperator.ADDITION: lambda x, y: x + y,
    SQLOperator.SUBTRACTION: lambda x, y: x - y,
    SQLOperator.MULTIPLICATION: lambda x, y: x * y,
    SQLOperator.DIVISION: lambda x, y: x / y,
    SQLOperator.MODULUS: lambda x, y: x % y,

    # Boolean operators
    SQLOperator.AND: lambda x, y: x & y,
    SQLOperator.OR: lambda x, y: x | y,
    SQLOperator.NOT: lambda x: np.logical_not(x),
    SQLOperator.EQUALS: lambda x, y: x == y,
    SQLOperator.NOT_EQUALS: lambda x, y: x != y,
    SQLOperator.GREATER_THAN: lambda x, y: x > y,
    SQLOperator.GREATER_THAN_OR_EQUAL: lambda x, y: x >= y,
    SQLOperator.LESS_THAN: lambda x, y: x < y,
    SQLOperator.LESS_THAN_OR_EQUAL: lambda x, y: x <= y,
    SQLOperator.IS_NULL: is_null_mask,
    SQLOperator.IS_NOT_NULL: is_not_null_mask,
    SQLOperator.IN: np.isin,
    SQLOperator.NOT_IN: partial(np.isin, invert=True),

    # Binary operators
    SQLOperator.BINARY_AND: np.bitwise_and,
    SQLOperator.BINARY_OR: np.bitwise_or,

    # SQL specific operators
    SQLOperator.BETWEEN: lambda x, low, high:
        np.logical_and(x >= low, x <= high),
    SQLOperator.NOT_BETWEEN: lambda x, low, high:
        np.logical_or(x < low, x > high),

    # String operators
    SQLOperator.CONCAT: ConcatOperator,

}


BINARY_OPERATORS = {
    SQLOperator.ADDITION,
    SQLOperator.SUBTRACTION,
    SQLOperator.MULTIPLICATION,
    SQLOperator.DIVISION,
    SQLOperator.MODULUS,
    SQLOperator.AND,
    SQLOperator.OR,
    SQLOperator.EQUALS,
    SQLOperator.NOT_EQUALS,
    SQLOperator.GREATER_THAN,
    SQLOperator.GREATER_THAN_OR_EQUAL,
    SQLOperator.LESS_THAN,
    SQLOperator.LESS_THAN_OR_EQUAL,
}


AGGREGATE_OPERATORS = {
    'count': CountOperator,
    'sum': np.nansum,
    'min': np.nanmin,
    'max': np.nanmax,
    'avg': np.nanmean,
    'mean': np.nanmean,
    'median': np.nanmedian,
    'var': np.nanvar,
    'std': np.nanstd,
}
