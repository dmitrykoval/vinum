from functools import partial

import numpy as np
import pyarrow.compute as pc

from vinum.core.functions import (
    ConcatFunction,
    FunctionType,
)
from vinum.parser.query import SQLOperator

SQL_OPERATOR_FUNCTIONS = {
    SQLOperator.NEGATION: (np.negative, FunctionType.NUMPY),
    SQLOperator.BINARY_NOT: (lambda x: ~x, FunctionType.NUMPY),
    SQLOperator.BINARY_AND: (np.bitwise_and, FunctionType.NUMPY),
    SQLOperator.BINARY_OR: (np.bitwise_or, FunctionType.NUMPY),
    SQLOperator.BINARY_XOR: (np.bitwise_xor, FunctionType.NUMPY),

    # Math operators
    SQLOperator.ADDITION: (np.add, FunctionType.NUMPY),
    SQLOperator.SUBTRACTION: (np.subtract, FunctionType.NUMPY),
    SQLOperator.MULTIPLICATION: (np.multiply, FunctionType.NUMPY),
    SQLOperator.DIVISION: (np.divide, FunctionType.NUMPY),
    SQLOperator.MODULUS: (np.mod, FunctionType.NUMPY),

    # Boolean operators
    SQLOperator.AND: (pc.and_, FunctionType.ARROW),
    SQLOperator.OR: (pc.or_, FunctionType.ARROW),
    SQLOperator.NOT: (pc.invert, FunctionType.ARROW),
    SQLOperator.EQUALS: (lambda x, y: x == y, FunctionType.NUMPY),
    SQLOperator.NOT_EQUALS: (lambda x, y: x != y, FunctionType.NUMPY),
    SQLOperator.GREATER_THAN: (lambda x, y: x > y, FunctionType.NUMPY),
    SQLOperator.GREATER_THAN_OR_EQUAL:
        (lambda x, y: x >= y, FunctionType.NUMPY),
    SQLOperator.LESS_THAN: (lambda x, y: x < y, FunctionType.NUMPY),
    SQLOperator.LESS_THAN_OR_EQUAL: (lambda x, y: x <= y, FunctionType.NUMPY),
    SQLOperator.IS_NULL: (pc.is_null, FunctionType.ARROW),
    SQLOperator.IS_NOT_NULL: (pc.is_valid, FunctionType.ARROW),
    SQLOperator.IN: (np.isin, FunctionType.NUMPY),
    SQLOperator.NOT_IN: (partial(np.isin, invert=True), FunctionType.NUMPY),

    # SQL specific operators
    SQLOperator.BETWEEN:
        (lambda x, low, high: np.logical_and(x >= low, x <= high),
         FunctionType.NUMPY),
    SQLOperator.NOT_BETWEEN:
        (lambda x, low, high: np.logical_or(x < low, x > high),
         FunctionType.NUMPY),

    # String operators
    SQLOperator.CONCAT: (ConcatFunction, FunctionType.CLASS),
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
