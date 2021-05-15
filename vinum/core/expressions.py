from functools import partial

import numpy as np
import pyarrow.compute as pc

from vinum.core.functions import (
    ConcatFunction,
    FunctionType,
)
from vinum.parser.query import SQLExpression

EXPRESSION_FUNCTIONS = {
    SQLExpression.NEGATION: (np.negative, FunctionType.NUMPY),
    SQLExpression.BINARY_NOT: (lambda x: ~x, FunctionType.NUMPY),
    SQLExpression.BINARY_AND: (np.bitwise_and, FunctionType.NUMPY),
    SQLExpression.BINARY_OR: (np.bitwise_or, FunctionType.NUMPY),
    SQLExpression.BINARY_XOR: (np.bitwise_xor, FunctionType.NUMPY),

    # Math operators
    SQLExpression.ADDITION: (np.add, FunctionType.NUMPY),
    SQLExpression.SUBTRACTION: (np.subtract, FunctionType.NUMPY),
    SQLExpression.MULTIPLICATION: (np.multiply, FunctionType.NUMPY),
    SQLExpression.DIVISION: (np.divide, FunctionType.NUMPY),
    SQLExpression.MODULUS: (np.mod, FunctionType.NUMPY),

    # Boolean operators
    SQLExpression.AND: (pc.and_, FunctionType.ARROW),
    SQLExpression.OR: (pc.or_, FunctionType.ARROW),
    SQLExpression.NOT: (pc.invert, FunctionType.ARROW),
    SQLExpression.EQUALS: (lambda x, y: x == y, FunctionType.NUMPY),
    SQLExpression.NOT_EQUALS: (lambda x, y: x != y, FunctionType.NUMPY),
    SQLExpression.GREATER_THAN: (lambda x, y: x > y, FunctionType.NUMPY),
    SQLExpression.GREATER_THAN_OR_EQUAL:
        (lambda x, y: x >= y, FunctionType.NUMPY),
    SQLExpression.LESS_THAN: (lambda x, y: x < y, FunctionType.NUMPY),
    SQLExpression.LESS_THAN_OR_EQUAL: (lambda x, y: x <= y, FunctionType.NUMPY),
    SQLExpression.IS_NULL: (pc.is_null, FunctionType.ARROW),
    SQLExpression.IS_NOT_NULL: (pc.is_valid, FunctionType.ARROW),
    SQLExpression.IN: (np.isin, FunctionType.NUMPY),
    SQLExpression.NOT_IN: (partial(np.isin, invert=True), FunctionType.NUMPY),

    # SQL specific operators
    SQLExpression.BETWEEN:
        (lambda x, low, high: np.logical_and(x >= low, x <= high),
         FunctionType.NUMPY),
    SQLExpression.NOT_BETWEEN:
        (lambda x, low, high: np.logical_or(x < low, x > high),
         FunctionType.NUMPY),

    # String operators
    SQLExpression.CONCAT: (ConcatFunction, FunctionType.CLASS),
}


BINARY_EXPRESSIONS = {
    SQLExpression.ADDITION,
    SQLExpression.SUBTRACTION,
    SQLExpression.MULTIPLICATION,
    SQLExpression.DIVISION,
    SQLExpression.MODULUS,
    SQLExpression.AND,
    SQLExpression.OR,
    SQLExpression.EQUALS,
    SQLExpression.NOT_EQUALS,
    SQLExpression.GREATER_THAN,
    SQLExpression.GREATER_THAN_OR_EQUAL,
    SQLExpression.LESS_THAN,
    SQLExpression.LESS_THAN_OR_EQUAL,
}
