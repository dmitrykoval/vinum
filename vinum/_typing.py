from typing import Union, Tuple, List, Dict, TYPE_CHECKING

import numpy as np

from pyarrow.lib import Array, ChunkedArray

from vinum.parser.query import Literal, Column, Expression

if TYPE_CHECKING:
    from vinum.core.base import Operator
    from vinum.core.base import VectorizedExpression

ParserArgType = Union[Dict, List, str, int, float]
QueryBaseType = Union[Literal, Column, Expression]

OperatorBaseType = Union[Literal, Column, 'Operator']

OperatorArgument = Union[Literal, Column, 'VectorizedExpression']


PythonArray = Union[Tuple, List]
PyArrowArray = Union[Array, ChunkedArray]

AnyArrayLike = Union[PythonArray, PyArrowArray, np.ndarray]
