import warnings
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Union,
)

import pyarrow as pa
import numpy as np

from vinum._typing import OperatorArgument
from vinum.arrow.record_batch import RecordBatch
from vinum.parser.query import Literal, Column, HasColumnName
from vinum.util.tree_print import RecursiveTreePrint

if TYPE_CHECKING:
    pass


class BaseArgumentsProcessor:
    """
    Base args functionality.
    """
    def __init__(self) -> None:
        super().__init__()
        self._shared_expressions = []

    def _process_arguments(self,
                           arguments: Iterable[OperatorArgument],
                           batch: RecordBatch) -> Tuple:
        return tuple(
            self._process_argument(arg, batch) for arg in arguments
        )

    def _process_argument(self,
                          argument: OperatorArgument,
                          batch: RecordBatch) -> Any:
        if argument is None:
            return

        if isinstance(argument, Literal):
            return self._unpack_literal(argument)
        elif isinstance(argument, Column):
            return self._get_column(argument, batch)
        elif isinstance(argument, VectorizedExpression):
            return self._eval_expression(argument, batch)
        else:
            raise TypeError(
                f'Unsupported OperatorArgument type: {type(argument)}'
            )

    @staticmethod
    def _unpack_literal(value: Literal) -> Any:
        return value.value

    def _get_column(self, column: Column, batch: RecordBatch) -> pa.Array:
        return batch.get_pa_column(column)

    def _eval_expression(self, expression: 'VectorizedExpression',
                         batch: RecordBatch) -> Any:
        return expression.evaluate(batch)


class VectorizedExpression(BaseArgumentsProcessor,
                           HasColumnName,
                           RecursiveTreePrint):
    """
    Vectorized expression.

    Computes expressions on data columns.

    VectorizedExpression arguments is a list of any combination of:
        - `Literal`: represents a generic literal.
        - `Column`: represents a column in the data table.
        - `VectorizedExpression`: represents the result of the execution of
            given expression.
        - `Iterable`: a sequence of the above arguments.
    During the execution, Column arguments would be provided as array-like
    data structures, where possible utilizing read-only views from the
    underlying data table. VectorizedExpression arguments would be executed,
    and the result passed as an array-like structure.
    This processes is called 'arguments resolution' and it makes sure that
    the expression has all the data it needs to compute the expression.
    """
    def __init__(self,
                 arguments: Iterable[OperatorArgument],
                 function: Optional[Callable] = None,
                 is_numpy_func: bool = False,
                 is_binary_func: bool = False,
                 ) -> None:
        super().__init__()
        self._arguments: Tuple[OperatorArgument, ...] = tuple(arguments)

        self._function: Optional[Callable] = function

        self._is_numpy_function: bool = is_numpy_func
        self._is_binary_func: bool = is_binary_func

        self._shared_id: Optional[str] = None

    def evaluate(self, batch: RecordBatch) -> Any:
        """
        Evaluate the expression.

        The Executor ensures that all the Operators in the arguments are
        already executed and the results are available.
        """
        processed_args = self._process_arguments(self._arguments, batch)
        if self._function and processed_args:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                if self._is_binary_func:
                    result = self._apply_binary_args_function(
                        *processed_args
                    )
                else:
                    result = self._function(*processed_args)
        else:
            result = self._expr_kernel(processed_args, batch)

        return self._post_process_result(result)

    def _expr_kernel(self,
                     arguments: Any,
                     batch: RecordBatch) -> Any:
        """
        Expression kernel.

        Override in case `function` constructor parameter is not provided.
        """
        pass

    def _apply_binary_args_function(self, *args: OperatorArgument) -> Any:
        assert self._function is not None
        result = args[0]
        for arg in args[1:]:
            result = self._function(result, arg)

        return result

    @staticmethod
    def _post_process_result(result: Any) -> Any:
        """
        Apply post-processing transformations to the result.
        """
        return result

    def _get_column(self,
                    column: Column,
                    batch: RecordBatch) -> Union[np.ndarray, pa.Array]:
        if self._is_numpy_function:
            return batch.get_np_column(column)
        else:
            return batch.get_pa_column(column)

    def _eval_expression(self,
                         expression: 'VectorizedExpression',
                         batch: RecordBatch) -> Any:
        if (
                expression.is_shared()
                and batch.has_column(expression.get_shared_id())
        ):
            # TODO is this ever invoked or already done in the planner?
            return self._get_column(
                Column(expression.get_shared_id()),
                batch
            )
        else:
            return super()._eval_expression(expression, batch)

    def get_column_name(self) -> str:
        """
        Return column name that may be used to store the result of this
        operation in the data table.

        Returns
        -------
        str
            Column name.
        """
        if self.is_shared():
            assert self._shared_id is not None
            return self._shared_id
        else:
            return str(id(self))

    def is_shared(self) -> bool:
        return bool(self._shared_id)

    def get_shared_id(self) -> str:
        return self._shared_id

    def set_shared_id(self, shared_id: str) -> None:
        self._shared_id = shared_id

    def str_lines_repr(self, indent_level: int,) -> Tuple:
        lines = []

        op_line = (f'{self._level_indent_string(indent_level)}'
                   f'VectorizedExpression: {self.__class__.__name__}')
        lines.append(op_line)

        for arg in self._arguments:
            if isinstance(arg, VectorizedExpression):
                for line in arg.str_lines_repr(indent_level + 1):
                    lines.append(line)
            else:
                lines.append(
                    f'{self._level_indent_string(indent_level + 1)}{arg}'
                )
        lines.append('')

        return tuple(lines)


class Operator(BaseArgumentsProcessor, HasColumnName, RecursiveTreePrint):
    """
    Physical Operator.

    Operator is a basic building block representing minimal,
    atomic data transformation in the pipeline.

    Parameters
    ----------
    parent_operator : Operator
        Parent operator
    arguments : Optional[Iterable[OperatorArgument]]
        List of argument needed to perform data transformation.
    """
    def __init__(self,
                 parent_operator: 'Operator',
                 arguments: Optional[Iterable[OperatorArgument]] = None,
                 ) -> None:
        super().__init__()

        self._parent_operator: 'Operator' = parent_operator

        if arguments is None:
            arguments = []
        self._arguments: Tuple[OperatorArgument, ...] = tuple(arguments)

    def next(self) -> Iterable[RecordBatch]:
        """
        Execute Operator logic and yeild the result.
        """
        for batch in self._parent_operator.next():
            args = self._process_arguments(self._arguments, batch=batch)
            yield self._kernel(batch, args)

    def _kernel(self,
                batch: RecordBatch,
                arguments: Tuple) -> RecordBatch:
        """
        Operator kernel.
        """
        pass

    def str_lines_repr(self, indent_level: int,) -> Tuple:
        lines = []

        op_line = (f'{self._level_indent_string(indent_level)}'
                   f'Operator: {self.__class__.__name__}')
        lines.append(op_line)

        for arg in self._arguments:
            if isinstance(arg, VectorizedExpression):
                for line in arg.str_lines_repr(indent_level + 1):
                    lines.append(line)
            else:
                lines.append(
                    f'{self._level_indent_string(indent_level + 1)}{arg}'
                )
        lines.append('')

        if self._parent_operator:
            for line in self._parent_operator.str_lines_repr(indent_level + 1):
                lines.append(line)

        return tuple(lines)
