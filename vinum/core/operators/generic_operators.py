import warnings
from typing import (
    Any,
    Callable,
    Iterable,
    Optional,
    Set,
    TYPE_CHECKING,
    Tuple,
)

from vinum._typing import OperatorBaseType
from vinum.arrow.arrow_table import ArrowTable
from vinum.parser.query import Literal, Column, HasColumnName
from vinum.util.tree_print import RecursiveTreePrint

if TYPE_CHECKING:
    import vinum.executor.executor


class Operator(HasColumnName, RecursiveTreePrint):
    """
    Data transformation Operator.

    Operator is a basic building block representing minimal,
    atomic data transformation.
    Operator provides basic methods for interaction with underlying
    data table, performs arguments resolution and communicates with Executor.

    There are two ways to define data transformation with Operator:
        1. By providing a function via `function` parameter in the constructor.
            In this case, during the execution Operator would resolve
            all the arguments and invoke `function` passing the args.
        2. By overriding protected `_run` method.
            In case of a more complex data transformation logic,
            it is recommended to subclcass Operator and override `_run` method.
            Arguments are available via the `self._processed_args`
            property.

    Operator arguments is a list of any combination of:
        - `Literal`: represents a generic literal.
        - `Column`: represents a column in the data table.
        - `Operator`: represents the result of the execution of given operator.
        - `Iterable`: a sequence of the above arguments.
    During the execution, Column arguments would be provided as array-like
    data structures, where possible utilizing read-only views from the
    underlying data table. Operator arguments would be executed,
    and the result passed as an array-like structure.
    This processes is called 'arguments resolution' and it makes sure that
    the operator has all the data it needs to perform data transformation.

    Shared property indicates that the result of this operator is used
    in multiple places, for example and SELECT clause and ORDER BY clause.

    Parameters
    ----------
    arguments : List[OperatorBaseType]
        List of argument needed to perform data transformation.
    table : ArrowTable
        Data table. Use of data table is optional and depends on
        the transformation.
    function : Optional[Callable]
        Function to perform data transformation.
        During the execution, resolved arguments would be passed:
        `function(*args)`.
    is_binary_op : bool
        True if the operation is binary, ie has two operands: `c1 AND c2`.
    depends_on : Optional['Operator']
        Do not execute until the provided operator execution is finished.
    """
    def __init__(self,
                 arguments: Iterable[OperatorBaseType],
                 table: ArrowTable,
                 function: Optional[Callable] = None,
                 is_binary_op: bool = False,
                 depends_on: Optional['Operator'] = None
                 ) -> None:
        super().__init__()
        self.arguments: Tuple[OperatorBaseType, ...] = tuple(arguments)
        self.table: ArrowTable = table

        self.function: Optional[Callable] = function
        self.depends_on: Optional['Operator'] = depends_on

        self._processed_args: Tuple = tuple()
        self._is_binary_op: bool = is_binary_op

        self._shared_id: Optional[str] = None
        self._executor: 'vinum.executor.executor.Executor' = None

    def set_table(self, table: ArrowTable) -> None:
        self.table = table

    def set_depends_on(self, depends_on_operator: 'Operator') -> None:
        self.depends_on = depends_on_operator

    def is_shared(self) -> bool:
        return bool(self._shared_id)

    def get_shared_id(self) -> str:
        return self._shared_id

    def set_shared_id(self, shared_id: str) -> None:
        self._shared_id = shared_id

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

    def set_executor(self,
                     executor: 'vinum.executor.executor.Executor') -> None:
        self._executor = executor

    def get_child_operators(self) -> Tuple['Operator', ...]:
        """
        Return all the operators that should be executed before.
        """
        operators = []
        if self.depends_on:
            operators.append(self.depends_on)
        for arg in self.arguments:
            if isinstance(arg, Operator):
                operators.append(arg)
        return tuple(operators)

    def execute(self) -> Any:
        """
        Execute Operator and return the results.

        The Executor ensures that all the Operators in the arguments are
        already executed and the results are available.
        """
        self._process_arguments()
        if self.function and self._processed_args:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=RuntimeWarning)
                if self._is_binary_op:
                    return self._apply_binary_args_function(
                        *self._processed_args
                    )
                else:
                    return self.function(*self._processed_args)
        else:
            return self._run()

    def _run(self) -> Any:
        """
        Run operator data transformation logic.

        Override in case `function` constructor parameter is not provided.
        """
        pass

    def _apply_binary_args_function(self, *args: OperatorBaseType) -> Any:
        assert self.function is not None
        result = args[0]
        for arg in args[1:]:
            result = self.function(result, arg)

        return result

    def _process_arguments(self) -> None:
        args = []
        for arg in self.arguments:
            args.append(self._resolve_argument(arg))
        self._processed_args = tuple(args)

    def _resolve_argument(self, argument: OperatorBaseType) -> Any:
        if argument is None:
            return

        if isinstance(argument, Literal):
            return self._resolve_value(argument)
        elif isinstance(argument, Column):
            return self._resolve_column(argument)
        elif isinstance(argument, Operator):
            operator = argument
            if (
                    operator.is_shared()
                    and self.table.has_column(operator.get_shared_id())
            ):
                return self._resolve_column(Column(operator.get_shared_id()))
            else:
                return self._resolve_operator(operator)
        elif isinstance(argument, Iterable):
            return [self._resolve_argument(arg) for arg in argument]
        else:
            raise TypeError(
                f'Unsupported OperatorArgument type: {type(argument)}'
            )

    def _resolve_value(self, value: Literal) -> Any:
        return value.value

    def _resolve_column(self, column: Column) -> Any:
        pass

    def _resolve_operator(self, operator: 'Operator') -> Any:
        assert self._executor is not None
        return self._executor.get_operator_result(operator, self.table)

    def str_lines_repr(self,
                       indent_level: int,
                       processed: Set = None) -> Tuple:
        if processed is None:
            processed = set()

        lines = []

        op_line = (f'{self._level_indent_string(indent_level)}'
                   f'Operator: {self.__class__.__name__}')
        if self.function:
            op_line = f'{op_line}, function: {self.function}'
        lines.append(op_line)

        args = list(self.arguments)
        if self.depends_on:
            args = args + [self.depends_on]
        for arg in args:
            if isinstance(arg, Operator):
                if arg in processed:
                    continue
                for line in arg.str_lines_repr(indent_level + 1, processed):
                    lines.append(line)
            else:
                lines.append(
                    f'{self._level_indent_string(indent_level + 1)}{arg}'
                )
        lines.append('')

        processed.add(self)

        return tuple(lines)


class AggregateOperator(Operator):
    """
    Abstract Base Class for all aggregate operators.
    """
    pass


class AbstractCombineGroupByGroupsOperator(Operator):
    """
    Abstract Base Class for Combine Group By Operator.

    Executor uses this class to mark an end of the Group By phase.
    """
    pass


class SerialExecutorOperator(Operator):
    """
    Serial Executor.

    Execute operators in the order of arguments list.
    Executor avoids scheduling arguments of this operator in parallel
    fashion.
    """
