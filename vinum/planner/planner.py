import pyarrow as pa

from vinum.arrow.arrow_table import ArrowTable
from vinum.parser.query import Query

from typing import (
    List,
    Set,
    Tuple,
    Callable,
    Iterable,
    Optional,
    TYPE_CHECKING,
    Type,
    Union,
    cast,
    Any,
    Dict,
)

from vinum.core.aggregate import AggregateOperator, AggregateFunction
from vinum.core.udf import (
    lookup_udf,
)
from vinum.core.algebra import (
    SortOperator,
    SliceOperator,
    ProjectOperator,
    TableReaderOperator,
    FilterOperator,
    MaterializeTableOperator,
    EmptyTableReaderOperator,
    FileReaderOperator,
)
from vinum.core.functions import (
    LikeFunction,
    is_aggregate_func,
    FunctionType,
    ensure_numpy_mapping,
)
from vinum.core.sql_operators_mapping import (
    SQL_OPERATOR_FUNCTIONS,
    BINARY_OPERATORS, )
from vinum.errors import PlannerError
from vinum.core.base import (
    Operator,
    VectorizedExpression,
)
from vinum.parser.query import (
    SQLOperator,
    Expression,
    Column,
    HasAlias,
)
from vinum.util.util import is_expression, is_literal

if TYPE_CHECKING:
    from vinum._typing import QueryBaseType, OperatorArgument
    from vinum import StreamReader


class QueryPlanner:
    """
    Query planner.

    Query Planner is responsible for transforming a Query AST into
    a query plan.

    Essentially, query planner outlines the steps needed to execute the query.

    Parameters
    ----------
    query : Query
        Query syntax tree.
    table : ArrowTable
        Data table.
    reader : StreamReader
        Data stream reader.
    """
    def __init__(self,
                 query: Query,
                 table: ArrowTable = None,
                 reader: 'StreamReader' = None) -> None:
        self._query = query
        self._table = table
        self._reader: pa.RecordBatchFileReader = (
            reader.reader if reader else None
        )

        if table:
            self._schema = table.get_schema()
        elif reader:
            self._schema = self._reader.schema
        else:
            raise PlannerError('Either table or reader has to be provided.')

    @staticmethod
    def _new_vectorized_expression(
            kernel: Union[Callable, Type[VectorizedExpression]],
            arguments: Iterable['OperatorArgument'],
            func_type: FunctionType,
            is_binary_func: bool = False
    ) -> VectorizedExpression:
        """
        Instantiate VectorizedExpression.

        Parameters
        ----------
        kernel : Callable
            Operator class or function.
        arguments : List[OperatorArgument]
            Arguments.
        func_type : FunctionType
            Function type: Arrow, Numpy or Class.
        is_binary_func : bool
            Is binary args function.

        Returns
        -------
        Operator
            Operator instance.
        """
        if func_type == FunctionType.CLASS:
            return kernel(arguments)
        else:
            return VectorizedExpression(
                arguments=arguments,
                function=kernel,
                is_numpy_func=(func_type == FunctionType.NUMPY),
                is_binary_func=is_binary_func
            )

    def _process_expressions_tree(
            self,
            expr: Expression,
            processed_shared_ids: Set[str]
    ) -> VectorizedExpression:
        """
        Recursively process Expressions tree and create VectorizedExpression
        tree.

        Essentially, transform Expressions into VectorizedExpression(s)
        implementing given Expression.

        Parameters
        ----------
        expr : Expression
            Expression to process.
        processed_shared_ids : Set[str]
            Set of shared expression IDs, that are already processed.

        Returns
        -------
        VectorizedExpression
            VectorizedExpression.
        """
        assert expr
        if (
                expr.is_shared()
                and expr.get_shared_id() in processed_shared_ids
        ):
            return Column(expr.get_shared_id())

        arguments: List[OperatorArgument] = []

        for arg in expr.arguments:  # type: Any
            if is_expression(arg):
                arg = self._process_expressions_tree(
                    arg,
                    processed_shared_ids
                )
            arguments.append(arg)

        if expr.sql_operator in SQL_OPERATOR_FUNCTIONS.keys():
            func, func_type = SQL_OPERATOR_FUNCTIONS[expr.sql_operator]
            vec_expr = self._new_vectorized_expression(
                kernel=func,
                arguments=arguments,
                func_type=func_type,
                is_binary_func=(expr.sql_operator in BINARY_OPERATORS)
            )

        elif expr.sql_operator in (SQLOperator.LIKE, SQLOperator.NOT_LIKE):
            vec_expr = LikeFunction(
                tuple(arguments),   # type: ignore
                expr.sql_operator == SQLOperator.NOT_LIKE,
            )
        elif expr.sql_operator == SQLOperator.FUNCTION:
            assert expr.function_name

            function_name = expr.function_name.lower()  # type: str

            if is_aggregate_func(function_name):
                if (function_name == 'count'
                        and (not arguments
                             or (is_literal(arguments[0]) and arguments[0].value == '*')
                             or (isinstance(arguments[0], str) and arguments[0] == '*'))):
                    function_name = 'count_star'
                    arguments = [""]
                function_name = ensure_numpy_mapping(function_name)

                vec_expr = AggregateFunction(function_name, *arguments)
                if not expr.is_shared():
                    expr.set_shared_id(f'{function_name}_{id(expr)}')
            else:
                func, func_type = lookup_udf(function_name)
                vec_expr = self._new_vectorized_expression(
                    kernel=func,
                    arguments=arguments,
                    func_type=func_type,
                    is_binary_func=(expr.sql_operator in BINARY_OPERATORS)
                )

        else:
            raise PlannerError(f'Unsupported SQLOperator: {expr.sql_operator}')

        if expr.is_shared():
            vec_expr.set_shared_id(expr.get_shared_id())
            processed_shared_ids.add(expr.get_shared_id())

        return vec_expr

    def _new_filter_operator(
            self,
            filter_expression: Expression,
            parent_operator: Operator,
            processed_shared_ids: Optional[Set[str]] = None
    ) -> FilterOperator:
        """
        Create boolean Operator


        Parameters
        ----------
        filter_expression : Expression
            Boolean expression.
        processed_shared_ids : Optional[Set[str]]
            Set of shared expression IDs, that are already processed.

        Returns
        -------
        BooleanFilterOperator
            Instance of BooleanFilterOperator.
        """
        assert filter_expression
        if not processed_shared_ids:
            processed_shared_ids = set()

        predicate = self._process_expressions_tree(
            filter_expression,
            processed_shared_ids
        )  # type: ignore

        return FilterOperator(predicate, parent_operator)

    def _process_expressions(
            self,
            expressions: Tuple['QueryBaseType', ...],
            processed_shared_ids: Set[str]
    ) -> Tuple['OperatorArgument', ...]:
        """
        Process a list of expressions.

        Parameters
        ----------
        expressions : Tuple['QueryBaseType', ...]
            A list of Expressions to process.
        processed_shared_ids : Set[str]
            Set of shared expression IDs, that are already processed.

        Returns
        -------
        Tuple[OperatorArgument, ...]
            List of VectorizedExpression, Column or Literal.
        """
        operators = []

        for expr in expressions:
            if is_expression(expr):
                operator_expr = self._process_expressions_tree(
                    expr,   # type: ignore
                    processed_shared_ids
                )
            else:
                operator_expr = expr    # type: ignore
            operators.append(operator_expr)

        return tuple(operators)

    @staticmethod
    def _column_names(
            expressions: Tuple['QueryBaseType', ...]
    ) -> Tuple[str, ...]:
        """
        Return final output column names for all expressions.

        Parameters
        ----------
        expressions : Tuple['QueryBaseType', ...]
            A list of Expressions.

        Returns
        -------
        Tuple[str, ...]
            Column names.
        """

        column_names: List[str] = []
        column_names_index: Dict[str, int] = {}
        unnamed_count = 0

        for select_expr in expressions:
            if isinstance(select_expr, HasAlias) and select_expr.get_alias():
                name = select_expr.get_alias()
                name = cast(str, name)
            else:
                name = f'col_{unnamed_count}'
                unnamed_count += 1

            if name in column_names_index:
                column_names_index[name] = column_names_index[name] + 1
                name = f'{name}_{column_names_index[name]}'
            else:
                column_names_index[name] = 0

            column_names.append(name)

        return tuple(column_names)

    def plan_query(self) -> Operator:
        """
        Create a query execution plan.

        Using a query AST, generate a query plan outlining the operations
        needed to be performed to execute a query.

        Returns
        -------
        Operator
            Query plan.
        """
        processed_shared_ids: Set[str] = set()

        used_columns = self._query.get_all_used_column_names()
        unused_columns = set(self._schema.names) - used_columns
        skip_table = False
        # Need to test for the cases like count(*),
        # when all columns will get removed.
        project_args = used_columns
        if unused_columns:
            if len(unused_columns) < len(self._schema.names):
                project_args = self._query.get_all_used_columns()
            elif self._query.has_count_star():
                project_args = [Column(self._schema.names[0])]
            else:
                skip_table = True

        if self._reader:
          current_op = FileReaderOperator(self._reader)
        elif skip_table:
            current_op = EmptyTableReaderOperator()
        else:
            current_op = TableReaderOperator(self._table)

        if unused_columns and not skip_table:
            current_op = ProjectOperator(
                arguments=project_args,
                parent_operator=current_op
            )

        if self._query.where_condition:
            current_op = self._new_filter_operator(
                filter_expression=self._query.where_condition,
                parent_operator=current_op,
                processed_shared_ids=processed_shared_ids
            )

        group_by_exprs = self._query.group_by
        if self._query.distinct:
            group_by_exprs += self._query.select_expressions

        if self._query.is_aggregate():
            inner_agg_exprs = []
            for expr in self._query.select_expressions:
                if (is_expression(expr)
                        and is_aggregate_func(expr.function_name)):
                    project_args = []
                    has_inner = False
                    for inner_expr in expr.arguments:
                        if is_expression(inner_expr):
                            if not inner_expr.is_shared():
                                inner_col_id = str(id(inner_expr))
                                inner_expr.set_shared_id(inner_col_id)
                            inner_agg_exprs.append(inner_expr)
                            inner_expr = Column(inner_expr.get_shared_id())
                            has_inner = True
                        project_args.append(inner_expr)
                    if has_inner:
                        expr.set_arguments(tuple(project_args))
            group_by = []
            for expr in group_by_exprs:
                if is_expression(expr):
                    if not expr.is_shared():
                        inner_col_id = str(id(expr))
                        expr.set_shared_id(inner_col_id)
                    inner_agg_exprs.append(expr)
                    expr = Column(expr.get_shared_id())
                group_by.append(expr)

            if inner_agg_exprs:
                current_op = ProjectOperator(
                    arguments=self._process_expressions(
                        tuple(inner_agg_exprs), processed_shared_ids),
                    parent_operator=current_op,
                    keep_input_table=True
                )
            agg_exprs = self._process_expressions(
                self._query.get_select_plus_post_agg_cols(),
                processed_shared_ids
            )
            current_op = AggregateOperator(
                parent_operator=current_op,
                group_by_columns=self._process_expressions(
                    group_by, processed_shared_ids),
                agg_exprs=agg_exprs
            )

        if self._query.having:
            current_op = self._new_filter_operator(
                filter_expression=self._query.having,
                parent_operator=current_op,
                processed_shared_ids=processed_shared_ids
            )

        if self._query.order_by:
            current_op = SortOperator(
                self._process_expressions(self._query.order_by,
                                          processed_shared_ids),
                self._query.sort_order,
                current_op
            )

        agg_exprs = self._process_expressions(
            self._query.select_expressions,
            processed_shared_ids
        )
        current_op = ProjectOperator(
            arguments=agg_exprs,
            parent_operator=current_op,
            col_names=self._column_names(self._query.select_expressions)
        )

        if self._query.has_limit():
            current_op = SliceOperator(
                self._query.limit,  # type: ignore
                self._query.offset,
                current_op
            )

        current_op = MaterializeTableOperator(
            parent_operator=current_op
        )

        return current_op
