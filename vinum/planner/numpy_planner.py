import inspect
from typing import (
    List,
    Set,
    Tuple,
    Callable,
    Iterable,
    Optional,
    TYPE_CHECKING, Type, Union, cast, Any, Dict,
)

from vinum.arrow.arrow_table import ArrowTable
from vinum.core.functions import (
    lookup_udf,
    lookup_aggregate_function,
    is_aggregate_function,
)
from vinum.core.operators.numpy_operators import (
    NumpyOperator,
    BooleanFilterOperator,
    ComputeSelectExpressionsOperator,
    DistinctOperator,
    UpdateTableOperator,
    LikeOperator,
    GroupByArguments,
    HashSplitGroupByOperator,
    TakeGroupByColumnValuesOperator,
    CombineGroupByGroupsOperator,
    AggregateNumpyOperator,
    DropTableColumnsOperator,
    RetainTableColumnsOperator,
    OrderByOperator,
    LimitOperator,
)
from vinum.core.operators.numpy_operator_mappings import (
    SQL_OPERATOR_FUNCTIONS,
    BINARY_OPERATORS,
)
from vinum.errors import PlannerError
from vinum.planner.planner import QueryPlanner
from vinum.core.operators.generic_operators import (
    Operator,
    SerialExecutorOperator,
    OperatorBaseType,
)
from vinum.parser.query import (
    SQLOperator,
    Expression,
    Column,
    HasAlias,
)
from vinum.util.util import is_expression, is_column, is_literal, is_operator

if TYPE_CHECKING:
    from vinum._typing import QueryBaseType


class NumpyQueryPlanner(QueryPlanner):
    """
    Numpy query planner.

    Create query plan based on numpy operators.
    """

    @staticmethod
    def _instantiate_operator(op: Union[Callable, Type[Operator]],
                              default_class: Type[Operator],
                              arguments: Iterable[OperatorBaseType],
                              table: ArrowTable,
                              is_binary_op: bool = False) -> Operator:
        """
        Instantiate Operator.

        If operator is a sub-class of NumpyOperator, create an instance
        of this class.
        If operator is a function, create an instance of 'default_class' and
        pass it 'op' as a function argument.

        Parameters
        ----------
        op : Callable
            Operator class or function.
        default_class : Callable
            In case operator function is provided,
            create instance of 'default_class', passing 'op' as a function.
        arguments : List[OperatorBaseType]
            Arguments.
        table : ArrowTable
            Arrow table.

        Returns
        -------
        Operator
            Operator instance.
        """
        if (inspect.isclass(op)
                and issubclass(op, NumpyOperator)):  # type: ignore
            return op(arguments, table)
        else:
            return default_class(arguments,
                                 table,
                                 function=op,
                                 is_binary_op=is_binary_op)

    def _process_expressions_tree(
            self,
            expr: Expression,
            processed_shared_ids: Set[str]
    ) -> OperatorBaseType:
        """
        Recursively process Expressions tree and create Operators tree.

        Essentially, transform Expressions into Operator(s)
        implementing given Expression.

        Parameters
        ----------
        expr : Expression
            Expression to process.
        processed_shared_ids : Set[str]
            Set of shared expression IDs, that are already processed.

        Returns
        -------
        OperatorBaseType
            Column or Operator instance.
        """
        # if not expr:
        #     return None
        assert expr
        if (
                expr.is_shared()
                and expr.get_shared_id() in processed_shared_ids
        ):
            return Column(expr.get_shared_id())

        arguments: List[OperatorBaseType] = []

        for arg in expr.arguments:  # type: Any
            if is_expression(arg):
                arg = self._process_expressions_tree(
                    arg,
                    processed_shared_ids
                )
            arguments.append(arg)

        if expr.sql_operator in SQL_OPERATOR_FUNCTIONS.keys():
            operator = self._instantiate_operator(
                SQL_OPERATOR_FUNCTIONS[expr.sql_operator],
                NumpyOperator,
                arguments,
                self._table,
                is_binary_op=(expr.sql_operator in BINARY_OPERATORS)
            )

        elif expr.sql_operator in (SQLOperator.LIKE, SQLOperator.NOT_LIKE):
            operator = LikeOperator(
                tuple(arguments),   # type: ignore
                expr.sql_operator == SQLOperator.NOT_LIKE,
                self._table
            )
        elif expr.sql_operator == SQLOperator.FUNCTION:
            assert expr.function_name

            function_name = expr.function_name.lower()  # type: str

            if is_aggregate_function(function_name):
                agg_function = lookup_aggregate_function(function_name)
                operator = self._instantiate_operator(
                    agg_function,
                    AggregateNumpyOperator,
                    arguments,
                    self._table
                )
            else:
                func = lookup_udf(function_name)
                operator = self._instantiate_operator(
                    func,
                    NumpyOperator,
                    arguments,
                    self._table
                )
        else:
            raise PlannerError(f'Unsupported SQLOperator: {expr.sql_operator}')

        if expr.is_shared():
            operator.set_shared_id(expr.get_shared_id())
            processed_shared_ids.add(expr.get_shared_id())

        return operator

    def _create_boolean_filter_op(
            self,
            filter_expression: Expression,
            processed_shared_ids: Optional[Set[str]] = None
    ) -> BooleanFilterOperator:
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

        filter_condition = self._process_expressions_tree(
            filter_expression,
            processed_shared_ids
        )  # type: ignore

        return BooleanFilterOperator(tuple((filter_condition,)), self._table)

    def _process_expressions(
            self,
            expressions: Tuple['QueryBaseType', ...],
            processed_shared_ids: Set[str]
    ) -> Tuple[OperatorBaseType, ...]:
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
        Tuple[OperatorBaseType, ...]
            List of Operators, Columns or Literals.
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
    def _internal_column_names(
            expressions: Tuple['QueryBaseType', ...]) -> Tuple[str, ...]:
        """
        Return table column names for a list of OperatorBaseType instances.

        Also includes column names for temporary columns, which wouldn't be
        included in the final output.

        Parameters
        ----------
        expressions : Tuple['QueryBaseType', ...]
            A list of Expressions.

        Returns
        -------
        Tuple[str, ...]
            Column names.
        """
        column_names = []
        unnamed_count = 0

        for expr in expressions:    # type: Any
            if is_column(expr):
                name = expr.get_column_name()
            elif is_expression(expr) and expr.is_shared():
                name = expr.get_shared_id()
            elif is_literal(expr):
                name = expr.get_alias()
            else:
                name = f'col_{unnamed_count}'
                unnamed_count += 1
            column_names.append(name)

        return tuple(column_names)

    @staticmethod
    def _final_column_names(
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

    def _set_split_operator(self,
                            operator: Operator,
                            split_operator: Operator) -> None:
        """
        Recursively set depends on 'split_operator' for given Operators tree.
        """
        if not is_operator(operator):
            return
        is_leaf = True
        for arg in operator.arguments:
            if is_operator(arg):
                is_leaf = False
                self._set_split_operator(arg, split_operator)   # type: ignore
        if is_leaf and is_operator(operator):
            operator.set_depends_on(split_operator)

    def plan_query(self) -> Operator:
        drop_columns_op = None  # type: ignore
        used_columns = self._query.get_all_used_column_names()
        if used_columns:
            unused_columns = set(self._table.column_names) - used_columns
            if unused_columns:
                drop_columns_op = DropTableColumnsOperator(
                    unused_columns,
                    self._table)

        if self._query.where_condition:
            where_tree = self._create_boolean_filter_op(
                self._query.where_condition
            )
        else:
            where_tree = None   # type: ignore

        processed_shared_ids: Set[str] = set()
        if self._query.is_group_by():
            group_by_args = GroupByArguments(
                self._process_expressions(self._query.group_by,
                                          processed_shared_ids)
            )
            select_columns = self._process_expressions(
                self._query.get_select_plus_post_processing_columns(),
                processed_shared_ids)
            group_by_split_op = HashSplitGroupByOperator(
                select_columns,
                group_by_args,
                self._table
            )

            groupby_exprs_columns = group_by_args.get_group_by_expr_names()
            select_columns_agg = []
            for column in select_columns:   # type: Any
                if (is_operator(column)
                        and column.get_column_name() in groupby_exprs_columns):
                    column = Column(column.get_column_name())

                if is_column(column):
                    column = TakeGroupByColumnValuesOperator(
                        column,
                        group_by_split_op
                    )
                select_columns_agg.append(column)
                self._set_split_operator(column, group_by_split_op)

            select_exprs_op = CombineGroupByGroupsOperator(
                select_columns_agg,
                self._table
            )   # type: Operator
        else:
            select_columns = self._process_expressions(
                self._query.get_select_plus_post_processing_columns(),
                processed_shared_ids
            )
            select_exprs_op = ComputeSelectExpressionsOperator(
                select_columns,
                self._table
            )

        if self._query.distinct and not self._query.is_group_by():
            select_exprs_op = DistinctOperator((select_exprs_op,), self._table)

        update_table_with_select = UpdateTableOperator(
            (select_exprs_op,),
            self._internal_column_names(
                self._query.get_select_plus_post_processing_columns()),
            self._table
        )

        if self._query.having:
            having = self._create_boolean_filter_op(
                self._query.having,
                processed_shared_ids
            )
        else:
            having = None   # type: ignore

        order_by_op = None
        if self._query.order_by:
            order_by_op = OrderByOperator(
                self._process_expressions(self._query.order_by,
                                          processed_shared_ids),
                self._query.sort_order,
                self._table
            )

        rename_columns_op = RetainTableColumnsOperator(
            len(self._query.select_expressions),
            self._final_column_names(self._query.select_expressions),
            self._table
        )

        limit_op = None
        if self._query.has_limit():
            limit_op = LimitOperator(self._query.limit,  # type: ignore
                                     self._query.offset,
                                     self._table)

        operators = tuple(
            op
            for op in (
                drop_columns_op,
                where_tree,
                update_table_with_select,
                having,
                order_by_op,
                rename_columns_op,
                limit_op
            )
            if op
        )

        return SerialExecutorOperator(operators, self._table)
