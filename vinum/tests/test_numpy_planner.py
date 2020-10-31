import pytest

from vinum.planner.numpy_planner import NumpyQueryPlanner
from vinum.core.operators.numpy_operators import (
    NumpyOperator,
    BooleanFilterOperator,
    ComputeSelectExpressionsOperator,
    LikeOperator
)
from vinum.core.operators.numpy_function_operators import CountOperator
from vinum.core.operators.numpy_operator_mappings import SQL_OPERATOR_FUNCTIONS
from vinum.core.operators.generic_operators import Operator
from vinum.parser.query import SQLOperator, Literal
from vinum.tests.conftest import _test_column, _test_literal, create_query_ast


class TestNumpyQueryPlanner:

    def _find_expression_by_type(self, expr, expression_type):
        if isinstance(expr, expression_type):
            return expr
        else:
            for arg in expr.arguments:
                if isinstance(arg, Operator):
                    res = self._find_expression_by_type(arg, expression_type)
                    if res:
                        return res

    @staticmethod
    def _test_select_expressions(select_expr_operator, num_args, table):
        assert select_expr_operator is not None
        assert isinstance(select_expr_operator,
                          ComputeSelectExpressionsOperator)

        assert select_expr_operator.arguments is not None
        assert len(select_expr_operator.arguments) == num_args

        assert select_expr_operator.table is not None
        assert select_expr_operator.table == table

    @staticmethod
    def _test_where_operator(where_operator, num_args, table):
        assert where_operator is not None
        assert isinstance(where_operator, BooleanFilterOperator)

        assert where_operator.arguments is not None
        assert len(where_operator.arguments) == num_args

        assert where_operator.table is not None
        assert where_operator.table == table

    @staticmethod
    def _test_numpy_operator(operator, function, num_args):
        assert operator is not None
        assert isinstance(operator, NumpyOperator)

        assert operator.function is not None
        assert operator.function == function

        assert operator.arguments is not None
        assert len(operator.arguments) == num_args

    def test_select_all(self, test_arrow_table, test_table_column_names):
        query = "select * from t"
        query = create_query_ast(query, test_arrow_table)
        assert query is not None

        query_dag = NumpyQueryPlanner(query, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      len(test_table_column_names),
                                      test_arrow_table)

        _test_column(select_expr_operator.arguments[0],
                     test_table_column_names[0])
        _test_column(
            select_expr_operator.arguments[len(test_table_column_names) - 1],
            test_table_column_names[-1])

    def test_select_expr(self, test_arrow_table):
        query = "select lat * lng as passenger_km from t where total >= 15"
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      1,
                                      test_arrow_table)

        expr = select_expr_operator.arguments[0]
        self._test_numpy_operator(expr, SQL_OPERATOR_FUNCTIONS[
            SQLOperator.MULTIPLICATION], 2)
        _test_column(expr.arguments[0], 'lat')
        _test_column(expr.arguments[1], 'lng')

        where_condition = self._find_expression_by_type(query_dag,
                                                        BooleanFilterOperator)
        self._test_where_operator(where_condition,
                                  1,
                                  test_arrow_table)

        where_operator = where_condition.arguments[0]
        self._test_numpy_operator(where_operator, SQL_OPERATOR_FUNCTIONS[
            SQLOperator.GREATER_THAN_OR_EQUAL], 2)
        _test_column(where_operator.arguments[0], 'total')
        _test_literal(where_operator.arguments[1], 15)

    @pytest.mark.parametrize("query, function", (
            ("select -tip from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.NEGATION]),
            ("select ~tip from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.BINARY_NOT]),
    ))
    def test_select_unary_operators(self, test_arrow_table, query, function):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      1,
                                      test_arrow_table)

        expr = select_expr_operator.arguments[0]
        assert expr.function == function
        _test_column(expr.arguments[0], 'tip')

    @pytest.mark.parametrize("query, function", (
            ("select 1+2 from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.ADDITION]),
            ("select 1-2 from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.SUBTRACTION]),
            ("select 1/2 from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.DIVISION]),
            ("select 1*2 from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.MULTIPLICATION]),
            ("select 1%2 from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.MODULUS]),
    ))
    def test_select_math_operators(self, test_arrow_table, query, function):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      1,
                                      test_arrow_table)

        expr = select_expr_operator.arguments[0]
        self._test_numpy_operator(expr, function, 2)
        _test_literal(expr.arguments[0], 1)
        _test_literal(expr.arguments[1], 2)

    @pytest.mark.parametrize("query, function, operator_class", (
            ("select tip and tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.AND], NumpyOperator),
            ("select tip or tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.OR], NumpyOperator),
            ("select tip == tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.EQUALS], NumpyOperator),
            ("select tip != tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.NOT_EQUALS], NumpyOperator),
            ("select tip > tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.GREATER_THAN], NumpyOperator),
            ("select tip >= tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.GREATER_THAN_OR_EQUAL],
             NumpyOperator),
            ("select tip < tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.LESS_THAN], NumpyOperator),
            ("select tip <= tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.LESS_THAN_OR_EQUAL],
             NumpyOperator),
            ("select tip || tax from t",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.CONCAT], NumpyOperator),
    ))
    def test_select_logical_operators(self, test_arrow_table, query, function,
                                      operator_class):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      1,
                                      test_arrow_table)

        expr = select_expr_operator.arguments[0]
        assert isinstance(expr, operator_class)
        assert expr.function == function
        _test_column(expr.arguments[0], 'tip')
        _test_column(expr.arguments[1], 'tax')

    @pytest.mark.parametrize("query, function", (
            ("select * from t where tip in (1,2)",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.IN]),
            ("select * from t where tip not in (1,2)",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.NOT_IN]),
    ))
    def test_in_operator(self, test_arrow_table, query, function):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        where_condition = self._find_expression_by_type(query_dag,
                                                        BooleanFilterOperator)
        self._test_where_operator(where_condition, 1, test_arrow_table)

        where_operator = where_condition.arguments[0]
        self._test_numpy_operator(where_operator, function, 2)
        _test_column(where_operator.arguments[0], 'tip')

        in_list = where_operator.arguments[1]
        _test_literal(in_list[0], 1)
        _test_literal(in_list[1], 2)

    @pytest.mark.parametrize("query, function, from_arg, to_arg", (
            ("select * from t where tip between 2 and 10",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.BETWEEN], 2, 10),
            ("select * from t where tip not between 2 and 10",
             SQL_OPERATOR_FUNCTIONS[SQLOperator.NOT_BETWEEN], 2, 10),
    ))
    def test_between_operator(self,
                              test_arrow_table,
                              query,
                              function,
                              from_arg,
                              to_arg):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        where_condition = self._find_expression_by_type(query_dag,
                                                        BooleanFilterOperator)
        self._test_where_operator(where_condition, 1, test_arrow_table)

        where_operator = where_condition.arguments[0]
        self._test_numpy_operator(where_operator, function, 3)
        _test_column(where_operator.arguments[0], 'tip')
        _test_literal(where_operator.arguments[1], from_arg)
        _test_literal(where_operator.arguments[2], to_arg)

    @pytest.mark.parametrize("query, operator_class, pattern, invert", (
            ("SELECT * FROM t WHERE name LIKE '%abc_'", LikeOperator, '%abc_',
             False),
            ("SELECT * FROM t WHERE name NOT LIKE '%abc_'", LikeOperator,
             '%abc_', True),
    ))
    def test_like_operator(self, test_arrow_table, query, operator_class,
                           pattern, invert):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        where_condition = self._find_expression_by_type(query_dag,
                                                        BooleanFilterOperator)
        self._test_where_operator(where_condition, 1, test_arrow_table)

        like_operator = where_condition.arguments[0]
        assert like_operator is not None
        assert isinstance(like_operator, operator_class)
        assert like_operator.arguments is not None
        assert len(like_operator.arguments) == 2

        _test_column(like_operator.arguments[0], 'name')
        _test_literal(like_operator.arguments[1], pattern)

        assert like_operator.invert == invert

    @pytest.mark.parametrize("query, num_args, argument_class", (
            ("SELECT COUNT(*) FROM t", 1, Literal),
            ("SELECT COUNT(tax > 1.2) FROM t", 1, Operator),
            ("SELECT COUNT(tax > 1.2 and tip > 10) FROM t", 1, Operator),
    ))
    def test_count_operator(self, test_arrow_table, query, num_args,
                            argument_class):
        query_ast = create_query_ast(query, test_arrow_table)
        assert query_ast is not None

        query_dag = NumpyQueryPlanner(query_ast, test_arrow_table).plan_query()
        assert query_dag is not None

        select_expr_operator = self._find_expression_by_type(
            query_dag,
            ComputeSelectExpressionsOperator
        )
        self._test_select_expressions(select_expr_operator,
                                      1,
                                      test_arrow_table)

        count_operator = select_expr_operator.arguments[0]
        assert count_operator is not None
        assert isinstance(count_operator, CountOperator)
        assert count_operator.arguments is not None
        assert len(count_operator.arguments) == num_args

        if num_args:
            expression_operator = count_operator.arguments[0]
            assert expression_operator is not None
            assert isinstance(expression_operator, argument_class)
