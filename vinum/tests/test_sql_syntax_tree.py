import pytest

from vinum.parser.query import (
    Expression,
    SQLOperator,
    SortOrder,
    Literal,
    Column
)
from vinum.tests.conftest import _test_column, _test_literal, create_query_ast


class TestSyntaxTree:

    def _test_expression(self,
                         expression,
                         sql_operator,
                         num_args,
                         function_name=None,
                         alias=None):
        assert expression is not None
        assert isinstance(expression, Expression)

        assert expression.sql_operator == sql_operator

        if function_name:
            assert expression.function_name is not None
            assert expression.function_name == function_name
        else:
            assert expression.function_name is None

        if alias:
            assert expression._alias is not None
            assert expression._alias == alias

        if num_args:
            assert expression.arguments is not None
            assert len(expression.arguments) == num_args
        else:
            assert expression.arguments is None

    def _test_select_exprs(self, query, sel_exprs_len):
        assert query is not None

        assert query.select_expressions is not None
        assert isinstance(query.select_expressions, tuple)
        assert len(query.select_expressions) == sel_exprs_len

    def test_select(self, test_arrow_table, test_table_column_names):
        query = "select * from table"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, len(test_table_column_names))

        assert query_ast.where_condition is None

    def test_select_column(self, test_arrow_table):
        query = "select tax as total from table"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 1)
        _test_column(query_ast.select_expressions[0], 'tax', 'total')

        assert query_ast.where_condition is None

    def test_select_columns(self, test_arrow_table):
        query = "select tax as total, lat as dropoff from table"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 2)
        _test_column(query_ast.select_expressions[0], 'tax', 'total')
        _test_column(query_ast.select_expressions[1], 'lat', 'dropoff')

        assert query_ast.where_condition is None

    def test_select_expression(self, test_arrow_table):
        query = "select tip as total, tax + tip as with_tip from t"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 2)
        _test_column(query_ast.select_expressions[0], 'tip', 'total')

        expr = query_ast.select_expressions[1]
        self._test_expression(expr, SQLOperator.ADDITION, 2, alias='with_tip')

        _test_column(expr.arguments[0], 'tax')
        _test_column(expr.arguments[1], 'tip')

        assert query_ast.where_condition is None

    def test_select_expression_functions(self, test_arrow_table):
        query = ("select np.power(10, np.min(total)) as exp, count(*) as cnt "
                 "from t")
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 2)
        expr = query_ast.select_expressions[0]
        self._test_expression(expr,
                              SQLOperator.FUNCTION,
                              2,
                              function_name='np.power',
                              alias='exp')

        args = expr.arguments

        _test_literal(args[0], 10)

        self._test_expression(args[1],
                              SQLOperator.FUNCTION,
                              1,
                              function_name='np.min')
        _test_column(args[1].arguments[0], 'total')

        expr = query_ast.select_expressions[1]
        self._test_expression(expr,
                              SQLOperator.FUNCTION,
                              1,
                              function_name='count',
                              alias='cnt')
        _test_literal(expr.arguments[0], '*')

        assert query_ast.where_condition is None

    def test_where(self, test_arrow_table, test_table_column_names):
        query = "select * from table where vendor_id > 1 and lat < 4.5"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, len(test_table_column_names))
        self._test_expression(query_ast.where_condition, SQLOperator.AND, 2)

        left_expr = query_ast.where_condition.arguments[0]
        self._test_expression(left_expr, SQLOperator.GREATER_THAN, 2)
        _test_column(left_expr.arguments[0], 'vendor_id')
        _test_literal(left_expr.arguments[1], 1)

        right_expr = query_ast.where_condition.arguments[1]
        self._test_expression(right_expr, SQLOperator.LESS_THAN, 2)
        _test_column(right_expr.arguments[0], 'lat')
        _test_literal(right_expr.arguments[1], 4.5)

    def test_where_parenthesis(self,
                               test_arrow_table,
                               test_table_column_names):
        sql = ("select * from table "
               "where (lat > 2.0 and 15 <= total) or tip = 12")
        query_ast = create_query_ast(sql, test_arrow_table)

        self._test_select_exprs(query_ast, len(test_table_column_names))
        self._test_expression(query_ast.where_condition, SQLOperator.OR, 2)

        left_arg = query_ast.where_condition.arguments[0]
        self._test_expression(left_arg, SQLOperator.AND, 2)

        nested_left = left_arg.arguments[0]
        self._test_expression(nested_left, SQLOperator.GREATER_THAN, 2)
        _test_column(nested_left.arguments[0], 'lat')
        _test_literal(nested_left.arguments[1], 2.0)

        nested_right = left_arg.arguments[1]
        self._test_expression(nested_right, SQLOperator.LESS_THAN_OR_EQUAL, 2)
        _test_literal(nested_right.arguments[0], 15)
        _test_column(nested_right.arguments[1], 'total')

        right_arg = query_ast.where_condition.arguments[1]
        self._test_expression(right_arg, SQLOperator.EQUALS, 2)
        _test_column(right_arg.arguments[0], 'tip')
        _test_literal(right_arg.arguments[1], 12)

    def test_literal_alias(self, test_arrow_table):
        query = "select 2 as alias from table"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 1)
        _test_literal(query_ast.select_expressions[0], 2, 'alias')

        assert query_ast.where_condition is None

    def test_expression_alias(self, test_arrow_table):
        query = "select int(np.sqrt(total)) as sqrt from t"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 1)

        self._test_expression(query_ast.select_expressions[0],
                              SQLOperator.FUNCTION,
                              1,
                              function_name='int',
                              alias='sqrt')
        nested_expr = query_ast.select_expressions[0].arguments[0]
        self._test_expression(nested_expr,
                              SQLOperator.FUNCTION,
                              1,
                              function_name='np.sqrt')
        _test_column(nested_expr.arguments[0], 'total')

        assert query_ast.where_condition is None

    @pytest.mark.parametrize("query, sql_operator", (
            ("select -tip from t", SQLOperator.NEGATION),
            ("select ~tip from t", SQLOperator.BINARY_NOT),
            ("select not tip from t", SQLOperator.NOT),
            ("select tip is null from t", SQLOperator.IS_NULL),
            ("select tip = null from t", SQLOperator.IS_NULL),
            ("select tip == null from t", SQLOperator.IS_NULL),
            ("select tip is not null from t", SQLOperator.IS_NOT_NULL),
            ("select tip != null from t", SQLOperator.IS_NOT_NULL),
            ("select tip <> null from t", SQLOperator.IS_NOT_NULL),
    ))
    def test_unary_sql_operators(self, test_arrow_table, query, sql_operator):
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 1)

        expr = query_ast.select_expressions[0]
        self._test_expression(expr, sql_operator, 1)
        _test_column(expr.arguments[0], 'tip')

        assert query_ast.where_condition is None

    @pytest.mark.parametrize("query, sql_operator", (
            ("select 1+2 from t", SQLOperator.ADDITION),
            ("select 1-2 from t", SQLOperator.SUBTRACTION),
            ("select 1/2 from t", SQLOperator.DIVISION),
            ("select 1*2 from t", SQLOperator.MULTIPLICATION),
            ("select 1%2 from t", SQLOperator.MODULUS),
            ("select 1&2 from t", SQLOperator.BINARY_AND),
            ("select 1|2 from t", SQLOperator.BINARY_OR),
            ("select 1=2 from t", SQLOperator.EQUALS),
            ("select 1==2 from t", SQLOperator.EQUALS),
            ("select 1 is 2 from t", SQLOperator.EQUALS),
            ("select 1!=2 from t", SQLOperator.NOT_EQUALS),
            ("select 1<>2 from t", SQLOperator.NOT_EQUALS),
            ("select 1>2 from t", SQLOperator.GREATER_THAN),
            ("select 1>=2 from t", SQLOperator.GREATER_THAN_OR_EQUAL),
            ("select 1<2 from t", SQLOperator.LESS_THAN),
            ("select 1<=2 from t", SQLOperator.LESS_THAN_OR_EQUAL),
            ("select 1 and 2 from t", SQLOperator.AND),
            ("select 1 or 2 from t", SQLOperator.OR),
            ("select 1 || 2 from t", SQLOperator.CONCAT),
    ))
    def test_binary_sql_operators(self, test_arrow_table, query, sql_operator):
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 1)

        expr = query_ast.select_expressions[0]
        self._test_expression(expr, sql_operator, 2)
        _test_literal(expr.arguments[0], 1)
        _test_literal(expr.arguments[1], 2)

        assert query_ast.where_condition is None

    @pytest.mark.parametrize("query, sql_operator, args", (
            ("select * from table where tip in (1,2,3)",
             SQLOperator.IN, (1, 2, 3)),
            ("select * from table where tip not in (1,2,3)",
             SQLOperator.NOT_IN, (1, 2, 3)),
    ))
    def test_where_in_operators(self,
                                test_arrow_table,
                                query,
                                sql_operator,
                                args):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast is not None

        expr = query_ast.where_condition
        self._test_expression(expr, sql_operator, 2)
        _test_column(expr.arguments[0], 'tip')
        for arg1, arg2 in zip(expr.arguments[1].value, args):
            assert arg1 == arg2

    @pytest.mark.parametrize("query, sql_operator, args", (
            ("select * from t where tip between 1 and 10",
             SQLOperator.BETWEEN, (1, 10)),
            ("select * from t where tip not between 1 and 10",
             SQLOperator.NOT_BETWEEN, (1, 10)),
    ))
    def test_where_between_operators(self,
                                     test_arrow_table,
                                     query,
                                     sql_operator,
                                     args):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast is not None

        expr = query_ast.where_condition
        self._test_expression(expr, sql_operator, 3)
        _test_column(expr.arguments[0], 'tip')
        assert expr.arguments[1].value == 1
        assert expr.arguments[2].value == 10

    @pytest.mark.parametrize("query, sql_operator, args", (
            ("select * from t where tip like '%STR%'",
             SQLOperator.LIKE, '%STR%'),
            ("select * from t where tip not like '%STR%'",
             SQLOperator.NOT_LIKE, '%STR%'),
    ))
    def test_where_like_operators(self,
                                  test_arrow_table,
                                  query,
                                  sql_operator,
                                  args):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast is not None

        expr = query_ast.where_condition
        self._test_expression(expr, sql_operator, 2)
        _test_column(expr.arguments[0], 'tip')
        assert expr.arguments[1].value == args

    def test_select_all_and_expressions(self,
                                        test_arrow_table,
                                        test_table_column_names):
        query = "select *, np.log(tip) * 17, total - tax from t"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, len(test_table_column_names) + 2)

        expr = query_ast.select_expressions[-2]
        self._test_expression(expr, SQLOperator.MULTIPLICATION, 2)
        args = expr.arguments
        self._test_expression(args[0],
                              SQLOperator.FUNCTION,
                              1,
                              function_name='np.log')
        _test_column(args[0].arguments[0], 'tip')
        _test_literal(args[1], 17)

        expr = query_ast.select_expressions[-1]
        self._test_expression(expr, SQLOperator.SUBTRACTION, 2)
        args = expr.arguments
        _test_column(args[0], 'total')
        _test_column(args[1], 'tax')

        assert query_ast.where_condition is None

    def test_select_distinct(self, test_arrow_table):
        query = "select distinct (tax, total, tip) from t"
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 3)

        _test_column(query_ast.select_expressions[0], 'tax')
        _test_column(query_ast.select_expressions[1], 'total')
        _test_column(query_ast.select_expressions[2], 'tip')

        assert query_ast.distinct is not None
        assert isinstance(query_ast.distinct, bool)
        assert query_ast.distinct

        assert query_ast.where_condition is None

    def test_groupby(self, test_arrow_table):
        query = ("select city_from, city_to, count(*) from t "
                 "group by city_from, city_to")
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 3)

        _test_column(query_ast.select_expressions[0], 'city_from')
        _test_column(query_ast.select_expressions[1], 'city_to')
        self._test_expression(query_ast.select_expressions[2],
                              SQLOperator.FUNCTION,
                              1,
                              function_name='count')

        assert query_ast.is_aggregate() is True
        assert query_ast.group_by is not None
        assert len(query_ast.group_by) == 2

        _test_column(query_ast.group_by[0], 'city_from')
        _test_column(query_ast.group_by[1], 'city_to')

        assert query_ast.where_condition is None

    @pytest.mark.parametrize("query, alias", (
            ("select city_from, timestamp % 2, count(*) from t "
             "group by city_from, timestamp % 2", None),
            ("select city_from, timestamp % 2 as even, count(*) from t "
             "group by city_from, even", 'even')
    ))
    def test_groupby_expr(self, test_arrow_table, query, alias):
        query_ast = create_query_ast(query, test_arrow_table)

        self._test_select_exprs(query_ast, 3)

        _test_column(query_ast.select_expressions[0], 'city_from')
        sel_expr = query_ast.select_expressions[1]
        self._test_expression(sel_expr, SQLOperator.MODULUS, 2, alias=alias)
        _test_column(sel_expr.arguments[0], 'timestamp')
        _test_literal(sel_expr.arguments[1], 2)
        assert sel_expr.is_shared() is True
        self._test_expression(query_ast.select_expressions[2],
                              SQLOperator.FUNCTION,
                              1,
                              function_name='count')

        assert query_ast.is_aggregate() is True
        assert query_ast.group_by is not None
        assert len(query_ast.group_by) == 2

        _test_column(query_ast.group_by[0], 'city_from')
        groupby_expr = query_ast.group_by[1]
        self._test_expression(groupby_expr,
                              SQLOperator.MODULUS,
                              2,
                              alias=alias)
        _test_column(groupby_expr.arguments[0], 'timestamp')
        _test_literal(groupby_expr.arguments[1], 2)
        assert groupby_expr.is_shared() is True

        assert sel_expr.get_shared_id() == groupby_expr.get_shared_id()

        assert query_ast.where_condition is None

    @pytest.mark.parametrize("query, order_columns", (
            ("select * from t order by total",
             (('total', SortOrder.ASC),)),
            ("select * from t order by total asc",
             (('total', SortOrder.ASC),)),
            ("select * from t order by total, city_from",
             (('total', SortOrder.ASC), ('city_from', SortOrder.ASC))
             ),
            ("select * from t order by total desc, city_from",
             (('total', SortOrder.DESC), ('city_from', SortOrder.ASC))
             ),
            ("select * from t order by total desc, city_from asc",
             (('total', SortOrder.DESC), ('city_from', SortOrder.ASC))
             ),
            ("select * from t order by total asc, city_from desc",
             (('total', SortOrder.ASC), ('city_from', SortOrder.DESC))
             ),
            ("select * from t order by total asc, city_from asc",
             (('total', SortOrder.ASC), ('city_from', SortOrder.ASC))
             ),
            ("select * from t "
             "order by total desc, city_from asc, tax desc, tip asc",
             (
                     ('total', SortOrder.DESC), ('city_from', SortOrder.ASC),
                     ('tax', SortOrder.DESC), ('tip', SortOrder.ASC)
             )
             ),
    ))
    def test_order_by(self, test_arrow_table, query, order_columns):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast.order_by is not None
        assert query_ast.sort_order is not None
        assert len(query_ast.order_by) == len(order_columns)
        assert len(query_ast.order_by) == len(query_ast.sort_order)

        order_by_list = zip(query_ast.order_by,
                            query_ast.sort_order,
                            order_columns)
        for col, sort_order, (expected_name, expected_order) in order_by_list:
            _test_column(col, expected_name)
            assert sort_order == expected_order

    @pytest.mark.parametrize("query, is_shared", (
            ("select * from t order by tax + tip + total desc",
             False),
            ("select tax + tip + total from t order by tax + tip + total desc",
             True),
            ("select tax + tip + total as sum from t order by sum desc",
             True),
    ))
    def test_order_by_expr(self, test_arrow_table, query, is_shared):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast.order_by is not None
        assert query_ast.sort_order is not None
        assert len(query_ast.order_by) == 1
        assert len(query_ast.sort_order) == 1

        assert query_ast.sort_order[0] == SortOrder.DESC

        self._test_expression(query_ast.order_by[0], SQLOperator.ADDITION, 3)
        assert query_ast.order_by[0].is_shared() == is_shared

        _test_column(query_ast.order_by[0].arguments[0], 'tax')
        _test_column(query_ast.order_by[0].arguments[1], 'tip')
        _test_column(query_ast.order_by[0].arguments[2], 'total')

    @pytest.mark.parametrize("query, limit, offset", (
            ("select * from t", None, 0),
            ("select * from t limit 1", 1, 0),
            ("select * from t limit 10 offset 5", 10, 5),
    ))
    def test_limit(self, test_arrow_table, query, limit, offset):
        query_ast = create_query_ast(query, test_arrow_table)

        assert query_ast.limit == limit
        assert query_ast.offset == offset

    @pytest.mark.parametrize("query, args", (
            (
                    "select datetime('2020') from t",
                    (Literal('2020'),)
            ),
            (
                    "select datetime(2020) from t",
                    (Literal(2020),)
            ),
            (
                    "select datetime('2020', 'Y') from t",
                    (Literal('2020'), Literal('Y'))
            ),
            (
                    "select datetime('2020', 'Y', 19) from t",
                    (Literal('2020'), Literal('Y'), Literal(19))
            ),
            (
                    "select datetime(tax, '2020', 19) from t",
                    (Column('tax'), Literal('2020'), Literal(19))
            ),
            (
                    "select datetime('2020', 19, tax) from t",
                    (Literal('2020'), Literal(19), Column('tax'))
            ),
            (
                    "select datetime('2020', tax, 19) from t",
                    (Literal('2020'), Column('tax'), Literal(19))
            ),
    ))
    def test_func_args(self, test_arrow_table, query, args):
        query_ast = create_query_ast(query, test_arrow_table)

        for actual, expected in zip(
                query_ast.select_expressions[0].arguments,
                args):
            assert actual == expected
