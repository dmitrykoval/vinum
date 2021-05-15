from typing import Dict, Any

import pyarrow as pa

from pglast import Node, parse_sql
from pglast.enums import A_Expr_Kind, BoolExprType

from vinum.errors import ParserError
from vinum.parser.query import (
    Column,
    Literal,
    Query,
    SQLExpression,
    Expression,
    SortOrder,
)
from vinum.util.util import (
    append_flat,
    is_literal,
)


class AbstractSqlParser:
    """
    Abstract SQL Parser.

    Generate AST from SQL statement.

    Parameters
    ----------
    sql : str
        SQL statement to parse.
    schema : pa.Schema
        Schema.
    """
    def __init__(self, sql: str, schema: pa.Schema) -> None:
        self._sql = sql
        self._schema = schema

    def parse(self) -> Query:
        """
        Generate AST of SQL statement.

        Column names are resolved and exception is raised in case columns
        can not be found.

        Returns
        -------
        Query
            AST of a statement.
        """
        pass


class PglastParser(AbstractSqlParser):
    """
    Parses SQL query into AST.
    Uses Postgres parser provided by an amazing Pglast project.
    https://github.com/lelit/pglast
    """
    OPERATORS = {
        '+': SQLExpression.ADDITION,
        '-': SQLExpression.SUBTRACTION,
        '*': SQLExpression.MULTIPLICATION,
        '/': SQLExpression.DIVISION,
        '%': SQLExpression.MODULUS,

        '=': SQLExpression.EQUALS,
        '==': SQLExpression.EQUALS,
        '!=': SQLExpression.NOT_EQUALS,
        '<>': SQLExpression.NOT_EQUALS,
        '>': SQLExpression.GREATER_THAN,
        '>=': SQLExpression.GREATER_THAN_OR_EQUAL,
        '<': SQLExpression.LESS_THAN,
        '<=': SQLExpression.LESS_THAN_OR_EQUAL,

        '|': SQLExpression.BINARY_OR,
        '&': SQLExpression.BINARY_AND,
        '#': SQLExpression.BINARY_XOR,
        '~': SQLExpression.BINARY_NOT,
        '||': SQLExpression.CONCAT,
    }

    BOOL_OPERATORS = {
        BoolExprType.AND_EXPR: SQLExpression.AND,
        BoolExprType.OR_EXPR: SQLExpression.OR,
        BoolExprType.NOT_EXPR: SQLExpression.NOT,
    }

    def __init__(self, sql: str, schema: pa.Schema) -> None:
        super().__init__(sql, schema)
        self._sql_parsed: Dict[str, Any] = {}

    def _parse(self):
        root_node = Node(parse_sql(self._sql))

        assert len(root_node) == 1
        statement = root_node[0].parse_tree['stmt']
        if 'SelectStmt' not in statement:
            raise ParserError('Only SELECT statements are supported.')

        return statement['SelectStmt']

    def _unpack_literal(self, val: dict):
        if 'String' in val:
            return val['String']['str']
        elif 'Integer' in val:
            return val['Integer']['ival']
        elif 'Float' in val:
            return float(val['Float']['str'])
        elif 'Null' in val:
            return None
        else:
            raise ParserError(
                f'Failed to unpack literal: {val}'
            )

    def _parse_node(self, node):
        try:
            if 'A_Const' in node:
                val = node['A_Const']['val']
                return Literal(self._unpack_literal(val))
            elif 'ColumnRef' in node:
                col_val = node['ColumnRef']['fields'][0]

                if isinstance(col_val, dict) and 'A_Star' in col_val:
                    return tuple(
                        (Column(col_name) for col_name in self._schema.names)
                    )

                return Column(self._unpack_literal(col_val))
            elif 'A_Expr' in node:
                expr = node['A_Expr']
                kind = A_Expr_Kind(expr['kind'])
                name = self._unpack_literal(expr['name'][0])

                if kind == A_Expr_Kind.AEXPR_OP:
                    sql_operator = self.OPERATORS[name]
                    args = []
                    for arg_type in ('lexpr', 'rexpr'):
                        if arg_type in expr:
                            arg = self._parse_node(expr[arg_type])
                            if is_literal(arg) and arg.value is None and name in ('=', '=='):
                                sql_operator = SQLExpression.IS_NULL
                            elif is_literal(arg) and arg.value is None and name in ('!=', '<>'):
                                sql_operator = SQLExpression.IS_NOT_NULL
                            else:
                                args.append(arg)
                    if name == '-' and len(args) == 1:
                        sql_operator = SQLExpression.NEGATION
                elif kind == A_Expr_Kind.AEXPR_IN:
                    if name == '=':
                        sql_operator = SQLExpression.IN
                    else:
                        sql_operator = SQLExpression.NOT_IN
                    args = [self._parse_node(expr['lexpr'])]
                    in_list = []
                    for arg in expr['rexpr']:
                        in_list.append(self._parse_node(arg).value)
                    args.append(Literal(in_list))
                elif kind in (A_Expr_Kind.AEXPR_BETWEEN, A_Expr_Kind.AEXPR_NOT_BETWEEN):
                    if kind == A_Expr_Kind.AEXPR_BETWEEN:
                        sql_operator = SQLExpression.BETWEEN
                    else:
                        sql_operator = SQLExpression.NOT_BETWEEN
                    args = [self._parse_node(expr['lexpr'])]
                    for arg in expr['rexpr']:
                        args.append(self._parse_node(arg))
                elif kind == A_Expr_Kind.AEXPR_LIKE:
                    if name == '~~':
                        sql_operator = SQLExpression.LIKE
                    else:
                        sql_operator = SQLExpression.NOT_LIKE
                    args = [
                        self._parse_node(expr['lexpr']),
                        self._parse_node(expr['rexpr'])
                    ]

                else:
                    raise ParserError(f'Expression type "{kind}" is not implemented')

                return Expression(
                    sql_operator,
                    tuple(args)
                )
            elif 'BoolExpr' in node:
                expr = node['BoolExpr']
                bool_operator = self.BOOL_OPERATORS[BoolExprType(expr['boolop'])]
                args = []
                for arg in expr['args']:
                    args.append(self._parse_node(arg))
                return Expression(
                    bool_operator,
                    tuple(args)
                )
            elif 'NullTest' in node:
                expr = node['NullTest']
                is_null_op = (SQLExpression.IS_NULL
                              if expr['nulltesttype'] == 0
                              else SQLExpression.IS_NOT_NULL
                              )
                return Expression(
                    is_null_op,
                    tuple([self._parse_node(expr['arg'])])
                )
            elif 'FuncCall' in node:
                expr = node['FuncCall']
                name = '.'.join(self._unpack_literal(fc) for fc in expr['funcname'])
                args = []
                if 'agg_star' in expr and name and name.lower() == 'count':
                    name = 'count_star'
                elif 'args' in expr:
                    for arg in expr['args']:
                        args.append(self._parse_node(arg))
                return Expression(
                    SQLExpression.FUNCTION,
                    tuple(args),
                    function_name=name
                )

        except (KeyError, IndexError):
            raise ParserError('Failed to parse the query.')

    def _create_ast(self):
        ast = self._parse()

        is_aggregate_query = False
        distinct_on = False
        if 'distinctClause' in ast:
            distinct_on = True
            is_aggregate_query = True

        select_expressions = []
        for sel_expr in ast['targetList']:
            target = sel_expr['ResTarget']
            alias = target['name'] if 'name' in target else None
            node = self._parse_node(target['val'])
            if alias:
                node.set_alias(alias)
            append_flat(select_expressions, node)
        select_expressions = tuple(select_expressions)

        where_clause = None
        if 'whereClause' in ast:
            where_clause = self._parse_node(ast['whereClause'])

        group_by = []
        if 'groupClause' in ast:
            is_aggregate_query = True
            group_by = []
            for expr in ast['groupClause']:
                group_by.append(self._parse_node(expr))
            group_by = tuple(group_by)

        having = None
        if 'havingClause' in ast:
            having = self._parse_node(ast['havingClause'])

        order_by = []
        sort_order = []
        if 'sortClause' in ast:
            for sort_expr in ast['sortClause']:
                sort_by = sort_expr['SortBy']
                order_by.append(self._parse_node(sort_by['node']))
                sortby_dir = SortOrder.ASC if sort_by['sortby_dir'] <= 1 else SortOrder.DESC
                sort_order.append(sortby_dir)
        order_by = tuple(order_by)
        sort_order = tuple(sort_order)

        limit = None
        offset = 0
        if 'limitCount' in ast:
            limit = self._parse_node(ast['limitCount']).value
            if 'limitOffset' in ast:
                offset = self._parse_node(ast['limitOffset']).value

        return Query(
            self._schema,
            select_expressions,
            is_aggregate_query,
            distinct_on,
            where_clause,
            group_by,
            having,
            order_by,
            sort_order,
            limit,
            offset
        )

    def parse(self) -> Query:
        return self._create_ast()


def parser_factory(sql: str, schema: pa.Schema) -> AbstractSqlParser:
    """
    Get SQL Parser instance.

    Parameters
    ----------
    sql : str
        SQL statement to parse.
    schema : pa.Schema
        Schema.

    Returns
    -------
    AbstractSqlParser
        Instance of AbstractSqlParser.
    """
    return PglastParser(sql, schema)
