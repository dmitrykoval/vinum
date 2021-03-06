from collections import defaultdict
from typing import List, Union, Optional, Tuple, Dict, cast, Set, Any

import pyarrow as pa
import numpy as np

import moz_sql_parser
import pyparsing

from vinum._typing import ParserArgType, QueryBaseType
from vinum.arrow.arrow_table import ArrowTable
from vinum.core.functions import is_aggregate_func
from vinum.errors import ParserError
from vinum.parser.query import (
    Column,
    Literal,
    Query,
    SQLOperator,
    Expression,
    SortOrder,
)
from vinum.util.util import (
    append_flat,
    ensure_is_array,
    is_array_type,
    is_column,
    is_expression,
    is_literal,
)


def flatten_expressions_tree(
        expression: Optional[Expression]) -> Tuple[Expression, ...]:
    """
    Flatten expressions tree into a list.
    """
    if not expression:
        return tuple()

    expressions = [expression]
    for arg in expression.arguments:
        if is_expression(arg):
            expressions.extend(flatten_expressions_tree(arg))

    return tuple(expressions)


class AbstractSqlParser:
    """
    Abstract SQL Parser.

    Generate AST from SQL statement. In the process column names are resolved
    and exception is raised in case columns can not be found.

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

    def generate_query_tree(self) -> Query:
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


class MozSqlParser(AbstractSqlParser):
    """
    Implementation of AbstractSqlParser based on moz_sql_parser.

    Moz SQL Parser parses SQL SELECT statements into a json object.
    The json object is further processed to create parser-independent
    Query AST object.

    For more details see: https://github.com/mozilla/moz-sql-parser

    Parameters
    ----------
    sql : str
        SQL statement to parse.
    table : ArrowTable
        Data table.
    """
    OPERATORS = {
        'add': SQLOperator.ADDITION,
        'sub': SQLOperator.SUBTRACTION,
        'mul': SQLOperator.MULTIPLICATION,
        'div': SQLOperator.DIVISION,
        'mod': SQLOperator.MODULUS,

        'neg': SQLOperator.NEGATION,
        'binary_not': SQLOperator.BINARY_NOT,

        'binary_or': SQLOperator.BINARY_OR,
        'binary_and': SQLOperator.BINARY_AND,

        'eq': SQLOperator.EQUALS,
        'neq': SQLOperator.NOT_EQUALS,
        'gt': SQLOperator.GREATER_THAN,
        'gte': SQLOperator.GREATER_THAN_OR_EQUAL,
        'lt': SQLOperator.LESS_THAN,
        'lte': SQLOperator.LESS_THAN_OR_EQUAL,

        'and': SQLOperator.AND,
        'or': SQLOperator.OR,
        'between': SQLOperator.BETWEEN,
        'not_between': SQLOperator.NOT_BETWEEN,
        'exists': SQLOperator.IS_NOT_NULL,
        'missing': SQLOperator.IS_NULL,
        'in': SQLOperator.IN,
        'nin': SQLOperator.NOT_IN,
        'like': SQLOperator.LIKE,
        'nlike': SQLOperator.NOT_LIKE,
        'not': SQLOperator.NOT,
        'distinct': SQLOperator.DISTINCT,

        'concat': SQLOperator.CONCAT,
    }

    SQL_TOKENS = {
        '*': '*',
        'np.pi': np.pi,
        'np.e': np.e,
    }

    def __init__(self, sql: str, schema: pa.Schema) -> None:
        super().__init__(sql, schema)
        self._sql_parsed: Dict[str, Any] = {}

    def _parse_sql(self) -> None:
        """
        Parses SQL statement string to json based structure.
        """
        self._sql_parsed = moz_sql_parser.parse(self._sql)

    def _resolve_column(self,
                        column_name: str,
                        alias: Optional[str] = None) -> Optional[Column]:
        """
        Return Column object if column exist in a table, None otherwise.
        """
        if column_name in self._schema.names:
            return Column(column_name, alias)
        else:
            return None

    def _resolve_expressions(
            self,
            expression: ParserArgType,
            alias: Optional[str] = None,
            aliases_map: Dict[str, QueryBaseType] = None
    ) -> Union[QueryBaseType, List]:
        """
        Recursively parse Expressions tree.

        Resolve columns and package literals into Literal object.
        """
        arguments: List[Union[QueryBaseType, List]] = []
        if isinstance(expression, dict):
            assert len(expression.keys()) == 1
            operator = list(expression.keys())[0]
            args = expression[operator]
            if operator == 'literal':
                if is_array_type(args):
                    return [Literal(arg) for arg in args]
                else:
                    return Literal(args, alias)
            elif operator in ('in', 'nin'):
                for arg in args:
                    if is_array_type(arg):
                        arguments.append(Literal(arg, alias))
                    else:
                        arguments.append(
                            self._resolve_expressions(
                                arg,
                                aliases_map=aliases_map
                            )
                        )
            else:
                res_args = self._resolve_expressions(
                    args,
                    aliases_map=aliases_map
                )
                append_flat(arguments, res_args)
        elif isinstance(expression, list):
            for arg in expression:
                arguments.append(
                    self._resolve_expressions(
                        arg,
                        aliases_map=aliases_map
                    )
                )
            return arguments
        else:
            column = self._resolve_column(str(expression), alias)
            if column:
                return column
            else:
                if isinstance(expression, str):
                    if aliases_map and expression in aliases_map:
                        return aliases_map[expression]
                    elif expression not in self.SQL_TOKENS:
                        raise ParserError(
                            f'Column: {expression} is not found.'
                        )
                    else:
                        expression = self.SQL_TOKENS[expression]
                return Literal(expression, alias)

        if operator in MozSqlParser.OPERATORS:
            sql_operator = MozSqlParser.OPERATORS[operator]
            function_name = None
        else:
            sql_operator = SQLOperator.FUNCTION
            function_name = operator

        return Expression(
            sql_operator,
            tuple(arguments),
            function_name=function_name,
            alias=alias
        )

    def _process_select_clause(self,
                               select_clause: ParserArgType
                               ) -> Tuple[QueryBaseType, ...]:
        """
        Recursively process all the expressions in the SELECT clause,
        and resolve them into Literal, Column or Expression.
        """
        if isinstance(select_clause, list):
            source_columns = select_clause
        elif isinstance(select_clause, dict):
            source_columns = [select_clause]
        elif isinstance(select_clause, str) and select_clause == '*':
            return tuple(
                (Column(col_name) for col_name in self._schema.names)
            )
        else:
            raise ParserError(
                f'Unrecognized select clause type: {select_clause}'
            )

        resolved_expressions: List[QueryBaseType] = []
        for select_expression in source_columns:
            if isinstance(select_expression, dict):
                select_value = select_expression['value']
                alias_name = select_expression.get('name')

                resolved_expressions.append(
                    cast(
                        QueryBaseType,
                        self._resolve_expressions(select_value, alias_name)
                    )
                )
            else:
                resolved_expressions.extend(
                    self._process_select_clause(select_expression)
                )

        return tuple(resolved_expressions)

    @staticmethod
    def _process_distinct(select_expressions: Tuple[QueryBaseType, ...]
                          ) -> Tuple[Tuple[QueryBaseType, ...], bool]:
        """
        Process DISTINCT clause if present.
        """
        select_exprs: List[QueryBaseType] = []
        distinct_on = None
        for expr in select_expressions:  # type: Any
            if (
                    is_expression(expr)
                    and expr.sql_operator == SQLOperator.DISTINCT
            ):
                select_exprs.extend(expr.arguments)
                distinct_on = set(expr.arguments)
            else:
                select_exprs.append(expr)

        if distinct_on and set(select_exprs) != distinct_on:
            raise ParserError(
                'DISTINCT column(s) are mixed with non-DISTINCT column(s), '
                'this behaviour is not supported.'
            )

        return (
            tuple(select_exprs),
            distinct_on is not None
        )

    @staticmethod
    def _create_aliases_map(
            select_expressions: Tuple[QueryBaseType, ...]
    ) -> Dict[str, QueryBaseType]:
        """
        Create a map of aliases and corresponding expressions.
        """
        aliases: Dict[str, QueryBaseType] = {}
        for expr in select_expressions:
            if expr.has_alias():
                alias = expr.get_alias()
                assert alias is not None
                aliases[alias] = expr
        return aliases

    @staticmethod
    def _mark_shared_expressions(
            operators_groups: Tuple[Tuple[QueryBaseType, ...], ...]
    ) -> None:
        """
        Assign shared expressions IDs.

        Process groups of operators
        (ie. select expressions, group by expressions, order by, ..),
        find equal expressions (ie timestamp % 60) in different groups
        and assign a random identifier to sets of shared expressions.

        Parameters
        ----------
        operators_groups : Tuple[Tuple[QueryBaseType, ...], ...]
            Groups of operators across which to mark shared expressions.
        """
        groups = [grp for grp in operators_groups if grp]
        if len(groups) < 2:
            return

        duplicate_indices: Dict[Expression, Set] = defaultdict(set)
        for group_one_idx, group_one in enumerate(groups):
            for group_two_idx, group_two in enumerate(groups):
                if group_one_idx == group_two_idx:
                    continue

                for op_one_idx, op_one in enumerate(group_one):
                    if not is_expression(op_one):
                        continue
                    for op_two_idx, op_two in enumerate(group_two):
                        if is_expression(op_two) and op_one == op_two:
                            duplicate_indices[op_one].update(
                                ((group_one_idx, op_one_idx),
                                 (group_two_idx, op_two_idx))
                            )

        for expression, group_indices in duplicate_indices.items():
            prefix = (expression.function_name
                      if expression.function_name
                      else expression.sql_operator)
            shared_id = f'{prefix}_{id(expression)}'
            for group_idx, expression_idx in group_indices:
                shared_expr = groups[group_idx][expression_idx]
                shared_expr.set_shared_id(shared_id)

    def _process_group_by(self,
                          group_by_clause: Tuple[Dict, ...],
                          aliases_map: Dict[str, QueryBaseType]
                          ) -> Tuple[QueryBaseType, ...]:
        """
        Process GROUP BY clause.
        """
        group_by = []
        for value in group_by_clause:
            group_by.append(
                cast(
                    QueryBaseType,
                    self._resolve_expressions(value['value'],
                                              aliases_map=aliases_map)
                )
            )

        return tuple(group_by)

    def _process_order_by(self,
                          order_by_clause: Tuple[Dict, ...],
                          aliases_map: Dict[str, QueryBaseType]
                          ) -> Tuple[
                                    Tuple[QueryBaseType, ...],
                                    Tuple[SortOrder, ...]]:
        """
        Process ORDER BY clause.
        """
        order_by = []
        sort_order = []
        for value in order_by_clause:
            expr = self._resolve_expressions(value['value'],
                                             aliases_map=aliases_map)
            col_order = (SortOrder.ASC
                         if value.get('sort', 'asc') == 'asc'
                         else SortOrder.DESC)
            order_by.append(cast(QueryBaseType, expr))
            sort_order.append(col_order)

        return (
            tuple(order_by),
            tuple(sort_order)
        )

    @staticmethod
    def _is_aggregate_query(
            select_expressions: Tuple[QueryBaseType, ...],
    ) -> bool:
        """
        Test if query contains aggregate functions.
        """
        for expr in select_expressions:
            if is_expression(expr):
                for expr in flatten_expressions_tree(expr):
                    if is_aggregate_func(expr.function_name):
                        return True
        return False

    @staticmethod
    def _ensure_groupby_select_correctness(
            select_expressions: Tuple[QueryBaseType, ...],
            group_by: Tuple[QueryBaseType, ...]) -> None:
        """
        Ensure structural correctness of the GROUP BY clause.

        For example SELECT columns are either part of the GROUP BY clause
        or used in the aggregate functions.
        """
        usage_msg = ('Only aggregate functions and columns present in the '
                     '"GROUP BY" clause are allowed.')

        group_by_columns = set(c for c in group_by if is_column(c))
        group_by_expressions = set(
            e for e in group_by if is_expression(e)
        )

        for column in select_expressions:
            if is_column(column) and column not in group_by_columns:
                column = cast(Column, column)
                raise ParserError(
                    f'Column "{column.get_column_name()}" is not part of the '
                    f'"GROUP BY" clause. {usage_msg}.'
                )
            elif is_expression(column):
                column = cast(Expression, column)
                is_aggr_expr = False
                for expr in flatten_expressions_tree(column):
                    if is_aggregate_func(expr.function_name):
                        is_aggr_expr = True
                        break

                if (column not in group_by_expressions and not is_aggr_expr):
                    op_name = (column.function_name
                               if column.function_name
                               else column.sql_operator)
                    raise ParserError(
                        f'Operator "{op_name}" is neither aggregate function '
                        f'nor part of the "GROUP BY" clause. {usage_msg}.'
                    )
            elif is_literal(column):
                column = cast(Literal, column)
                raise ParserError(
                    f'Literal value "{column.value}" is not allowed '
                    f'in the "GROUP BY" mode. {usage_msg}.'
                )

    @staticmethod
    def _ensure_having_correctness(having: Expression) -> None:
        """
        Ensure aggregate functions in HAVING clause are also
        referenced in either SELECT or GROUP BY.
        """
        for expr in flatten_expressions_tree(having):
            if (is_aggregate_func(expr.function_name)
                    and not expr.is_shared()):
                raise ParserError(
                    f'Aggregate function "{expr.function_name}" is used '
                    'in HAVING, but is neither part of "SELECT" '
                    'nor "GROUP BY.'
                )

    def generate_query_tree(self) -> Query:
        try:
            self._parse_sql()
        except pyparsing.ParseException as e:
            raise ParserError('Failed to parse the query.') from e

        assert self._sql_parsed
        select_expressions = self._process_select_clause(
            self._sql_parsed['select']
        )
        select_expressions, distinct_on = self._process_distinct(
            select_expressions
        )

        where_operators: Optional[Expression] = None
        if 'where' in self._sql_parsed:
            where_operators = self._resolve_expressions(
                self._sql_parsed['where']
            )   # type: Optional[Expression]

        is_aggregate_query = False
        group_by: Tuple[QueryBaseType, ...] = tuple()
        aliases_map = self._create_aliases_map(select_expressions)
        if 'groupby' in self._sql_parsed:
            group_by = self._process_group_by(
                ensure_is_array(self._sql_parsed['groupby']),
                aliases_map
            )
            is_aggregate_query = True
        if not is_aggregate_query:
            is_aggregate_query = self._is_aggregate_query(select_expressions)
        if is_aggregate_query:
            self._ensure_groupby_select_correctness(select_expressions,
                                                    group_by)

        having: Optional[Expression] = None
        if 'having' in self._sql_parsed:
            having = cast(
                Optional[Expression],
                self._resolve_expressions(
                    self._sql_parsed['having'],
                    aliases_map=aliases_map)
            )

        order_by: Tuple[QueryBaseType, ...] = tuple()
        sort_order: Tuple[SortOrder, ...] = tuple()
        if 'orderby' in self._sql_parsed:
            order_by, sort_order = self._process_order_by(
                ensure_is_array(self._sql_parsed['orderby']),
                aliases_map
            )

        self._mark_shared_expressions(
            (
                select_expressions,
                group_by,
                flatten_expressions_tree(having),
                order_by
            )
        )

        if having:
            self._ensure_having_correctness(having)

        limit = None
        offset = 0
        if 'limit' in self._sql_parsed:
            try:
                limit = int(self._sql_parsed['limit'])
                if 'offset' in self._sql_parsed:
                    offset = int(self._sql_parsed['offset'])
            except ValueError as e:
                raise ParserError(
                    'Failed to parse LIMIT clause. Expected integer value.'
                ) from e

        query = Query(
            self._schema,
            select_expressions,
            is_aggregate_query,
            distinct_on,
            where_operators,
            group_by,
            having,
            order_by,
            sort_order,
            limit,
            offset
        )

        return query


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
    return MozSqlParser(sql, schema)
