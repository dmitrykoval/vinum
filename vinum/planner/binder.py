from typing import Tuple, Dict, cast, Any, Iterable, Optional, Union
from vinum._typing import QueryBaseType

import pyarrow as pa

from vinum.errors import ParserError
from vinum.core.functions import is_aggregate_func
from vinum.parser.query import (
    Column,
    Literal,
    Query,
    Expression,
)
from vinum.util.util import (
    is_literal,
    is_expression,
    is_column,
    is_array_type,
    traverse_exprs,
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


class Binder:
    @classmethod
    def bind(cls, query: Query) -> Query:
        aliases_map = cls._build_aliases_map(query.select_expressions)

        where_clause = cls._substitute_aliases(query.where_condition, aliases_map)
        group_by = cls._substitute_aliases(query.group_by, aliases_map)
        having = cls._substitute_aliases(query.having, aliases_map)
        order_by = cls._substitute_aliases(query.order_by, aliases_map)

        cls._validate_columns(
            [
                query.select_expressions,
                where_clause,
                group_by,
                having,
                order_by
            ],
            query.schema,
        )

        is_aggregate_query = query.is_aggregate()
        if not is_aggregate_query:
            is_aggregate_query = cls._is_aggregate_query(query.select_expressions)

        if query.is_aggregate():
            cls._ensure_groupby_select_correctness(query.select_expressions,
                                                    group_by)

        cls._mark_shared_expressions(
            (
                query.select_expressions,
                group_by,
                having,
                order_by
            )
        )

        return Query(
            query.schema,
            query.select_expressions,
            is_aggregate_query,
            query.distinct,
            where_clause,
            group_by,
            having,
            order_by,
            query.sort_order,
            query.limit,
            query.offset
        )


    @classmethod
    def _build_aliases_map(cls, expressions) -> Dict:
        aliases_map = {}
        for expr in expressions:
            if expr.has_alias():
                aliases_map[expr.get_alias()] = expr
        return aliases_map

    @classmethod
    def _substitute_aliases(
            cls, clause: Any,
            aliases_map: dict) -> Union[QueryBaseType,
                                        Tuple[QueryBaseType, ...]]:
        if clause is None:
            return None
        elif is_array_type(clause):
            return tuple(
                cls._substitute_alias(expr, aliases_map)
                for expr in clause
            )
        else:
            return cls._substitute_alias(clause, aliases_map)

    @classmethod
    def _substitute_alias(cls, expr: QueryBaseType,
                          aliases_map: dict) -> QueryBaseType:
        if is_column(expr) and expr.get_column_name() in aliases_map:
            alias_expr = aliases_map[expr.get_column_name()]
            if is_expression(alias_expr):
                return alias_expr.copy()
            else:
                return alias_expr
        elif is_expression(expr):
            args = []
            for arg in expr.arguments:
                args.append(
                    cls._substitute_alias(arg, aliases_map)
                )
            expr.set_arguments(tuple(args))

        return expr

    @classmethod
    def _validate_columns(cls, clauses: Iterable,
                          schema: pa.Schema) -> None:
        for clause in clauses:
            if not clause:
                continue
            elif is_array_type(clause):
                for expr in clause:
                    cls._ensure_col_exists(expr, schema)
            else:
                cls._ensure_col_exists(clause, schema)

    @classmethod
    def _ensure_col_exists(cls, expr: QueryBaseType,
                           schema: pa.Schema) -> None:
        """
        Recursively process expressions tree and raise
        if column was not found.
        """
        if is_column(expr):
            column_name = expr.get_column_name()
            if column_name not in schema.names:
                raise ParserError(
                    f"Column '{column_name}' is not found."
                )
        elif is_expression(expr):
            for arg in expr.arguments:
                cls._ensure_col_exists(arg, schema)

    @classmethod
    def _mark_shared_expressions(
            cls,
            expr_groups: Tuple[Tuple[QueryBaseType, ...], ...]
    ) -> None:
        """
        Assign shared expressions IDs.

        Find equal expressions (ie timestamp % 60) in the input subtrees
        and assign a random identifier to sets of shared expressions.

        Parameters
        ----------
        expr_groups : Tuple[Tuple[QueryBaseType, ...], ...]
            Groups of expressions across which to mark shared expressions.
        """
        flat_exprs = []
        for group in expr_groups:
            flat_exprs.extend(traverse_exprs(group))

        for expr_one in flat_exprs:
            for expr_two in flat_exprs:
                if expr_one is expr_two:
                    continue

                if expr_one == expr_two:
                    if expr_one.is_shared():
                        shared_id = expr_one.get_shared_id()
                    elif expr_two.is_shared():
                        shared_id = expr_two.get_shared_id()
                    else:
                        prefix = (expr_one.function_name
                                  if expr_one.function_name
                                  else expr_one.sql_operator)
                        shared_id = f'{prefix}_{id(expr_one)}'
                    expr_one.set_shared_id(shared_id)
                    expr_two.set_shared_id(shared_id)

    @classmethod
    def _is_aggregate_query(
            cls,
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

    @classmethod
    def _ensure_groupby_select_correctness(
            cls,
            select_expressions: Tuple[QueryBaseType, ...],
            group_by: Tuple[QueryBaseType, ...]) -> None:
        """
        Ensure structural correctness of the GROUP BY clause.

        For example SELECT columns are either part of the GROUP BY clause
        or used in the aggregate functions.
        """
        if not group_by:
            return
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
