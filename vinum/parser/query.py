from enum import Enum, auto
from typing import Any, Optional, Tuple, TYPE_CHECKING, FrozenSet, List, cast

from vinum.util.util import (
    TREE_INDENT_SYMBOL,
    find_all_columns_recursively,
    append_flat,
    is_expression,
)
from vinum.util.tree_print import RecursiveTreePrint


if TYPE_CHECKING:
    from vinum._typing import QueryBaseType


class SQLExpression(Enum):
    # Arithmetic operators
    ADDITION = auto()
    SUBTRACTION = auto()
    MULTIPLICATION = auto()
    DIVISION = auto()
    MODULUS = auto()

    NEGATION = auto()
    BINARY_NOT = auto()
    BINARY_AND = auto()
    BINARY_OR = auto()
    BINARY_XOR = auto()

    CONCAT = auto()

    # Comparison operators
    EQUALS = auto()
    NOT_EQUALS = auto()
    GREATER_THAN = auto()
    GREATER_THAN_OR_EQUAL = auto()
    LESS_THAN = auto()
    LESS_THAN_OR_EQUAL = auto()

    # Logical operators
    AND = auto()
    BETWEEN = auto()
    NOT_BETWEEN = auto()
    EXISTS = auto()
    NOT_EXISTS = auto()
    IN = auto()
    NOT_IN = auto()
    LIKE = auto()
    NOT_LIKE = auto()
    NOT = auto()
    OR = auto()
    IS_NULL = auto()
    IS_NOT_NULL = auto()
    DISTINCT = auto()

    # Functions
    FUNCTION = auto()


class SortOrder(Enum):
    """
    Sort order (ASC, DESC).
    """
    ASC = auto()
    DESC = auto()


class HasAlias:
    """
    Abstract class indicating that class has an alias.
    """

    def has_alias(self) -> bool:
        """
        Return True if object has an an alias.

        Returns
        -------
        bool
            True if object has an an alias.
        """
        pass

    def get_alias(self) -> Optional[str]:
        """
        Return alias.

        Returns
        -------
        str
            Alias.
        """
        pass

    def set_alias(self, alias: str) -> None:
        """
        Set alias.

        Parameters
        ----------
        alias : str
            Alias.
        """
        pass


class HasColumnName:
    """
    Abstract class indicating that class has a column name property.
    """

    def get_column_name(self) -> str:
        """
        Return column name.

        Returns
        -------
        str
            Column name.
        """
        pass


class Literal(HasAlias, HasColumnName, RecursiveTreePrint):
    """
    Generic literal.


    Parameters
    ----------
    value : Any
        Literal Value. Can be of any type.
    alias : Optional[str]
        Optional alias.
    """
    def __init__(self, value: Any, alias: Optional[str] = None) -> None:
        self._value: Any = value
        self._alias: Optional[str] = alias

    @property
    def value(self) -> Any:
        """
        Literal value.

        Returns
        -------
        Any
            Literal value.
        """
        return self._value

    def has_alias(self) -> bool:
        return self._alias is not None

    def get_alias(self) -> Optional[str]:
        return self._alias

    def set_alias(self, alias: str) -> None:
        self._alias = alias

    def get_column_name(self) -> str:
        return str(id(self))

    def str_lines_repr(self, indent_level: int) -> Tuple[str, ...]:
        return tuple((
            f'{self._level_indent_string(indent_level)}Literal: {self._value}',
        ))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        return self._value.__eq__(o.value)

    def __hash__(self):
        return hash(self._value)


class Column(HasAlias, HasColumnName, RecursiveTreePrint):
    """
    Table column.

    Parameters
    ----------
    name : str
        Column name.
    alias : Optional[str]
        Optional alias.
    """
    def __init__(self,
                 name: str,
                 alias: Optional[str] = None) -> None:
        assert name, 'Column name is required.'

        self._name: str = name
        self._alias: Optional[str] = alias

    def get_column_name(self) -> str:
        return self._name

    def has_alias(self) -> bool:
        return self._alias is not None

    def get_alias(self) -> Optional[str]:
        if self._alias:
            return self._alias
        else:
            return self._name

    def set_alias(self, alias: str) -> None:
        self._alias = alias

    def str_lines_repr(self, indent_level: int) -> Tuple[str, ...]:
        str_repr = (
            f'{self._level_indent_string(indent_level)}'
            f'Column: {self._name}'
        )
        if self._alias:
            str_repr += f', alias: {self._alias}'
        return tuple((str_repr,))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False
        return self._name.__eq__(o.get_column_name())

    def __hash__(self) -> int:
        return hash(self._name)


class Expression(HasAlias, HasColumnName, RecursiveTreePrint):
    """
    SQL Expression.

    Represent a standard SQL expression such as '>', '+', 'OR', 'IS NULL', ..
    or a generic function such as 'avg', 'np.log', ...

    Parameters
    ----------
    sql_operator : SQLExpression
        SQL Operator of this expression.
    arguments : Tuple[QueryBaseType, ...]
        Expression arguments.
    function_name : Optional[str]
        Function name, if expression is a function.
    alias : Optional[str]
        Optional alias.
    """
    def __init__(self,
                 sql_operator: SQLExpression,
                 arguments: Tuple['QueryBaseType', ...],
                 function_name: str = None,
                 alias: Optional[str] = None,
                 shared_id: Optional[str] = None):
        self._sql_operator: SQLExpression = sql_operator
        self._arguments: Tuple['QueryBaseType', ...] = arguments
        self._function_name: Optional[str] = function_name
        self._alias: Optional[str] = alias
        self._shared_id: Optional[str] = shared_id

    @property
    def sql_operator(self) -> SQLExpression:
        return self._sql_operator

    @property
    def arguments(self) -> Tuple['QueryBaseType', ...]:
        return self._arguments

    def set_arguments(self, arguments: Tuple['QueryBaseType', ...]) -> None:
        self._arguments = arguments

    @property
    def function_name(self) -> Optional[str]:
        return self._function_name

    def has_alias(self) -> bool:
        return self._alias is not None

    def get_alias(self) -> Optional[str]:
        if self._alias:
            return self._alias
        elif self._sql_operator == SQLExpression.FUNCTION:
            return str(self._function_name)
        else:
            return None

    def set_alias(self, alias: str) -> None:
        self._alias = alias

    def is_shared(self) -> bool:
        """
        Is expression shared.

        Expression is shared if exactly the same expression is used
        in multiple places in a query. For example in SELECT and in
        ORDER BY clauses.

        Returns
        -------
        bool
            True if expression is shared.
        """
        return bool(self._shared_id)

    def get_shared_id(self) -> str:
        """
        Return shared ID.

        Returns
        -------
        str
            Shared expressions ID.
        """
        assert self._shared_id
        return self._shared_id

    def set_shared_id(self, shared_id: str) -> None:
        """
        Set shared ID.

        Parameters
        ----------
        shared_id : str
            Shared expressions ID.
        """
        self._shared_id = shared_id

    def str_lines_repr(self, indent_level: int) -> Tuple[str, ...]:
        lines = []

        operator_line = (f'{self._level_indent_string(indent_level)}'
                         f'Expression: {self._sql_operator}')
        if self._sql_operator == SQLExpression.FUNCTION:
            operator_line = f'{operator_line}: {self.function_name}'
        if self.is_shared():
            operator_line = f'{operator_line}, IS_SHARED'

        lines.append(operator_line)

        for arg in self._arguments:
            lines.extend(arg.str_lines_repr(indent_level + 1))

        lines.append('')
        return tuple(lines)

    def copy(self) -> 'Expression':
        return Expression(
            self._sql_operator,
            self._arguments,
            self._function_name,
            self._alias,
            self._shared_id
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, self.__class__):
            return False

        if (self._sql_operator != o.sql_operator
                or self._function_name != o.function_name):
            return False

        for arg_one, arg_two in zip(self._arguments, o.arguments):
            if not arg_one.__eq__(arg_two):
                return False

        return True

    def __hash__(self):
        return hash(
            (self._sql_operator,
             self._function_name,
             tuple(hash(arg) for arg in self._arguments))
        )


class Query:
    """
    Query AST.

    Represents a query syntax tree.
    Query is comprised of functional blocks, where each block may represent
    a tree of expressions.

    Parameters
    ----------
    schema : pa.Schema
        Schema.
    select_expressions : Tuple['QueryBaseType', ...]
        SELECT clause expressions.
    distinct : bool
        True if DISTINCT is on.
    where_condition : Optional[Expression]
        WHERE clause expression.
    group_by : Tuple['QueryBaseType', ...]
        GROUP BY expressions.
    having : Optional[Expression]
        HAVING clause expression.
    order_by : Tuple['QueryBaseType', ...]
        ORDER BY clause expressions.
    sort_order : Tuple[SortOrder, ...]
        Sort order of the 'order_by' list, ASC / DESC.
    limit : Optional[int]
        LIMIT x.
    offset : int
        LIMIT .., OFFSET.
    """
    def __init__(self,
                 schema: 'pa.Schema',
                 select_expressions: Tuple['QueryBaseType', ...],
                 is_aggregate: bool,
                 distinct: bool,
                 where_condition: Optional[Expression],
                 group_by: Tuple['QueryBaseType', ...],
                 having: Optional[Expression],
                 order_by: Tuple['QueryBaseType', ...],
                 sort_order: Tuple[SortOrder, ...],
                 limit: Optional[int],
                 offset: int,
                 ):
        self.schema: 'pa.Schema' = schema
        self.select_expressions: Tuple['QueryBaseType', ...] = \
            select_expressions
        self._is_aggregate: bool = is_aggregate
        self.distinct: bool = distinct
        self.where_condition: Optional[Expression] = where_condition
        self.group_by: Tuple['QueryBaseType', ...] = group_by
        self.having: Optional[Expression] = having
        self.order_by: Tuple['QueryBaseType', ...] = order_by
        self.sort_order: Tuple[SortOrder, ...] = sort_order
        self.limit: Optional[int] = limit
        self.offset: int = offset

    def is_aggregate(self) -> bool:
        """
        Return True if query is aggregate.

        Returns
        -------
        bool
            True if query query is aggregate.
        """
        return self._is_aggregate or self.distinct

    def has_limit(self) -> bool:
        """
        Return True if query has LIMIT defined.

        Returns
        -------
        bool
            True if query has a LIMIT clause.
        """
        return self.limit is not None

    def get_all_used_columns(self) -> FrozenSet[Column]:
        """
        Return all the Columns referenced in the query.

        Method recursively searches for referenced Columns,
        in SELECT, WHERE, GROUP BY, HAVING, ORDER BY blocks.

        Returns
        -------
        FrozenSet[Column]
            Set of all the unique columns referenced in the query.
        """
        branches: List['QueryBaseType'] = []
        for expr_branch in (self.select_expressions,
                            self.where_condition,
                            self.group_by,
                            self.having,
                            self.order_by,
                            ):
            if expr_branch:
                append_flat(branches, expr_branch)
        return frozenset(find_all_columns_recursively(tuple(branches)))

    def get_all_used_column_names(self) -> FrozenSet[str]:
        """
        Return all the column names referenced in the query.

        Method recursively searches for referenced Columns,
        in SELECT, WHERE, GROUP BY, HAVING, ORDER BY blocks and then
        extracts column names.

        Returns
        -------
        FrozenSet[Column]
            Set of all the unique column names referenced in the query.
        """
        return frozenset(
            c.get_column_name() for c in self.get_all_used_columns()
        )

    def get_select_plus_post_agg_cols(
            self) -> Tuple['QueryBaseType', ...]:
        """
        Return all the SELECT expressions, plus all the Columns
        referenced in post-processing sections, such as HAVING and ORDER BY.

        Returns
        -------
        Tuple['QueryBaseType', ...]
            List of all the SELECT expressions, plus Columns referenced
            in post-processing sections.
        """
        columns = list(self.select_expressions)
        cols_set = set(columns)
        for group in ([self.having], self.order_by):
            group = tuple(cast(Tuple['QueryBaseType', ...], group))
            for col in find_all_columns_recursively(
                    tuple(group), skip_shared_expressions=True):
                if col not in cols_set:
                    columns.append(col)
                    cols_set.add(col)
        return tuple(columns)

    def has_count_star(self):
        for expr in self.select_expressions:
            if (is_expression(expr)
                    and expr.function_name
                    and expr.function_name.lower() == 'count_star'):
                return True

    def __str__(self):
        str_repr = 'Query syntax tree: \n'

        if self.distinct:
            str_repr += TREE_INDENT_SYMBOL + 'DISTINCT ON\n'

        str_repr += TREE_INDENT_SYMBOL + 'SELECT: \n'
        for sel_expr in self.select_expressions:
            if isinstance(sel_expr, Expression):
                str_repr += sel_expr.to_str(2)
            else:
                str_repr += (f'{TREE_INDENT_SYMBOL}{TREE_INDENT_SYMBOL}'
                             f'{sel_expr}\n')
        str_repr += '\n'

        if self.where_condition:
            str_repr += TREE_INDENT_SYMBOL + 'WHERE: \n'
            str_repr += f'{self.where_condition.to_str(2)}\n'

        if self.group_by:
            str_repr += TREE_INDENT_SYMBOL + 'GROUP BY: \n'
            for group_expr in self.group_by:
                str_repr += f'{group_expr.to_str(2)}\n'
            str_repr += '\n'

        if self.having:
            str_repr += TREE_INDENT_SYMBOL + 'HAVING: \n'
            str_repr += f'{self.having.to_str(2)}\n'
            str_repr += '\n'

        if self.order_by:
            str_repr += TREE_INDENT_SYMBOL + 'ORDER BY: \n'
            for order_by_expr, sort_order in zip(self.order_by,
                                                 self.sort_order):
                str_repr += f'{order_by_expr.to_str(2)} {sort_order}\n'
            str_repr += '\n'

        if self.has_limit():
            str_repr += TREE_INDENT_SYMBOL + 'LIMIT: '
            str_repr += str(self.limit)
            if self.offset:
                str_repr += f' OFFSET: {self.offset}'
            str_repr += '\n'

        return str_repr
