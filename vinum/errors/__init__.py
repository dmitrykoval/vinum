

class ParserError(Exception):
    """
    Error raised if SQL query cannot be parsed.

    For example is the syntax of the query is bad,
    or table columns cannot be found.
    """
    pass


class PlannerError(Exception):
    """
    Planner Error is raised when planner is unable to build a plan,
    given query AST.

    For example when aggregate and non-aggregate functions are mixed
    in the select clause.
    """
    pass


class OperatorError(Exception):
    """
    Operator Error is raised if Operator is unable to execute its normal
    flow, for example due to missing arguments or incorrect types of
    arguments, etc.
    """
    pass


class FunctionError(Exception):
    """
    Function error is raised when built-in or UDF functions cannot be found,
    function is not appropriate type, ie non-aggregate function is requested
    in the GROUP BY mode.
    """
    pass


class ExecutorError(Exception):
    """
    Executor Error is raised when some error occurs during execution.
    For example when results for requested Operator are not available.
    """
    pass
