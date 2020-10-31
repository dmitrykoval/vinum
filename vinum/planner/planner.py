from vinum.arrow.arrow_table import ArrowTable
from vinum.core.operators.generic_operators import Operator
from vinum.parser.query import Query


class QueryPlanner:
    """
    Abstract Query Planner.

    Query Planner is responsible for transforming a Query AST into
    a directed acyclic graph of operators, performing data transformations.

    Essentially, query planner outlines the steps needed to execute the query.

    Parameters
    ----------
    query : Query
        Query syntax tree.
    table : ArrowTable
        Data table.
    """
    def __init__(self, query: Query, table: ArrowTable) -> None:
        super().__init__()
        self._query = query
        self._table = table

    def plan_query(self) -> Operator:
        """
        Create a query execution plan.

        Using a query AST, generate a query DAG outlining the operations
        needed to be performed to execute a query.

        Returns
        -------
        Operator
            Query plan DAG.
        """
        pass
