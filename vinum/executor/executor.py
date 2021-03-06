from vinum.arrow.arrow_table import ArrowTable
from vinum.core.base import Operator


class Executor:
    """
    Abstract Executor.

    Responsible for execution of a query plan.
    """

    def execute(self, operator: Operator) -> ArrowTable:
        """
        Execute query plan.

        Parameters
        ----------
        operator : Operator
            Root of the query plan tree.
        """
        pass


class RecursiveExecutor(Executor):
    """
    Single-Threaded Recursive Executor.

    Execute the query plan recursively.
    """
    def execute(self, operator: Operator) -> ArrowTable:
        return next(operator.next())
