from typing import Tuple, Any, Dict

from vinum.arrow.arrow_table import ArrowTable
from vinum.core.operators.generic_operators import (
    Operator,
    AbstractCombineGroupByGroupsOperator,
)
from vinum.errors import ExecutorError


class Executor:
    """
    Abstract Executor.

    Responsible for execution of an Operators DAG.
    Takes care of the execution order and makes sure that all the
    arguments required for an operator are processed and available
    (ie all the operators passed arguments are already executed
    and results are available).

    Executor is responsible for GROUP BY processing orchestration.
    The Group By execution contract is:
        - When `set_groupby_groups` is called, executor enters
            the group by mode. In the group by mode each operation is executed
            not once for a single shared table, but once for each group,
            where each group's data is represented by a separate ArrowTable.
        - In the group by mode, when Executor detects an operator, which is
            a subclass of `AbstractCombineGroupByGroupsOperator`,
            it executes it only once, not once for each group.
        - Method `get_operator_result_for_all_groups` allows to collect
            the results for all groups for a given operator.
        - When `clear_groupby_groups` is called, all the group by groups
            are removed and Executor exits the group by mode.

    When the DAG is passed to `execute_dag`, it's not aware of what
    instance of Executor would be responsible for execution.
    Nevertheless, Operators need to communicate with Executor to get
    results of other operators (via `get_operator_result`),
    or group by results (via `get_operator_result_for_all_groups`).
    Therefore it's a responsibility of an Executor to set executor instance
    for all the operators in the DAG. See `_set_executor_for_dag`
    """

    def execute_dag(self, dag: Operator) -> None:
        """
        Execute DAG.

        Execute the DAG and take care of the execution order
        and GROUP BY semantics.

        Data transformations are done on the instance of a shared ArrowTable.

        Parameters
        ----------
        dag : Operator
            Operators DAG.
        """
        pass

    def get_operator_result(self,
                            operator: Operator,
                            table: ArrowTable) -> Any:
        """
        Return Operator results.

        Assumes the passed operator has already been executed and the result
        is available. If the result is not available raises `ExecutorError`.

        Parameters
        ----------
        operator : Operator
            Operator to return results for.
        table : ArrowTable
            For what table return results for. Table is used in group by
            mode to understand for what group results are requested.

        Returns
        -------
        Any
            Operator result.
        """
        pass

    def set_groupby_groups(self, groups: Tuple[ArrowTable, ...]) -> None:
        """
        Set GROUP BY groups.

        Each group is represented by a separate ArrowTable object.
        Calling this method switches an Executor into a group by mode.

        Parameters
        ----------
        groups : Tuple[ArrowTable, ...]
            Group by groups.
        """
        pass

    def collect_operator_result_for_all_groups(self,
                                               operator: Operator) -> Tuple:
        """
        Return the results for all the groups for a given operator.

        Parameters
        ----------
        operator : Operator
            Operator for which to collect groups results.

        Returns
        -------
        Tuple
            Tuple with results for all the group by groups.
        """
        pass

    def clear_groupby_groups(self) -> None:
        """
        Clear all group by groups and exit the group by mode.
        """
        pass

    def _set_executor_for_dag(self, operator: Operator):
        operator.set_executor(self)
        for op in operator.get_child_operators():
            self._set_executor_for_dag(op)


class RecursiveExecutor(Executor):
    """
    Single-Threaded Recursive Executor.

    Execute the DAG via recursion.
    """
    def __init__(self) -> None:
        self._operators_results: Dict[Operator, Any] = {}
        self._groupby_groups: Dict[ArrowTable, Dict[Operator, Any]] = {}

    def execute_dag(self, dag: Operator) -> None:
        self._set_executor_for_dag(dag)
        self._execute_recursively(dag)

    def get_operator_result(self,
                            operator: Operator,
                            table: ArrowTable) -> Any:
        if table in self._groupby_groups:
            return self._groupby_groups[table][operator]
        elif operator in self._operators_results:
            return self._operators_results[operator]
        else:
            raise ExecutorError(
                'Operator results are not available, '
                f'Operator {operator}.'
            )

    def set_groupby_groups(self, groups: Tuple[ArrowTable, ...]) -> None:
        for table in groups:
            self._groupby_groups[table] = {}

    def collect_operator_result_for_all_groups(self,
                                               operator: Operator) -> Tuple:
        return self._collect_groups_for_operator(operator)

    def clear_groupby_groups(self) -> None:
        self._groupby_groups = {}

    def _execute_recursively(self, operator: Operator) -> None:
        # Check if other execution branch has already processed this operator.
        if operator in self._operators_results:
            return

        for child in operator.get_child_operators():
            self._execute_recursively(child)

        if (self._groupby_groups
                and not isinstance(operator,
                                   AbstractCombineGroupByGroupsOperator)):
            for table, results in self._groupby_groups.items():
                operator.set_table(table)
                results[operator] = operator.execute()
        else:
            result = operator.execute()
            self._operators_results[operator] = result

    def _collect_groups_for_operator(self, operator: Operator) -> Tuple:
        result_groups = []
        for table_results in self._groupby_groups.values():
            result_groups.append(table_results[operator])
        return tuple(result_groups)
