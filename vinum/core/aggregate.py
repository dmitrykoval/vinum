import pyarrow as pa

from typing import Iterable

import vinum_lib

from vinum.arrow.record_batch import RecordBatch
from vinum.core.base import Operator, VectorizedExpression
from vinum.parser.query import Column


class AggregateFunction(VectorizedExpression):
    """
    Abstract Base Class for all aggregate functions.
    """
    def __init__(self, func, column: Column = None) -> None:
        super().__init__([])
        if not column:
            self._input_column_name = ''
        else:
            self._input_column_name = column.get_column_name()
        assert func
        self._func = func

    def get_input_column_name(self) -> str:
        return self._input_column_name

    def get_agg_func_name(self):
        return self._func.upper()


class AggregateOperator(Operator):
    """
    Aggregate Operator.

    Class is thin proxy which delegates all the work to C++ based
    physical operators.
    """

    FUNCS = {
        'COUNT': vinum_lib.AggFuncType.COUNT,
        'COUNT_STAR': vinum_lib.AggFuncType.COUNT_STAR,
        'MIN': vinum_lib.AggFuncType.MIN,
        'MAX': vinum_lib.AggFuncType.MAX,
        'SUM': vinum_lib.AggFuncType.SUM,
        'AVG': vinum_lib.AggFuncType.AVG,
    }

    def __init__(self,
                 parent_operator: 'Operator',
                 group_by_columns: Iterable[Column],
                 agg_funcs: Iterable[AggregateFunction],
                 agg_cols: Iterable[Column],
                 ) -> None:
        super().__init__(parent_operator, None)

        self._group_by_columns = group_by_columns
        self._agg_funcs = agg_funcs
        self._agg_cols = agg_cols

        self.agg_obj = None

    def _is_numeric_type(self, field_type):
        return (pa.types.is_integer(field_type)
                or pa.types.is_floating(field_type)
                or pa.types.is_temporal(field_type))

    def _init_agg_obj(self, batch):
        schema = batch.get_schema()

        only_numer_groupby = True
        groupby_col_names = []

        for c in self._group_by_columns:
            col_name = c.get_column_name()
            groupby_col_names.append(col_name)
            if not self._is_numeric_type(schema.field(col_name).type):
                only_numer_groupby = False

        agg_col_names = [
            c.get_column_name()
            for c in self._agg_cols
        ]

        agg_funcs = []
        for func in self._agg_funcs:
            col_name = func.get_input_column_name()
            out_col_name = func.get_column_name()
            func_def = vinum_lib.AggFuncDef(
                self.FUNCS[func.get_agg_func_name()],
                col_name,
                out_col_name
            )
            agg_funcs.append(func_def)

        if len(groupby_col_names) == 0:
            agg_obj = vinum_lib.OneGroupAggregate(agg_funcs)
        else:
            if only_numer_groupby and len(groupby_col_names) == 1:
                agg_class = vinum_lib.SingleNumericalHashAggregate
            elif only_numer_groupby:
                agg_class = vinum_lib.MultiNumericalHashAggregate
            else:
                agg_class = vinum_lib.GenericHashAggregate

            agg_obj = agg_class(
                groupby_col_names,
                agg_col_names,
                agg_funcs
            )

        self.agg_obj = agg_obj

    def next(self) -> Iterable[RecordBatch]:
        for batch in self._parent_operator.next():
            if not self.agg_obj:
                self._init_agg_obj(batch)

            self.agg_obj.next(batch.get_batch())

        if self.agg_obj:
            yield RecordBatch(self.agg_obj.result())

        del self.agg_obj
