import pytest

import pyarrow as pa
import pandas as pd

from vinum import Table
from vinum.tests.conftest import _assert_tables_equal

TEST_DATA_1 = (
    ({'col1': [1, 2, 3], 'col2': [7, 13, 17]},
     'select * from t',
     {
         'col1': (1, 2, 3),
         'col2': (7, 13, 17)
     }),
)


class TestTableAPI:

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_from_pydict(self, input, query, expected_result):
        actual_tbl = Table.from_pydict(input).sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_from_pyarrow(self, input, query, expected_result):
        arrow_table = pa.Table.from_pydict(input)
        tbl = Table.from_arrow(arrow_table)
        actual_tbl = tbl.sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_from_pandas(self, input, query, expected_result):
        pdf = pd.DataFrame(data=input)
        tbl = Table.from_pandas(pdf)
        actual_tbl = tbl.sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_sql_pd(self, input, query, expected_result):
        pd_df = Table.from_pydict(input).sql_pd(query)
        assert isinstance(pd_df, pd.DataFrame)

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_explain(self, input, query, expected_result):
        query_plan = Table.from_pydict(input).explain(
            'select col1, sum(col2) from t '
            'where col1 > 10 '
            'group by col2, col1 '
            'having col1 > 100 '
            'order by col1 '
            'limit 10 offset 20',
            print_query_tree=True)
        assert query_plan

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_head(self, input, query, expected_result):
        query_plan = Table.from_pydict(input).head(1)
        assert len(query_plan) == 1

    @pytest.mark.parametrize("input, query, expected_result", TEST_DATA_1)
    def test_schema(self, input, query, expected_result):
        schema = Table.from_pydict(input).schema
        assert isinstance(schema, pa.Schema)
