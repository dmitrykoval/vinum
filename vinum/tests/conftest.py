import pyarrow
import pytest

import numpy as np
import requests

from vinum.arrow.arrow_table import ArrowTable
from vinum.api.table import Table
from vinum.parser.parser import parser_factory
from vinum.parser.query import Column, Literal


def rows_to_columns_dict(rows, column_names):
    cols_dict = {}

    if not rows:
        return cols_dict

    for col_idx, col_name in enumerate(column_names):
        cols_dict[col_name] = [
            rows[row_idx][col_idx] for row_idx in range(len(rows))
        ]
    return cols_dict


def create_test_data():
    column_names = (
        'id', 'timestamp', 'vendor_id', 'city_from', 'city_to',
        'lat', 'lng', 'name', 'tax', 'tip', 'total'
    )
    test_dict = rows_to_columns_dict(
        (
            (1, 1596899421, 1, 'Berlin', 'Munich', 52.51, 13.66,
             'Joe', 0.43, 1, 2.43),
            (2, 1596999422, 2, 'Munich', 'Riva', 48.51, 12.3,
             'Jonas', 2.0, 5.34, 143.15),
            (3, 1597899423, 1, 'Riva', 'Naples', 44.89, 14.23,
             'Joseph', 1.59, 11, 33.40),
            (4, 1598899424, 3, 'San Francisco', 'Naples', 42.89, 15.89,
             'Joseph', 1.69, 5, 53.1),
        ),
        column_names)
    test_table = Table(pyarrow.Table.from_pydict(test_dict))
    return column_names, test_dict, test_table


def create_test_groupby_data():
    column_names = (
        'id', 'timestamp', 'date', 'vendor_id', 'city_from', 'city_to',
        'lat', 'lng', 'name', 'tax', 'tip', 'total'
    )
    test_dict = rows_to_columns_dict(
        (
            (1, 1602127614, '2020-10-08T03:26:54', 1, 'Berlin', 'Munich',
             52.51, 13.66, 'Joe', 0.43, 1, 2.43),
            (2, 1602217613, '2020-10-09T04:26:53', 2, 'Munich', 'Riva',
             48.51, 12.3, 'Jonas', 2.0, 4.34, 143.15),
            (3, 1602304012, '2020-10-10T04:26:52', 1, 'Riva', 'Naples',
             44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
            (4, 1602390411, '2020-10-11T04:26:51', 3, 'San Francisco',
             'Naples', 42.89, 15.89, 'Joseph', 1.69, 5.3, 53.1),
            (5, 1602476810, '2020-10-12T04:26:50', 1, 'Berlin', 'Riva',
             44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
            (6, 1602563209, '2020-10-13T04:26:49', 2, 'Munich', 'Riva',
             48.51, 12.3, 'Jonas', 2.0, 5.34, 13.15),
            (7, 1602649608, '2020-10-14T04:26:48', 1, 'Berlin', 'Munich',
             44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
            (8, 1602736007, '2020-10-15T04:26:47', 1, 'Berlin', 'Munich',
             52.51, 13.66, 'Joe', 0.43, 0.4, 2.43),
        ),
        column_names)
    return column_names, Table(pyarrow.Table.from_pydict(test_dict))


def create_null_test_data():
    column_names = (
        'id', 'timestamp', 'date', 'is_vendor', 'city_from', 'city_to',
        'lat', 'lng', 'name', 'total'
    )
    test_dict = rows_to_columns_dict(
        (
            (1, 1602127614, None, True, None, 'Munich', 52.51, 13.66,
             'Joe', None),
            (2, 1602217613, '2020-10-09T04:26:53', True, 'Munich', 'Riva',
             48.51, 12.3, None, 143.15),
            (3, 1602304012, '2020-10-10T04:26:52', False, None, 'Naples',
             44.89, 14.23, 'Joseph', 33.40),
            (4, 1602390411, '2020-10-11T04:26:51', None, 'San Francisco',
             'Naples', 42.89, 15.89, 'Joseph', 53.1),
            (5, None, '2020-10-12T04:26:50', True, 'Berlin', 'Riva',
             44.89, 14.23, None, np.nan),
            (6, 1602563209, '2020-10-13T04:26:49', None, 'Munich',
             'Riva', 48.51, 12.3, 'Jonas', None),
            (7, None, None, None, 'Berlin', 'Munich', 44.89, 14.23, 'Joseph',
             33.40),
            (8, 1602736007, '2020-10-15T04:26:47', None, 'Berlin', 'Munich',
             52.51, 13.66, 'Joe', np.nan),
        ),
        column_names)
    test_table = Table(pyarrow.Table.from_pydict(test_dict))
    return column_names, test_dict, test_table


def create_query_ast(query, test_arrow_table):
    return parser_factory(query,
                          test_arrow_table.get_schema()).generate_query_tree()


def _test_column(column, name, alias=None):
    assert column is not None
    assert isinstance(column, Column)
    assert column.get_column_name() == name
    assert column._alias == alias


def _test_literal(literal, value_var, alias=None):
    assert literal is not None
    assert isinstance(literal, Literal)
    assert literal.value == value_var
    if alias:
        assert literal.get_alias() is not None
        assert literal.get_alias() == alias
    else:
        assert literal.get_alias() is None


def _assert_tables_equal(actual, expected):
    assert actual is not None

    arrow_table: ArrowTable = actual._arrow_table.combine_chunks()
    assert arrow_table is not None
    assert arrow_table.num_columns == len(expected.keys())
    assert arrow_table.num_rows == len(next(iter(expected.values())))

    for column_name in expected.keys():
        expected_col = np.array(expected[column_name])
        result_col = arrow_table.get_np_column_by_name(column_name)
        if np.issubdtype(result_col.dtype, np.float):
            assert np.allclose(result_col, expected_col, equal_nan=True)
        else:
            assert np.array_equal(result_col, expected_col)


@pytest.fixture(scope="module")
def test_arrow_table():
    _, table_dict, __ = create_test_data()
    return ArrowTable(pyarrow.Table.from_pydict(table_dict))


@pytest.fixture(scope="module")
def test_table_column_names():
    column_names, _, __ = create_test_data()
    return column_names


def download_save(url, tmpdir_factory):
    res = requests.get(url)
    res.raise_for_status()

    fname = url.split('/')[-1]
    tmp_datafile = str(tmpdir_factory.mktemp("data").join(fname))
    with open(tmp_datafile, 'wb') as f:
        f.write(res.content)

    return tmp_datafile


@pytest.fixture(scope="session")
def csv_datafile(tmpdir_factory):
    url = 'https://raw.githubusercontent.com/dmitrykoval/vinum-test-data/main/data/taxi.csv.bz2'
    return download_save(url, tmpdir_factory)

@pytest.fixture(scope="session")
def parquet_datafile(tmpdir_factory):
    url = 'https://raw.githubusercontent.com/dmitrykoval/vinum-test-data/main/data/taxi.parquet'
    return download_save(url, tmpdir_factory)
