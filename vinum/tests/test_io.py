import pytest

from vinum import read_csv, stream_csv, read_parquet
from vinum.tests.conftest import csv_datafile, _assert_tables_equal

QUERIES = [
    ("select passenger_count pc, "
     "count(*), "
     "min(pickup_longitude) min_3, "
     "max(pickup_longitude) max_3, "
     "sum(pickup_longitude) sum_3, "
     "avg(pickup_longitude) avg_3, "
     "min(pickup_datetime) min_2, "
     "max(pickup_datetime) max_2 "
     "from t group by pc order by pc",
     {
         'pc': (0, 1, 2, 3, 4, 5, 6),
         'count': (165, 34808, 7386, 2183, 1016, 3453, 989),
         'min_3': (-74.009903, -75.423848316623, -75.414728173543, -74.711648,
                   -74.15226, -74.17716, -74.017193),
         'max_3': (0.0, 40.783472, 40.766125, 0.0,
                   40.76115, 40.76911, 40.760152),
         'sum_3': (-11909.803498292, -2521989.92686543, -536827.954870309,
                   -158829.103304955, -73490.7587017238, -250249.420533896,
                   -72190.8110274745),
         'avg_3': (-72.1806272623757, -72.4543187446974, -72.6818243799497,
                   -72.757262164432, -72.3334239190195, -72.4730438847077,
                   -72.9937421915819),
         'min_2': ("2011-01-09 10:09:58 UTC", "2009-01-01 01:31:49 UTC",
                   "2009-01-01 02:51:52 UTC", "2009-01-01 02:07:49 UTC",
                   "2009-01-03 15:19:44 UTC", "2009-01-01 15:19:00 UTC",
                   "2009-01-11 22:38:00 UTC"),
         'max_2': ("2015-06-02 23:16:15 UTC", "2015-06-30 22:42:39 UTC",
                   "2015-06-30 10:58:55 UTC", "2015-06-28 00:11:39 UTC",
                   "2015-06-28 10:14:25 UTC", "2015-06-30 20:50:04 UTC",
                   "2015-06-30 06:25:34 UTC"),
     }),
    ("select fare_amount, "
     "count(*), "
     "avg(fare_amount) "
     "from taxi "
     "where fare_amount < 5 "
     "group by passenger_count, fare_amount "
     "order by fare_amount, passenger_count",
     {
         'fare_amount': (-5.0, -3.0, -2.9, -2.5, 0.0, 0.01, 2.5, 2.5, 2.5, 2.5,
                         2.5, 2.5, 2.9, 2.9, 2.9, 2.9, 2.9, 3.0, 3.0, 3.0, 3.0,
                         3.0, 3.3, 3.3, 3.3, 3.3, 3.3, 3.3, 3.3, 3.5, 3.5, 3.5,
                         3.5, 3.5, 3.5, 3.7, 3.7, 3.7, 3.7, 3.7, 3.7, 3.7, 3.8,
                         3.8, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.1, 4.1, 4.1, 4.1,
                         4.1, 4.1, 4.1, 4.2, 4.2, 4.3, 4.5, 4.5, 4.5, 4.5, 4.5,
                         4.5, 4.5, 4.6, 4.6, 4.6, 4.7, 4.7, 4.9, 4.9, 4.9, 4.9,
                         4.9, 4.9, 4.9),
         'count': (1, 1, 1, 3, 3, 1, 1, 169, 15, 4, 12, 2, 87, 20, 4, 1, 6, 1,
                   68, 7, 1, 7, 5, 264, 55, 22, 11, 24, 1, 178, 24, 14, 4, 13,
                   6, 2, 501, 98, 38, 11, 54, 7, 1, 1, 324, 57, 22, 4, 25, 14,
                   7, 762, 129, 44, 14, 100, 13, 6, 2, 1, 10, 1488, 290, 86,
                   44, 157, 29, 11, 3, 1, 1, 1, 9, 959, 213, 63, 31, 107, 15),
         'avg': (-5.0, -3.0, -2.9, -2.5, 0.0, 0.01, 2.5, 2.5, 2.5, 2.5, 2.5,
                 2.5, 2.9, 2.9, 2.9, 2.9, 2.9, 3.0, 3.0, 3.0, 3.0, 3.0, 3.3,
                 3.29999999999999, 3.3, 3.3, 3.3, 3.3, 3.3, 3.5, 3.5, 3.5,
                 3.5, 3.5, 3.5, 3.7, 3.70000000000003, 3.69999999999999, 3.7,
                 3.7, 3.7, 3.7, 3.8, 3.8, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.1,
                 4.09999999999994, 4.10000000000001, 4.1, 4.1, 4.10000000000001,
                 4.1, 4.2, 4.2, 4.3, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.5, 4.6,
                 4.6, 4.6, 4.7, 4.7, 4.9, 4.90000000000001, 4.89999999999999,
                 4.9, 4.9, 4.89999999999999, 4.9),
     })
]


class TestQueryResults:

    @pytest.mark.parametrize("query, expected_result", QUERIES)
    def test_read_csv(self, query, expected_result, csv_datafile):
        actual_tbl = read_csv(csv_datafile).sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize("query, expected_result", QUERIES)
    def test_open_csv(self, query, expected_result, csv_datafile):
        actual_tbl = stream_csv(csv_datafile).sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize("query, expected_result", QUERIES)
    def test_read_parquet(self, query, expected_result, parquet_datafile):
        actual_tbl = read_parquet(parquet_datafile).sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

