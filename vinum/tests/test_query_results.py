from datetime import date, datetime

import pytest

import numpy as np

from vinum.core.udf import register_python, register_numpy
from vinum.tests.conftest import (
    create_test_data,
    create_null_test_data,
    rows_to_columns_dict,
    create_test_groupby_data,
    _assert_tables_equal
)

column_names, test_dict, test_table = create_test_data()
groupby_column_names, test_groupby_table = create_test_groupby_data()
_, __, test_table_null = create_null_test_data()

queries = (
    (test_table,
     "select * from t",
     test_dict
     ),

    (test_table,
     "select 2+2 as sum from t",
     {
         'sum': (4,),
     }),

    (test_table,
     "select 2+2+2+1 as sum from t",
     {
         'sum': (7,),
     }),

    (test_table,
     "select 1*2*3*4*5*6*7 as sum from t",
     {
         'sum': (5040,),
     }),

    (test_table,
     "select 2 as literal from t",
     {
         'literal': (2,),
     }),

    (test_table,
     "select tax+tip as res from t",
     {
         'res': (1.43, 7.34, 12.59, 6.69),
     }),

    (test_table,
     "select total-tax as res from t",
     {
         'res': (2.0, 141.15, 31.81, 51.41),
     }),

    (test_table,
     "select tax*tip as res from t",
     {
         'res': (0.43, 10.68, 17.49,  8.45),
     }),

    (test_table,
     "select id % 2 as res from t",
     {
         'res': (1, 0, 1, 0),
     }),

    (test_table,
     "select -id as res from t",
     {
         'res': (-1, -2, -3, -4),
     }),

    (test_table,
     "select ~id as res from t",
     {
         'res': (-2, -3, -4, -5),
     }),

    (test_table,
     "select id & timestamp as res from t",
     {
         'res': (1, 2, 3, 0),
     }),

    (test_table,
     "select id | timestamp as res from t",
     {
         'res': (1596899421, 1596999422, 1597899423, 1598899428),
     }),

    (test_table,
     "select id # timestamp as res from t",
     {
         'res': (1596899420, 1596999420, 1597899420, 1598899428),
     }),

    (test_table,
     "select city_from || city_to as res from t",
     {
         'res': ('BerlinMunich', 'MunichRiva',
                 'RivaNaples', 'San FranciscoNaples'),
     }),

    (test_table,
     "select city_from || city_to as res from t",
     {
         'res': ('BerlinMunich', 'MunichRiva',
                 'RivaNaples', 'San FranciscoNaples'),
     }),

    (test_table,
     "select '<_' || city_from || '_-_' || city_to || '_>' as res from t",
     {
         'res': ('<_Berlin_-_Munich_>', '<_Munich_-_Riva_>',
                 '<_Riva_-_Naples_>', '<_San Francisco_-_Naples_>'),
     }),

    (test_table,
     "select count(*) as count from t",
     {
         'count': (4,),
     }),

    (test_table,
     "select count(*) as count from t where vendor_id >= 2",
     {
         'count': (2,),
     }),

    (test_table,
     "select count(*) as count from t where vendor_id >= 2000",
     {
         'count': (0,),
     }),

    (test_table,
     "select count(lng * 10 > 130) as count from t",
     {
         'count': (4,),
     }),

    (test_table,
     "select distinct vendor_id from t",
     {
         'vendor_id': (1, 2, 3),
     }),

    (test_table,
     "select np.sum(total) as sum from t where vendor_id >= 2",
     {
         'sum': (196.25,),
     }),

    (test_table,
     "select * from t where vendor_id = 1",
     rows_to_columns_dict(
         [
             (1, 1596899421, 1, 'Berlin', 'Munich', 52.51, 13.66,
              'Joe', 0.43, 1, 2.43),
             (3, 1597899423, 1, 'Riva', 'Naples', 44.89, 14.23,
              'Joseph', 1.59, 11, 33.40),
         ],
         column_names
     )),

    (test_table,
     "select * from t where vendor_id != 1",
     rows_to_columns_dict(
         [
             (2, 1596999422, 2, 'Munich', 'Riva', 48.51, 12.3,
              'Jonas', 2.0, 5.34, 143.15),
             (4, 1598899424, 3, 'San Francisco', 'Naples', 42.89, 15.89,
              'Joseph', 1.69, 5, 53.1),
         ],
         column_names
     )),

    (test_table,
     "select id from t where vendor_id == 1",
     {
         'id': (1, 3),
     }),

    (test_table,
     "select id from t where vendor_id <> 1",
     {
         'id': (2, 4),
     }),

    (test_table,
     "select * from t where vendor_id >= 2",
     rows_to_columns_dict(
         [
             (2, 1596999422, 2, 'Munich', 'Riva', 48.51, 12.3,
              'Jonas', 2.0, 5.34, 143.15),
             (4, 1598899424, 3, 'San Francisco', 'Naples', 42.89, 15.89,
              'Joseph', 1.69, 5, 53.1),
         ],
         column_names
     )),

    (test_table,
     "select * from t where name like 'Jos%'",
     rows_to_columns_dict(
         [
             (3, 1597899423, 1, 'Riva', 'Naples', 44.89, 14.23,
              'Joseph', 1.59, 11, 33.40),
             (4, 1598899424, 3, 'San Francisco', 'Naples', 42.89, 15.89,
              'Joseph', 1.69, 5, 53.1),
         ],
         column_names
     )),

    (test_table,
     "select * from t where name not like 'Jos%'",
     rows_to_columns_dict(
         [
             (1, 1596899421, 1, 'Berlin', 'Munich', 52.51, 13.66,
              'Joe', 0.43, 1, 2.43),
             (2, 1596999422, 2, 'Munich', 'Riva', 48.51, 12.3,
              'Jonas', 2.0, 5.34, 143.15),
         ],
         column_names
     )),

    (test_table,
     "select id from t where total between 10 and 100",
     {
         'id': (3, 4),
     }),

    (test_table,
     "select id from t where total not between 10 and 100",
     {
         'id': (1, 2),
     }),

    (test_table,
     "select timestamp from t where id in (2, 3)",
     {
         'timestamp': (1596999422, 1597899423),
     }),

    (test_table,
     "select timestamp from t where id not in (2, 3)",
     {
         'timestamp': (1596899421, 1598899424),
     }),

    (test_table,
     "select id from t where lat * 10 > 440",
     {
         'id': (1, 2, 3),
     }),

    (test_table,
     ("select id from t "
      "where id = 4 or total / 10 > 10.1 or city_from like '%iv%'"),
     {
         'id': (2, 3, 4)
     }),

    (test_table,
     ("select id from t "
      "where id = 3 and timestamp - 1 = 1597899422 and name = 'Joseph'"),
     {
         'id': (3,)
     }),

    (test_table,
     ("select id from t "
      "where not (id = 3 and timestamp - 1 = 1597899422 and name = 'Joseph')"),
     {
         'id': (1, 2, 4)
     }),

    (test_table,
     "select id from t where id > 3",
     {
         'id': [4]
     }),

    (test_table,
     "select id from t where id >= 3",
     {
         'id': (3, 4)
     }),

    (test_table,
     "select id from t where id < 2",
     {
         'id': [1]
     }),

    (test_table,
     "select id from t where id <= 3",
     {
         'id': (1, 2, 3)
     }),

    (test_table,
     'select "id", "timestamp" from t where id < 2',
     {
         'id': [1],
         'timestamp': [1596899421]
     }),

    (test_table,
     'select count(*), sum(total), vendor_id from t group by vendor_id '
     'order by vendor_id',
     {
         'count_star': (2, 1, 1),
         'sum': (35.83, 143.15, 53.1),
         'vendor_id': (1, 2, 3)
     }),

    (test_groupby_table,
     'select id from t order by id limit 5',
     {
         'id': (1, 2, 3, 4, 5),
     }),

    (test_groupby_table,
     'select id from t limit 5 offset 2',
     {
         'id': (3, 4, 5, 6, 7),
     }),

    (test_groupby_table,
     'select id from t limit 10 offset 4',
     {
         'id': (5, 6, 7, 8),
     }),

    (test_groupby_table,
     'select id from t limit 1 offset 2',
     {
         'id': (3,),
     }),

    (test_groupby_table,
     ("select vendor_id, count(*) from t "
      "group by vendor_id having count(*) > 1 order by count(*)"),
     {
         'vendor_id': (2, 1),
         'count_star': (2, 5),
     }),

    (test_groupby_table,
     'select city_from, to_int(np.sin(lat) * 100000) % 11 as grp_exp, '
     ' count(*), min(tax) '
     'from t '
     'group by city_from, grp_exp '
     'having min(tax) > 1 '
     'order by grp_exp, city_from desc '
     'limit 2',
     {
         'city_from': ('San Francisco', 'Riva'),
         'grp_exp': (2, 5),
         'count_star': (1, 1),
         'min': (1.69, 1.59),
     }),

)


groupby_queries = (
    (test_groupby_table,
     'select vendor_id from t group by vendor_id order by vendor_id',
     {
         'vendor_id': (1, 2, 3)
     }),

    (test_groupby_table,
     ('select vendor_id, count(*), min(tax), max(tip), sum(total), '
      ' avg(total) from t group by vendor_id order by vendor_id'),
     {
         'vendor_id': (1, 2, 3),
         'count_star': (5, 2, 1),
         'min': (0.43, 2.0, 1.69),
         'max': (11, 5.34, 5.3),
         'sum': (105.06, 156.3, 53.1),
         'avg': (21.012, 78.15, 53.1),
     }),

    (test_groupby_table,
     ('select city_from, to_int(total) % 7 as mod, count(*) '
      'from t group by city_from, to_int(total) % 7 '
      'order by city_from, mod'),
     {
         'city_from': ('Berlin', 'Berlin', 'Munich', 'Munich',
                       'Riva', 'San Francisco'),
         'mod': (2, 5, 3, 6, 5, 4),
         'count_star': (2, 2, 1, 1, 1, 1),
     }),

    (test_groupby_table,
     ('select city_from, city_to, count(*) from t group by city_from, city_to '
      'order by city_from, count(*)'),
     {
         'city_from': ('Berlin', 'Berlin', 'Munich', 'Riva', 'San Francisco'),
         'city_to': ('Riva', 'Munich', 'Riva', 'Naples', 'Naples'),
         'count_star': (1, 3, 2, 1, 1),
     }),

    (test_groupby_table,
     'select city_from, to_int(np.sin(lat) * 100000) % 11 as grp_exp, '
     ' count(*), min(tax) '
     'from t '
     'group by city_from, grp_exp '
     'order by city_from, min(tax)',
     {
         'city_from': ('Berlin', 'Berlin', 'Munich', 'Riva', 'San Francisco'),
         'grp_exp': (6, 5, 8, 5, 2),
         'count_star': (2, 2, 2, 1, 1),
         'min': (0.43, 1.59, 2.0, 1.59, 1.69),
     }),

    (test_groupby_table,
     ('select city_from, count(*), count(timestamp % 2 < 1) from t '
      'group by city_from order by city_from'),
     {
         'city_from': ('Berlin', 'Munich', 'Riva', 'San Francisco'),
         'count_star': (4, 2, 1, 1),
         'count': (4, 2, 1, 1),
     }),

    (test_groupby_table,
     ('select city_from, sum(tax), sum((1-total)*(2+tax)*(1-tip)) '
      'from t group by city_from order by city_from'),
     {
         'city_from': ('Berlin', 'Munich', 'Riva', 'San Francisco'),
         'sum': (4.04, 4.0, 1.59, 1.69),
         'sum_1': (2324.23506, 2110.048, 1163.16, 826.6706),
     }),

    (test_groupby_table,
     ('select city_from, count(*) from t '
      'where tax > 1 group by city_from, city_to order by city_from, city_to'),
     {
         'city_from': ('Berlin', 'Berlin', 'Munich', 'Riva', 'San Francisco'),
         'count_star': (1, 1, 2, 1, 1),
     }),

    (test_groupby_table,
     'select city_from, sum(tax), sum((1-total)*(2+tax)*(1-tip)) from t '
     'group by city_from having sum((1-total)*(2+tax)*(1-tip)) > 1200 '
     'order by city_from',
     {
         'city_from': ('Berlin', 'Munich'),
         'sum': (4.04, 4.0),
         'sum_1': (2324.23506, 2110.048),
     }),

    (test_groupby_table,
     'select city_from, sum(tax), sum((1-total)*(2+tax)*(1-tip)) as agg_col '
     'from t group by city_from having agg_col > 827 '
     'order by city_from',
     {
         'city_from': ('Berlin', 'Munich', 'Riva'),
         'sum': (4.04, 4.0, 1.59),
         'agg_col': (2324.23506, 2110.048, 1163.16),
     }),

    (test_groupby_table,
     'select city_from, sum(tax), sum((1-total)*(2+tax)*(1-tip)) as agg_col '
     'from t group by city_from having agg_col > 827 and sum(tax) > 1.6 '
     'order by city_from',
     {
         'city_from': ('Berlin', 'Munich'),
         'sum': (4.04, 4.0),
         'agg_col': (2324.23506, 2110.048),
     }),

    (test_groupby_table,
     "select city_from, sum(tax), sum((1-total)*(2+tax)*(1-tip)) from t "
     "group by city_from "
     "having sum((1-total)*(2+tax)*(1-tip)) > 827 "
     " and sum(tax) > 1.6 and city_from='Munich'",
     {
         'city_from': ('Munich',),
         'sum': (4.0,),
         'sum_1': (2110.048,),
     }),

    (test_groupby_table,
     "select city_from, count(*) from t "
     "group by city_from having city_from='Berlin'",
     {
         'city_from': ('Berlin',),
         'count_star': (4,),
     }),

    (test_groupby_table,
     'select city_from, to_int(np.sin(lat) * 100000) % 11 as grp_exp, '
     ' count(*), min(tax) '
     'from t '
     'group by city_from, grp_exp '
     'having grp_exp between 4 and 7 '
     'order by city_from, min(tax)',
     {
         'city_from': ('Berlin', 'Berlin', 'Riva'),
         'grp_exp': (6, 5, 5),
         'count_star': (2, 2, 1),
         'min': (0.43, 1.59, 1.59),
     }),

    (test_groupby_table,
     "select id, city_from, total, timestamp from t "
     "having city_from='Berlin' and total < 3.0 and timestamp > 1602649608",
     {
         'id': (8,),
         'city_from': ('Berlin', ),
         'total': (2.43, ),
         'timestamp': (1602736007, ),
     }),

    (test_groupby_table,
     "select city_from from t group by city_from, city_to "
     "having city_to='Naples' "
     "order by city_from",
     {
         'city_from': ('Riva', 'San Francisco'),
     }),

    (test_groupby_table,
     "select vendor_id, count(*) from t "
     "group by vendor_id having vendor_id=1",
     {
         'vendor_id': (1, ),
         'count_star': (5, ),
     }),

    (test_groupby_table,
     "select vendor_id, count(*) from t "
     "group by vendor_id having vendor_id < 3 "
     "order by vendor_id",
     {
         'vendor_id': (1, 2),
         'count_star': (5, 2),
     }),

    (test_groupby_table,
     "select vendor_id, count(*) from t "
     "group by vendor_id having count(*) = 5",
     {
         'vendor_id': (1, ),
         'count_star': (5,),
     }),

    (test_groupby_table,
     "select vendor_id, count(*) from t "
     "group by vendor_id having count(*) > 1 "
     "order by vendor_id",
     {
         'vendor_id': (1, 2),
         'count_star': (5, 2),
     }),

    (test_groupby_table,
     "select vendor_id, sum(tax+tip) from t "
     "group by vendor_id having sum(tax+tip) * 2 > 5+9 "
     "order by vendor_id",
     {
         'vendor_id': (1, 2),
         'sum': (40.03, 13.68),
     }),

    (test_groupby_table,
     "SELECT sum(total) from t HAVING sum(total) > 1",
     {
         'sum': (314.46,),
     }),

    (test_groupby_table,
     """SELECT 
            city_from, 
            count(*) as cnt_all, 
            count(total) as cnt_total, 
            count(name) as cnt_name, 
            count(date) as cnt_date_str, 
            count(vendor_id) as cnt_bool, 
            count(datetime(date)) as cnt_datetime, 
            count(from_timestamp(timestamp)) as cnt_timestamp,
            min(total) as min_total,
            max(total) as max_total,
            avg(total) as avg_total, 
            sum(total) as sum_total 
        from t 
        group by city_from 
        order by city_from
     """,  # noqa: W291
     {
         'city_from': ('Berlin', 'Munich', 'Riva', 'San Francisco'),
         'cnt_all': (4, 2, 1, 1),
         'cnt_total': (4, 2, 1, 1),
         'cnt_name': (4, 2, 1, 1),
         'cnt_date_str': (4, 2, 1, 1),
         'cnt_bool': (4, 2, 1, 1),
         'cnt_datetime': (4, 2, 1, 1),
         'cnt_timestamp': (4, 2, 1, 1),
         'min_total': (2.43, 13.15, 33.4, 53.1),
         'max_total': (33.40, 143.15, 33.4, 53.1),
         'avg_total': (17.915, 78.15, 33.4, 53.1),
         'sum_total': (71.66, 156.2999, 33.4, 53.1),
     }),

    (test_groupby_table,
     "select vendor_id, sum(tax+tip) from t "
     "group by vendor_id having sum(tax+tip) * 2 > 5+9 "
     "order by vendor_id",
     {
         'vendor_id': (1, 2),
         'sum': (40.03, 13.68),
     }),

)


orderby_queries = (
    (test_groupby_table,
     'select * from t order by total',
     rows_to_columns_dict(
         [
             (1, 1602127614, '2020-10-08T03:26:54', 1, 'Berlin',
              'Munich', 52.51, 13.66, 'Joe', 0.43, 1, 2.43),
             (8, 1602736007, '2020-10-15T04:26:47', 1, 'Berlin',
              'Munich', 52.51, 13.66, 'Joe', 0.43, 0.4, 2.43),
             (6, 1602563209, '2020-10-13T04:26:49', 2, 'Munich',
              'Riva', 48.51, 12.3, 'Jonas', 2.0, 5.34, 13.15),
             (3, 1602304012, '2020-10-10T04:26:52', 1, 'Riva',
              'Naples', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (5, 1602476810, '2020-10-12T04:26:50', 1, 'Berlin',
              'Riva', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (7, 1602649608, '2020-10-14T04:26:48', 1, 'Berlin',
              'Munich', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (4, 1602390411, '2020-10-11T04:26:51', 3, 'San Francisco',
              'Naples', 42.89, 15.89, 'Joseph', 1.69, 5.3, 53.1),
             (2, 1602217613, '2020-10-09T04:26:53', 2, 'Munich',
              'Riva', 48.51, 12.3, 'Jonas', 2.0, 4.34, 143.15),
         ],
         groupby_column_names
     )),

    (test_groupby_table,
     'select * from t order by total, tip',
     rows_to_columns_dict(
         [
             (8, 1602736007, '2020-10-15T04:26:47', 1, 'Berlin',
              'Munich', 52.51, 13.66, 'Joe', 0.43, 0.4, 2.43),
             (1, 1602127614, '2020-10-08T03:26:54', 1, 'Berlin',
              'Munich', 52.51, 13.66, 'Joe', 0.43, 1, 2.43),
             (6, 1602563209, '2020-10-13T04:26:49', 2, 'Munich',
              'Riva', 48.51, 12.3, 'Jonas', 2.0, 5.34, 13.15),
             (3, 1602304012, '2020-10-10T04:26:52', 1, 'Riva',
              'Naples', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (5, 1602476810, '2020-10-12T04:26:50', 1, 'Berlin',
              'Riva', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (7, 1602649608, '2020-10-14T04:26:48', 1, 'Berlin',
              'Munich', 44.89, 14.23, 'Joseph', 1.59, 11, 33.40),
             (4, 1602390411, '2020-10-11T04:26:51', 3, 'San Francisco',
              'Naples', 42.89, 15.89, 'Joseph', 1.69, 5.3, 53.1),
             (2, 1602217613, '2020-10-09T04:26:53', 2, 'Munich',
              'Riva', 48.51, 12.3, 'Jonas', 2.0, 4.34, 143.15),
         ],
         groupby_column_names
     )),

    (test_groupby_table,
     "select total from t order by total",
     {
         'total': (2.43, 2.43, 13.15, 33.40, 33.40, 33.40, 53.1, 143.15),
     }),

    (test_groupby_table,
     "select total from t order by total desc",
     {
         'total': (143.15, 53.1, 33.40, 33.40, 33.40, 13.15, 2.43, 2.43),
     }),

    (test_groupby_table,
     "select city_from, total from t order by city_from desc, total asc",
     {
         'city_from': ('San Francisco', 'Riva', 'Munich', 'Munich',
                       'Berlin', 'Berlin', 'Berlin', 'Berlin'),
         'total': (53.1, 33.40, 13.15, 143.15, 2.43, 2.43, 33.40, 33.40),
     }),

    (test_groupby_table,
     "select city_from, total from t order by city_from desc, total desc",
     {
         'city_from': ('San Francisco', 'Riva', 'Munich', 'Munich',
                       'Berlin', 'Berlin', 'Berlin', 'Berlin'),
         'total': (53.1, 33.40, 143.15, 13.15, 33.40, 33.40, 2.43, 2.43),
     }),

    (test_groupby_table,
     "select total + tax + tip from t order by total + tax + tip",
     {
         'col_0': (3.26, 3.86, 20.49, 45.99, 45.99, 45.99, 60.09, 149.49),
     }),

    (test_groupby_table,
     "select total + tax + tip as total_sum from t order by total_sum",
     {
         'total_sum': (3.26, 3.86, 20.49, 45.99, 45.99, 45.99, 60.09, 149.49),
     }),

    (test_groupby_table,
     "select id from t order by np.log(total) * np.exp(tip)",
     {
         'id': (8, 1, 2, 6, 4, 3, 5, 7),
     }),

    (test_groupby_table,
     "select id from t order by vendor_id * 5 desc, np.exp(tip) asc",
     {
         'id': (4, 2, 6, 8, 1, 3, 5, 7),
     }),

    (test_groupby_table,
     """SELECT 
            city_from, 
            sum(total), 
            np.square(sum(total)), 
            np.log(sum(total)*100), 
            avg(tax*3)-10 
            FROM t 
            GROUP BY city_from
            ORDER BY city_from""",
     {
         'city_from': ('Berlin', 'Munich', 'Riva', 'San Francisco'),
         'sum': (71.66, 156.3, 33.4, 53.1),
         'np.square': (5135.1556, 24429.69, 1115.56, 2819.61),
         'np.log': (8.877103, 9.656947, 8.113726, 8.577347),
         'col_0': (-6.97, -4.0, -5.23, -4.93),
     }),

)

built_in_functions = (

    (test_groupby_table,
     "select to_bool(5) from t",
     {
         'to_bool': (True,),
     }),

    (test_groupby_table,
     "select to_bool(0) from t",
     {
         'to_bool': (False,),
     }),

    (test_groupby_table,
     "select to_float('3.7') from t",
     {
         'to_float': (3.7,),
     }),

    (test_groupby_table,
     "select to_float(1099511627776.757) from t",
     {
         'to_float': (1099511627776.757,),
     }),

    (test_groupby_table,
     "select to_float(3) from t",
     {
         'to_float': (3.0,),
     }),

    (test_groupby_table,
     "select to_int(3.5) from t",
     {
         'to_int': (3,),
     }),

    (test_groupby_table,
     "select to_int('7') from t",
     {
         'to_int': (7,),
     }),

    (test_groupby_table,
     "select to_int('1', '2', '3') from t",
     {
         'to_int': (1, 2, 3),
     }),

    (test_groupby_table,
     "select to_int(1099511627776.375) from t",
     {
         'to_int': (1099511627776,),
     }),

    (test_groupby_table,
     "select to_str(1099511627776.375) from t",
     {
         'to_str': ('1099511627776.375',),
     }),

    (test_groupby_table,
     "select to_str(17) from t",
     {
         'to_str': ('17',),
     }),

    (test_groupby_table,
     "select to_str('st') from t",
     {
         'to_str': ('st',),
     }),

    (test_groupby_table,
     "select to_bool(total) from t",
     {
         'to_bool': (True, True, True, True, True, True, True, True),
     }),

    (test_groupby_table,
     "select to_bool(to_int(tax)) from t",
     {
         'to_bool': (False, True, True, True, True, True, True, False),
     }),

    (test_groupby_table,
     "select to_float(id) from t",
     {
         'to_float': (1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0),
     }),

    (test_groupby_table,
     "select to_int(total) from t",
     {
         'to_int': (2, 143, 33, 53, 33, 13, 33, 2),
     }),

    (test_groupby_table,
     "select to_str(total) from t",
     {
         'to_str': ('2.43', '143.15', '33.4', '53.1', '33.4', '13.15',
                 '33.4', '2.43'),
     }),

    (test_groupby_table,
     "select concat('random string', '>', 7) res",
     {
         'res': ('random string>7',),
     }),

    (test_groupby_table,
     "select upper('random string') res",
     {
         'res': ('RANDOM STRING',),
     }),

    (test_groupby_table,
     "select lower('RANDOM STRING') res",
     {
         'res': ('random string',),
     }),

    (test_groupby_table,
     "select concat(upper('random s'), 13, lower(upper('LUCKY'))) res",
     {
         'res': ('RANDOM S13lucky',),
     }),

)


math_functions = (

    (test_groupby_table,
     "select abs(-5)",
     {
         'abs': (5,),
     }),

    (test_groupby_table,
     "select sqrt(4)",
     {
         'sqrt': (2,),
     }),

    (test_groupby_table,
     "select cos(0)",
     {
         'cos': (1,),
     }),

    (test_groupby_table,
     "select sin(pi() / 2)",
     {
         'sin': (1,),
     }),

    (test_groupby_table,
     "select tan(pi() / 4)",
     {
         'tan': (1,),
     }),

    (test_groupby_table,
     "select power(2, 3)",
     {
         'power': (8,),
     }),

    (test_groupby_table,
     "select log(power(e(), 3))",
     {
         'log': (3,),
     }),

    (test_groupby_table,
     "select log2(32)",
     {
         'log2': (5,),
     }),

    (test_groupby_table,
     "select log10(100)",
     {
         'log10': (2,),
     }),

    (test_groupby_table,
     "select abs(-id) from t",
     {
         'abs': (1, 2, 3, 4, 5, 6, 7, 8),
     }),

    (test_groupby_table,
     "select sqrt(id) from t",
     {
         'sqrt': (1., 1.41421356, 1.73205081, 2.,
                  2.23606798, 2.44948974, 2.64575131, 2.82842712),
     }),

    (test_groupby_table,
     "select cos(total) from t",
     {
         'cos': (-0.75732277,  0.2060477, -0.40161271, -0.95322176,
                 -0.40161271, 0.83446815, -0.40161271, -0.75732277),
     }),

    (test_groupby_table,
     "select sin(total) from t",
     {
         'sin': (0.65304075, -0.97854195,  0.9158096,  0.30227187,
                 0.9158096, 0.55105617,  0.9158096,  0.65304075),
     }),

    (test_groupby_table,
     "select tan(total) from t",
     {
         'tan': (-0.86230175, -4.74910396, -2.28033021, -0.3171055,
                 -2.28033021, 0.66036812, -2.28033021, -0.86230175),
     }),

    (test_groupby_table,
     "select power(id, 2) from t",
     {
         'power': (1, 4, 9, 16, 25, 36, 49, 64),
     }),

    (test_groupby_table,
     "select log(timestamp) from t",
     {
         'log': (21.19459834, 21.19465451, 21.19470844, 21.19476236,
                 21.19481628, 21.19487019, 21.1949241, 21.19497801),
     }),

    (test_groupby_table,
     "select log2(timestamp) from t",
     {
         'log2': (30.57734192, 30.57742296, 30.57750076, 30.57757855,
                  30.57765633, 30.57773412, 30.57781189, 30.57788967),
     }),

    (test_groupby_table,
     "select log10(timestamp) from t",
     {
         'log10': (9.20469711, 9.2047215, 9.20474492, 9.20476834,
                   9.20479175, 9.20481517, 9.20483858, 9.20486199),
     }),

)

datetime_queries_scalar = (

    (test_groupby_table,
     "select datetime('2020-10-06')",
     {
         'datetime': (np.datetime64(date(2020, 10, 6), 'D'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10')",
     {
         'datetime': (np.datetime64(date(2020, 10, 1), 'D'),),
     }),

    (test_groupby_table,
     "select datetime('2020')",
     {
         'datetime': (np.datetime64(date(2020, 1, 1), 'D'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07 19:30:27') from t",
     {
         'datetime': (np.datetime64(datetime(2020, 10, 7, 19, 30, 27), 's'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07T19:30:27')",
     {
         'datetime': (np.datetime64(datetime(2020, 10, 7, 19, 30, 27), 's'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07 19:30')",
     {
         'datetime': (np.datetime64(datetime(2020, 10, 7, 19, 30), 's'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07 19')",
     {
         'datetime': (np.datetime64(datetime(2020, 10, 7, 19), 's'),),
     }),

    (test_groupby_table,
     "select from_timestamp(1602841523)",
     {
         'from_timestamp': (
             np.datetime64(datetime(2020, 10, 16, 9, 45, 23), 's'),
         ),
     }),

    (test_groupby_table,
     "select from_timestamp(1602841523, 's')",
     {
         'from_timestamp': (
             np.datetime64(datetime(2020, 10, 16, 9, 45, 23), 's'),
         ),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07T19:30:27', 'D')",
     {
         'datetime': (np.datetime64(date(2020, 10, 7), 'D'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07T19:30:27', 's')",
     {
         'datetime': (np.datetime64(datetime(2020, 10, 7, 19, 30, 27), 's'),),
     }),

    (test_groupby_table,
     "select datetime('2020-10-07T19:30:27') - timedelta(5, 'D') "
     " + timedelta(3, 's') as dtime",
     {
         'dtime': (np.datetime64(datetime(2020, 10, 2, 19, 30, 30), 's'),),
     }),

    (test_groupby_table,
     "select date('2020-10-06')",
     {
         'date': (np.datetime64(date(2020, 10, 6), 'D'),),
     }),

    (test_groupby_table,
     "select date('2020-10')",
     {
         'date': (np.datetime64(date(2020, 10, 1), 'D'),),
     }),

    (test_groupby_table,
     "select date('2020')",
     {
         'date': (np.datetime64(date(2020, 1, 1), 'D'),),
     }),

    (test_groupby_table,
     "select date('2020-10-07 19:30:27')",
     {
         'date': (np.datetime64(date(2020, 10, 7), 'D'),),
     }),

    (test_groupby_table,
     "select date('2020-10-07T19:30:27')",
     {
         'date': (np.datetime64(date(2020, 10, 7), 'D'),),
     }),

)


datetime_queries_column = (

    (test_groupby_table,
     "select datetime(date, 'D') from t",
     {
         'datetime': (
             np.datetime64(date(2020, 10, 8), 'D'),
             np.datetime64(date(2020, 10, 9), 'D'),
             np.datetime64(date(2020, 10, 10), 'D'),
             np.datetime64(date(2020, 10, 11), 'D'),
             np.datetime64(date(2020, 10, 12), 'D'),
             np.datetime64(date(2020, 10, 13), 'D'),
             np.datetime64(date(2020, 10, 14), 'D'),
             np.datetime64(date(2020, 10, 15), 'D'),
         ),
     }),

    (test_groupby_table,
     "select datetime(date) from t",
     {
         'datetime': (
             np.datetime64(datetime(2020, 10, 8, 3, 26, 54), 's'),
             np.datetime64(datetime(2020, 10, 9, 4, 26, 53), 's'),
             np.datetime64(datetime(2020, 10, 10, 4, 26, 52), 's'),
             np.datetime64(datetime(2020, 10, 11, 4, 26, 51), 's'),
             np.datetime64(datetime(2020, 10, 12, 4, 26, 50), 's'),
             np.datetime64(datetime(2020, 10, 13, 4, 26, 49), 's'),
             np.datetime64(datetime(2020, 10, 14, 4, 26, 48), 's'),
             np.datetime64(datetime(2020, 10, 15, 4, 26, 47), 's'),
         ),
     }),

    (test_groupby_table,
     "select from_timestamp(timestamp) from t",
     {
         'from_timestamp': (
             np.datetime64(datetime(2020, 10, 8, 3, 26, 54), 's'),
             np.datetime64(datetime(2020, 10, 9, 4, 26, 53), 's'),
             np.datetime64(datetime(2020, 10, 10, 4, 26, 52), 's'),
             np.datetime64(datetime(2020, 10, 11, 4, 26, 51), 's'),
             np.datetime64(datetime(2020, 10, 12, 4, 26, 50), 's'),
             np.datetime64(datetime(2020, 10, 13, 4, 26, 49), 's'),
             np.datetime64(datetime(2020, 10, 14, 4, 26, 48), 's'),
             np.datetime64(datetime(2020, 10, 15, 4, 26, 47), 's'),
         ),
     }),

    (test_groupby_table,
     "select (datetime(date) - timedelta(35, 'D') - timedelta(7, 'h') "
     " - timedelta(13, 'm') - timedelta(3, 's')) as tdelta from t",
     {
         'tdelta': (
             np.datetime64(datetime(2020, 9, 2, 20, 13, 51), 's'),
             np.datetime64(datetime(2020, 9, 3, 21, 13, 50), 's'),
             np.datetime64(datetime(2020, 9, 4, 21, 13, 49), 's'),
             np.datetime64(datetime(2020, 9, 5, 21, 13, 48), 's'),
             np.datetime64(datetime(2020, 9, 6, 21, 13, 47), 's'),
             np.datetime64(datetime(2020, 9, 7, 21, 13, 46), 's'),
             np.datetime64(datetime(2020, 9, 8, 21, 13, 45), 's'),
             np.datetime64(datetime(2020, 9, 9, 21, 13, 44), 's'),
         ),
     }),

    (test_groupby_table,
     "select id, is_busday(date(date)) from t order by id",
     {
         'id': (1, 2, 3, 4, 5, 6, 7, 8),
         'is_busday': (True, True, False, False, True, True, True, True),
     }),

)


null_data = (
    (test_table_null,
     "select id from t where name is null order by id",
     {
         'id': (2, 5)
     }),

    (test_table_null,
     "select id from t where name is not null order by id",
     {
         'id': (1, 3, 4, 6, 7, 8)
     }),

    (test_table_null,
     "select id from t where total is null order by id",
     {
         'id': (1, 6)
     }),

    (test_table_null,
     "select id from t where total is not null order by id",
     {
         'id': (2, 3, 4, 5, 7, 8)
     }),

    (test_table_null,
     "select id from t where is_vendor is null order by id",
     {
         'id': (4, 6, 7, 8)
     }),

    (test_table_null,
     "select id from t where is_vendor is not null order by id",
     {
         'id': (1, 2, 3, 5)
     }),

    (test_table_null,
     "select id from t where datetime(date) is null order by id",
     {
         'id': (1, 7)
     }),

    (test_table_null,
     "select id from t where datetime(date) is not null order by id",
     {
         'id': (2, 3, 4, 5, 6, 8)
     }),

    (test_table_null,
     "select id from t where from_timestamp(timestamp) is null order by id",
     {
         'id': (5, 7)
     }),

    (test_table_null,
     ("select id from t "
      "where from_timestamp(timestamp) is not null order by id"),
     {
         'id': (1, 2, 3, 4, 6, 8)
     }),

    (test_table_null,
     ("select id from t "
      "where is_vendor is null and city_from = 'Berlin' order by id"),
     {
         'id': (7, 8)
     }),

    (test_table_null,
     "select id from t order by total, id",
     {
         'id': (3, 7, 4, 2, 5, 8, 1, 6)
     }),

    (test_table_null,
     "select id from t order by datetime(date)",
     {
         'id': (2, 3, 4, 5, 6, 8, 1, 7)
     }),

    (test_table_null,
     "select id from t order by from_timestamp(timestamp)",
     {
         'id': (1, 2, 3, 4, 6, 8, 5, 7)
     }),

    (test_table_null,
     """SELECT 
            city_from, 
            count(*) as cnt_all, 
            count(total) as cnt_total, 
            count(name) as cnt_name, 
            count(date) as cnt_date_str, 
            count(is_vendor) as cnt_bool, 
            count(datetime(date)) as cnt_datetime, 
            count(from_timestamp(timestamp)) as cnt_timestamp,
            min(total) as min_total,
            max(total) as max_total,
            avg(total) as avg_total, 
            sum(total) as sum_total 
        from t group by city_from  
        order by city_from
     """,  # noqa: W291
     {
         'city_from': ('Berlin', 'Munich', 'San Francisco', None),
         'cnt_all': (3, 2, 1, 2),
         'cnt_total': (3, 1, 1, 1),
         'cnt_name': (2, 1, 1, 2),
         'cnt_date_str': (2, 2, 1, 1),
         'cnt_bool': (1, 1, 0, 2),
         'cnt_datetime': (2, 2, 1, 1),
         'cnt_timestamp': (1, 2, 1, 2),
         'min_total': (np.nan, 143.15, 53.1, 33.4),
         'max_total': (np.nan, 143.15, 53.1, 33.4),
         'avg_total': (np.nan, 143.15, 53.1, 33.4),
         'sum_total': (np.nan, 143.15, 53.1, 33.4),
     }),

    (test_table_null,
     "select city_from || '-' || city_to || name as res from t order by id",
     {
         'res': ('None-MunichJoe',
                 'Munich-RivaNone',
                 'None-NaplesJoseph',
                 'San Francisco-NaplesJoseph',
                 'Berlin-RivaNone',
                 'Munich-RivaJonas',
                 'Berlin-MunichJoseph',
                 'Berlin-MunichJoe')
     }),

    (test_table_null,
     "select concat(city_from, 7, city_to, name) as res from t order by id",
     {
         'res': ('None7MunichJoe',
                 'Munich7RivaNone',
                 'None7NaplesJoseph',
                 'San Francisco7NaplesJoseph',
                 'Berlin7RivaNone',
                 'Munich7RivaJonas',
                 'Berlin7MunichJoseph',
                 'Berlin7MunichJoe')
     }),

    (test_table_null,
     "select upper(city_from) res from t order by id",
     {
         'res': (None, 'MUNICH', None, 'SAN FRANCISCO',
                 'BERLIN', 'MUNICH', 'BERLIN', 'BERLIN'),
     }),

    (test_table_null,
     "select lower(city_from) res from t order by id",
     {
         'res': (None, 'munich', None, 'san francisco',
                 'berlin', 'munich', 'berlin', 'berlin'),
     }),

    (test_table_null,
     "select id from t order by city_from, city_to",
     {
         'id': (7, 8, 5, 2, 6, 4, 1, 3)
     }),

    (test_table_null,
     "select id from t order by city_to, city_from, name",
     {
         'id': (8, 7, 1, 4, 3, 5, 6, 2)
     }),

    (test_table_null,
     "select id from t order by to_float(is_vendor)",
     {
         'id': (3, 1, 2, 5, 4, 6, 7, 8)
     }),

    (test_table_null,
     "select id from t order by to_float(is_vendor) desc, lng desc",
     {
         'id': (5, 1, 2, 3, 4, 7, 8, 6)
     }),

    (test_table_null,
     "select id from t order by name, to_float(is_vendor), lng",
     {
         'id': (1, 8, 6, 3, 7, 4, 2, 5)
     }),

    (test_table_null,
     "select id from t order by name desc, to_float(is_vendor) desc, lng desc",
     {
         'id': (3, 4, 7, 6, 1, 8, 5, 2)
     }),

    (test_table_null,
     "select id from t order by name desc, to_float(is_vendor) desc, np.log(lng) desc",
     {
         'id': (3, 4, 7, 6, 1, 8, 5, 2)
     }),

)


class TestQueryResults:

    @pytest.mark.parametrize("source_tbl, query, expected_result",
                             (queries
                              + groupby_queries
                              + orderby_queries
                              + datetime_queries_scalar
                              + datetime_queries_column
                              + built_in_functions
                              + math_functions
                              + null_data
                              )
                             )
    def test_queries(self, source_tbl, query, expected_result):
        actual_tbl = source_tbl.sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize(
        "source_tbl, udf_name, udf, is_python_udf, query, expected_result",
        (
                pytest.param(test_groupby_table,
                             'cube',
                             lambda x: x ** 3,
                             True,
                             "SELECT cube(id) from t ORDER BY cube(id) DESC",
                             {
                                 'cube': (512, 343, 216, 125, 64, 27, 8, 1),
                             }),
                pytest.param(test_groupby_table,
                             'cube_np',
                             lambda x: np.power(x, 3),
                             False,
                             ("SELECT cube_np(id) from t "
                              "ORDER BY cube_np(id) DESC"),
                             {
                                 'cube_np': (512, 343, 216, 125, 64, 27, 8, 1),
                             }),
        )
    )
    def test_udfs(self,
                  source_tbl,
                  udf_name,
                  udf,
                  is_python_udf,
                  query,
                  expected_result):
        if is_python_udf:
            register_python(udf_name, udf)
        else:
            register_numpy(udf_name, udf)
        actual_tbl = source_tbl.sql(query)
        _assert_tables_equal(actual_tbl, expected_result)

    @pytest.mark.parametrize(
        "source_tbl, udf_name, udf, is_python_udf, query, expected_result",
        (
                pytest.param(test_groupby_table,
                             'corr',
                             lambda x, y: np.corrcoef(x, y)[0, 1],
                             False,
                             ("SELECT city_to, corr(tip, tax) from t "
                              "GROUP BY city_to "
                              "ORDER BY corr(tip, tax) DESC"),
                             {
                                 'city_to': ('Munich', 'Riva', 'Naples'),
                                 'corr': (0.998730, -0.990262, -1.,),
                             }),
        )
    )
    def test_udfs_raising(self,
                  source_tbl,
                  udf_name,
                  udf,
                  is_python_udf,
                  query,
                  expected_result):
        if is_python_udf:
            register_python(udf_name, udf)
        else:
            register_numpy(udf_name, udf)
        with pytest.raises(Exception):
            actual_tbl = source_tbl.sql(query)

    @pytest.mark.parametrize(
        "source_tbl, is_python_udf",
        (
                pytest.param(
                    test_groupby_table,
                    True),
                pytest.param(
                    test_groupby_table,
                    False),
        )
    )
    def test_update_udf(self, source_tbl, is_python_udf):
        def f_square(x): return x**2
        def f_cube(x): return x**3

        if is_python_udf:
            register_python('udf_upd', f_square)
            register_python('udf_upd', f_cube)
        else:
            register_python('udf_upd', f_square)
            register_python('udf_upd', f_cube)
        actual_tbl = source_tbl.sql(
            "select udf_upd(id) as pow from t order by pow"
        )
        expected = {
            'pow': (1, 8, 27, 64, 125, 216, 343, 512),
        }
        _assert_tables_equal(actual_tbl, expected)

    @pytest.mark.parametrize(
        "source_tbl, query",
        (
                pytest.param(
                    test_table,
                    "select city_from, count(total>100) from t", ),
        )
    )
    def test_select_exprs_different_sizes(self, source_tbl, query):
        with pytest.raises(Exception):
            source_tbl.sql(query)

    @pytest.mark.parametrize(
        "source_tbl, query",
        (
                pytest.param(
                    test_table,
                    "select bla from t",
                ),
                pytest.param(
                    test_table,
                    "select udf_missing(vendor_id) from t",
                ),
                pytest.param(
                    test_table,
                    "select udf_missing(vendor_id) from t group by vendor_id",
                ),
        )
    )
    def test_column_not_found(self, source_tbl, query):
        with pytest.raises(Exception):
            source_tbl.sql(query)

    @pytest.mark.parametrize(
        "source_tbl, query",
        (
                pytest.param(
                    test_groupby_table,
                    "select city_to, city_from, count(*) from t "
                    "group by city_from", ),
                pytest.param(
                    test_groupby_table,
                    "select city_from, total > 20, count(*) from t "
                    "group by city_from", ),
                pytest.param(
                    test_groupby_table,
                    "select np.sin(lat), city_from from t "
                    "group by city_from", ),
                pytest.param(
                    test_groupby_table,
                    "select city_from, count(*) from t group by total > 20", ),
                pytest.param(
                    test_groupby_table,
                    "select total > 20, 4 from t group by total > 20", ),
                pytest.param(
                    test_groupby_table,
                    "select vendor_id from t "
                    "group by vendor_id having count(*) = 5", ),
                pytest.param(
                    test_groupby_table,
                    "select vendor_id from t "
                    "group by vendor_id having count(*) > 1", ),
        )
    )
    def test_non_groupby_columns_in_select(self, source_tbl, query):
        with pytest.raises(Exception):
            source_tbl.sql(query)

    @pytest.mark.parametrize("source_tbl, function, unit", (
            (test_table, 'date', 'D'),
            (test_table, 'datetime', 's'),
            (test_table, 'now', 's'),
    ))
    def test_datetime_now(self, source_tbl, function, unit):
        test_execution_time_tolerance = np.timedelta64(5, 's')  # in seconds
        actual_tbl = source_tbl.sql(f"select {function}('now') from t")

        dt_col = (actual_tbl._arrow_table.combine_chunks()
                  .get_np_column_by_name(function))
        assert len(dt_col) == 1
        actual_now = dt_col[0]
        expected_now = np.datetime64('now', unit)
        assert (expected_now - actual_now) < test_execution_time_tolerance

    @pytest.mark.parametrize("source_tbl", (test_table,))
    def test_head(self, source_tbl):
        head_df = source_tbl.head(2)
        assert head_df.shape == (2, 11)
