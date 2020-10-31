***************
Getting started
***************

Install
=======

``pip install vinum``


Examples
========

Query python dict
-----------------

Create a a Table from a python dict and return result of the query
as a Pandas DataFrame.

    >>> import vinum as vn
    >>> data = {'value': [300.1, 2.8, 880], 'mode': ['air', 'bus', 'air']}
    >>> tbl = vn.Table.from_pydict(data)
    >>> tbl.sql_pd("SELECT value, np.log(value) FROM t WHERE mode='air'")
       value    np.log
    0  300.1  5.704116
    1  880.0  6.779922


Query pandas dataframe
----------------------

    >>> import pandas as pd
    >>> import vinum as vn
    >>> data = {'col1': [1, 2, 3], 'col2': [7, 13, 17]}
    >>> pdf = pd.DataFrame(data=data)
    >>> tbl = vn.Table.from_pandas(pdf)
    >>> tbl.sql_pd('SELECT * FROM t WHERE col2 > 10 ORDER BY col1 DESC')
       col1  col2
    0     3    17
    1     2    13


Query csv
---------
    >>> import vinum as vn
    >>> tbl = vn.read_csv('test.csv')
    >>> res_tbl = tbl.sql('SELECT * FROM t WHERE fare > 5 LIMIT 3')
    >>> res_tbl.to_pandas()
       id                            ts        lat        lng  fare
    0   1   2010-01-05 16:52:16.0000002  40.711303 -74.016048  16.9
    1   2  2011-08-18 00:35:00.00000049  40.761270 -73.982738   5.7
    2   3   2012-04-21 04:30:42.0000001  40.733143 -73.987130   7.7


Compute Euclidean distance with numpy functions
-----------------------------------------------

Use any numpy functions via the 'np.*' namespace.

    >>> import vinum as vn
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT *, np.sqrt(np.square(x) + np.square(y)) dist '
    ...            'FROM t ORDER BY dist DESC')
       x   y       dist
    0  3  17  17.262677
    1  2  13  13.152946
    2  1   7   7.071068


Compute Euclidean distance with vectorized UDF
----------------------------------------------

Register UDF performing vectorized operations on Numpy arrays.

    >>> import vinum as vn
    >>> vn.register_numpy('distance',
    ...                   lambda x, y: np.sqrt(np.square(x) + np.square(y)))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT *, distance(x, y) AS dist '
    ...            'FROM t ORDER BY dist DESC')
       x   y       dist
    0  3  17  17.262677
    1  2  13  13.152946
    2  1   7   7.071068


Compute Euclidean distance with python UDF
------------------------------------------

Register Python lambda function as UDF.

    >>> import math
    >>> import vinum as vn
    >>> vn.register_python('distance', lambda x, y: math.sqrt(x**2 + y**2))
    >>> tbl = vn.Table.from_pydict({'x': [1, 2, 3], 'y': [7, 13, 17]})
    >>> tbl.sql_pd('SELECT x, y, distance(x, y) AS dist FROM t')
       x   y       dist
    0  1   7   7.071068
    1  2  13  13.152946
    2  3  17  17.262677


Group by z-score
----------------

    >>> import numpy as np
    >>> import vinum as vn
    >>> def z_score(x: np.ndarray):
    ...     """Compute Standard Score"""
    ...     mean = np.mean(x)
    ...     std = np.std(x)
    ...     return (x - mean) / std
    ...
    >>> vn.register_numpy('score', z_score)
    >>> tbl = vn.read_csv('test.csv')
    >>> tbl.sql_pd('select int(score(fare)) AS bucket, avg(fare), count(*) '
    ...            'FROM t GROUP BY bucket ORDER BY bucket')
       bucket        avg  count
    0       0   8.111630     92
    1       1  19.380000      3
    2       2  27.433333      3
    3       3  34.670000      1
    4       6  58.000000      1
