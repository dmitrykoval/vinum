************
Installation
************

With pip
========
``pip install vinum``


Usage example
=============

    >>> import vinum as vn
    >>> tbl = vn.read_csv('test.csv')
    >>> res_tbl = tbl.sql('SELECT * FROM t WHERE fare > 5 LIMIT 3')
    >>> res_tbl.to_pandas()
       id                            ts        lat        lng  fare
    0   1   2010-01-05 16:52:16.0000002  40.711303 -74.016048  16.9
    1   2  2011-08-18 00:35:00.00000049  40.761270 -73.982738   5.7
    2   3   2012-04-21 04:30:42.0000001  40.733143 -73.987130   7.7
