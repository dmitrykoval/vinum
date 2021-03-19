import pyarrow as pa
from typing import TYPE_CHECKING

from vinum.executor.executor import RecursiveExecutor
from vinum.parser.parser import parser_factory
from vinum.planner.planner import QueryPlanner

if TYPE_CHECKING:
    import pyarrow as pa


class StreamReader:
    """
    `StreamReader` represents a stream of data which is used is an input
    for query processor.

    Since input file may not fit into memory, StreamReader is the recommended
    way to execute queries on large files.

    `StreamReader` instances are created by vinum.stream_* functions,
    for example: :func:`vinum.stream_csv`.

    Parameters
    ----------
    reader : pa.RecordBatchFileReader
        Arrow Stream Reader
    """
    def __init__(self, reader):
        super().__init__()
        self._reader: pa.RecordBatchFileReader = reader

    def sql(self, query: str):
        """
        Executes SQL SELECT query on an input stream and return
        the result as a Table materialized in memory.

        Parameters
        ----------
        query : str
            SQL SELECT query.

        Returns
        -------
        :class:`vinum.Table`
            Vinum Table instance.

        See also
        --------
        sql_pd : Executes SQL SELECT query on a Table and returns
            the result of the query as a Pandas DataFrame.

        Notes
        -----
        Only SELECT statements are supported.
        For SELECT statements, JOINs and subqueries are currently
        not supported.
        However, optimizations aside, one can run a subsequent query on the
        result of a query, to model the behaviour of subqueries.

        Table name in 'select * from table' clause is ignored.
        The table of the underlying DataFrame is used to run a query.

        By default, all the Numpy functions are available via 'np.*'
        namespace.

        User Defined Function can be registered via
        :func:`vinum.register_python` or :func:`vinum.register_numpy`
        
        Examples
        --------
        Run aggregation query on a csv stream:

        >>> import vinum as vn
        >>> query = 'select passenger_count pc, count(*) from t group by pc'
        >>> vn.stream_csv('taxi.csv').sql(query).to_pandas()
           pc  count
        0   0    165
        1   5   3453
        2   6    989
        3   1  34808
        4   2   7386
        5   3   2183
        6   4   1016
        """
        from vinum import Table

        query_tree = parser_factory(query,
                                    self._reader.schema).parse()
        query_dag = QueryPlanner(query_tree, reader=self).plan_query()

        executor = RecursiveExecutor()
        result_table = executor.execute(query_dag)

        return Table(result_table.get_table())

    @property
    def reader(self) -> pa.RecordBatchFileReader:
        return self._reader
