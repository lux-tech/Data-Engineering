from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Checks the data quality based on SQL queries and expected results.
    :param redshift_conn_id: Airflow connection id for Redshift connection secret
    :param sql_queries: List of SQL queries that should be tested (must have the same length as tables)
    :param test_results: List of functions that take the result and return True if the test succeeded and False otherwise.
    :raises AssertionError: The first query that does not match the result raises an AssertionError
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        sql_queries=[],
        test_results=[],
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_queries=sql_queries
        self.test_results=test_results

    def execute(self, context):
        self.log.info('Running data quality checks')
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        test_pairs = zip(self.sql_queries, self.test_results)
        for query, test_fn in test_pairs:
            self.log.debug(f'Run data quality query: {query}')
            result = redshift_conn.get_pandas_df(query)
            self.log.debug(f'Result: {result}')
            if test_fn(result):
                self.log.info('Data quality check passed.')
            else:
                self.log.info('Data quality check failed.')
                raise AssertionError('Data quality check failed.')