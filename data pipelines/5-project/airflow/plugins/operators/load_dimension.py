from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Load data into dimension table with a customizable query.
    :param redshift_conn_id: Airflow connection id for Redshift connection secret
    :param table: Name of the table in Redshift that will be populated
    :param sql_select_stmt: Select query to retrieve rows that will be used for populating the target table.
    :param update_mode: Defines if the data should be appended to an existing table or overwritten. Use 'overwrite' if the target table should be truncated first.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table='',
        sql_select_stmt='',
        update_mode='append',
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql_select_stmt=sql_select_stmt
        self.update_mode=update_mode.lower()

        self._sql = 'INSERT INTO "{table:}" ({sql_select_stmt:})'

    def execute(self, context):
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        query = ''
        if self.update_mode == 'overwrite':
            query += f'TRUNCATE TABLE "{self.table}";\n';

        query += self._sql.format(**dict(
            table=self.table,
            sql_select_stmt=self.sql_select_stmt
        ))

        self.log.info(f'Loading dimension table {self.table}')
        self.log.debug(f"Formatted query: {query}")
        redshift_conn.run(query)
        self.log.info(f'Finished loading dimension table {self.table}')
