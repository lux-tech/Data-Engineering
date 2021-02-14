from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to transfer data from S3 to staging tables in a Redshift database.
    
    :param redshift_conn_id: Airflow connection id for Redshift connection secret
    :param aws_credentials_id: Airflow connection id for AWS connection secret
    :param table: Name of the table in Redshift that will be populated
    :param s3_src_bucket: Name of S3 source bucket, e.g. 'udacity-dend'
    :param s3_src_pattern: Pattern for files in the S3 source bucket. Templatable field.
        E.g. 'log_data/{execution_date.year}/{execution_date.month}/'
    :param data_format: Determines the format of the input data. Can be either 'csv' or 'json'.
    :param delimiter: CSV delimiter, will be ignored if `data_format` is not set to 'csv'
    :param jsonpaths: Defines how JSON objects are handled during import. More information see
        [here](https://docs.aws.amazon.com/en_us/redshift/latest/dg/copy-usage_notes-copy-from-json.html).
        Defaults to 'auto'.
    :param ignore_header: Specifies how many header lines should be ignored.
        [See documentation for
        details](https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-ignoreheader)
    """
    ui_color = '#358140'
    template_fields = ("s3_src_pattern",)

    @apply_defaults
    def __init__(
        self,
        # connections
        redshift_conn_id='',
        aws_credentials_id='',

        # source / target config
        table='',
        s3_src_bucket='',
        s3_src_pattern='',
        data_format='json',    # 'csv' or 'json', case insensitive
        delimiter=',',
        jsonpaths='auto',
        copy_opts='',
        ignore_header=0,
        *args, **kwargs
    ):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_src_bucket = s3_src_bucket
        self.s3_src_pattern = s3_src_pattern
        self.data_format = data_format.lower()
        self.delimiter = delimiter
        self.jsonpaths = jsonpaths
        self.copy_opts = copy_opts
        self.ignore_header = ignore_header

        self._sql = """
            COPY {table:}
            FROM '{source:}'
            ACCESS_KEY_ID '{access_key:}'
            SECRET_ACCESS_KEY '{secret_access_key:}'
            {fmt:}
        """

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift_conn = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.debug(f"Truncate table: {self.table}")
        redshift_conn.run(f"TRUNCATE TABLE {self.table}")

        fmt = ''
        if self.data_format == 'csv' and self.ignore_header > 0:
            fmt += f"IGNOREHEADER {self.ignore_header}\n"

        if self.data_format == 'csv':
            fmt = f"DELIMITER '{self.delimiter}'\n"
        elif self.data_format == 'json':
            json_option = self.jsonpaths
            fmt = f"FORMAT AS JSON '{json_option}'\n"
        fmt += f"{self.copy_opts}"
        self.log.debug(f"fmt: {fmt}")

        formatted_pattern = self.s3_src_pattern.format(**context)
        self.log.info(f'Rendered S3 source file pattern: {formatted_pattern}')
        s3_uri = f"s3://{self.s3_src_bucket}/{formatted_pattern}"
        self.log.debug(f"S3 URI: {s3_uri}")
        formatted_sql = self._sql.format(**dict(
            table=self.table,
            source=s3_uri,
            access_key=aws_credentials.access_key,
            secret_access_key=aws_credentials.secret_key,
            ignore_header=self.ignore_header,
            fmt=fmt
        ))
        self.log.debug(f"Base SQL: {self._sql}")

        self.log.info(f"Copying data from S3 to Redshift [table: {self.table}] ...")
        redshift_conn.run(formatted_sql)
        self.log.info(f"Finished copying data from S3 to Redshift [table: {self.table}]")





