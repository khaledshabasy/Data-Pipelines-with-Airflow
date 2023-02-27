from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {0}
        FROM '{1}'
        ACCESS_KEY_ID '{2}'
        SECRET_ACCESS_KEY '{3}'
        {4}
        {5}
        {6}
        REGION 'us-west-2'
        TIMEFORMAT as 'epochmillisecs'
        {7}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
#                  create_stmt="",
                 cols="",
#                  ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
#         self.create_stmt = create_stmt
#         self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.cols = cols
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        """
        Copy data from S3 buckets to redshift cluster into staging tables.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
            
        aws_credentials_id: string
            airflow connection to AWS
            
        table: string
            destination staging tables located in redshift cluster
            
        s3_bucket: string
            bucket location of staging data
            
        s3_key: boolean
            path location of staging data
            
        file_format: string
            file format to copy data, default JSON
                        
        cols: string
            extra parameters to specify how to handle nulls in columns
        """
        self.log.info('StageToRedshiftOperator is being implemented')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         self.log.info("Creating the destination Redshift table to receive the data")
#         redshift.run(self.create_stmt)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        if self.file_format == 'CSV':
            format = "CSV"
            delimiter = "DELIMITER ','"
            header = "IGNOREHEADER 1"
            
        else:
            if self.table == 'staging_events':
                format = "FORMAT AS json 's3://udacity-dend/log_json_path.json'"
                delimiter = ""
                header = ""
                
            elif self.table == 'staging_songs':
                format = "FORMAT as json 'auto'"
                delimiter = ""
                header = ""

            
            
            
#             if self.table == 'staging_events':
#                 format = "FORMAT AS json 's3://udacity-dend/log_json_path.json'"
#                 delimiter = ""
#                 header = ""
#                 self.cols = "TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL"

#             elif self.table == 'staging_songs':
#                 format = "FORMAT as json 'auto'"
#                 delimiter = ""
#                 header = ""
#                 self.cols = ""


                


        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            format,
            delimiter,
            header,
            self.cols
        )
        redshift.run(formatted_sql)


