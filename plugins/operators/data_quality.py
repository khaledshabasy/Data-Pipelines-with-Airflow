from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
#     count_sql = """
#         SELECT COUNT (*)
#         FROM {}
#     """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        """
        Perform data quality checks on resulting fact and dimension tables.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
            
        table: string
            table located in redshift cluster
            
        dq_checks: empty list
            an empty list to store key-value pairs of SQL queries and their expected results
        """
        self.log.info('DataQualityOperator is being implemented')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         for item in self.table:
#             records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {item}")
#             if len(records) < 1 or len(records[0]) < 1:
#                 raise ValueError(f"Data quality check failed. {item} returned no results")
#             num_records = records[0][0]
#             if num_records < 1:
#                 raise ValueError(f"Data quality check failed. {item} contained 0 rows")
#             self.log.info(f"Data quality on table {item} check passed with {records[0][0]} records")
        
        
        error_count = 0
        failed_tests = []
        
        for check in self.dq_checks:
            query=check.get('dq_query')
            result=check.get('exp_result')
            records = redshift_hook.get_records(query)[0]
                                            
            if result != records[0]:
                error_count += 1
                failed_tests.append(query)
                                            
        if error_count > 0:
            self.log.info(f"Test Failed:  {failed_tests}")
            raise ValueError("Data quality check failed")
                                            
        self.log.info(f"Data quality check succeeded on {self.table} table")