from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {}
    """
    truncate_sql = """
        TRUNCATE TABLE {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 insert_stmt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_stmt = insert_stmt
        self.truncate_table = truncate_table
        
    def execute(self, context):
        """
        Insert data into dimensional tables from staging events and song data.
        Using a truncate-insert method to empty target tables prior to load.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
            
        table: string
            destination dim tables located in redshift cluster
            
        insert_stmt: string
            the SQL statement in which we perfrom a select query to insert data into fact table

        truncate_table: string
            specified to False to check if the table is full and must be cleared before insertion
        """
        self.log.info('LoadDimensionOperator is being implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.truncate_table:
            self.log.info(f"Clearing data from {self.table} dimension table")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))
            
            self.log.info(f"Inserting data into {self.table} dimension table")
            formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_stmt
        )
            redshift.run(formatted_sql)
            self.log.info(f"Successfully Loaded {self.table} Dimension table")



        
        if self.truncate_table:
            self.log.info(f"Inserting data into {self.table} dimension table")
            formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_stmt
        )
            redshift.run(formatted_sql)
            self.log.info(f"Successfully Loaded {self.table} Dimension table")
