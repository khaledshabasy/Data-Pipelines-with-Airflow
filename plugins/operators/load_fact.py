from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {}
        {}
    """
    delete_sql = """
        DELETE FROM {}
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 insert_stmt="",
#                  create_stmt="",
                 append_mode=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_stmt = insert_stmt
#         self.create_stmt = create_stmt
        self.append_mode = append_mode
        
    def execute(self, context):
        """
        Insert data into fact tables from staging events and song data.
        Using a truncate-insert method to empty target tables prior to load.
        Parameters:
        ----------
        redshift_conn_id: string
            airflow connection to redshift cluster
            
        table: string
            destination fact table located in redshift cluster
            
        insert_stmt: string
            the SQL statement in which we perfrom a select query to insert data into fact table
            
            
        append_mode: string
            specified to True to check if the table is full and must be deleted to append
        """
        self.log.info('LoadFactOperator is being implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
#         self.log.info("Creating the fact table to receive the data")
#         redshift.run(self.create_stmt)
        
        if self.append_mode:
            
            self.log.info(f"Inserting data into {self.table} fact table")
            formatted_sql = LoadFactOperator.insert_sql.format(
                self.table,
                self.insert_stmt
            )
            redshift.run(formatted_sql)
            self.log.info(f"Successfully Loaded {self.table} fact table")
            
        else:
            self.log.info(f"Deleting data from {self.table} fact table")
            redshift.run(LoadFactOperator.delete_sql.format(self.table))
            
            self.log.info(f"Inserting data into {self.table} fact table")
            formatted_sql = LoadFactOperator.insert_sql.format(
                self.table,
                self.insert_stmt
            )
            redshift.run(formatted_sql)
            self.log.info(f"Successfully Loaded {self.table} fact table")
        
        

