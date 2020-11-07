from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """load data into songplays table
    by sql_queries from songplay_table_insert
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql_stmt="",
                 *args, **kwargs):
        """input: redshift, table, and sql"""

        # as sql stmt already defines the input table (staging_events, staing_songs)
        # we do not have to input them
        # however, it lacks "INSERT INTO" in the beginning
        # so we need to expand the sql statment
        # we do not need aws_credentials, because this is transformation inside RS
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table=target_table
        self.sql_stmt=sql_stmt

    def execute(self, context):
        """ run sql to insert songplays table"""
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Inserting data")

        formatted_sql=f"""
                    INSERT INTO {self.target_table}
                    {self.sql_stmt}
                    """
        # we do not need to empty table
        # Facttables are massive that they should only allow append type functionality.
        redshift.run(formatted_sql)
        self.log.info(f"Finished inserting data into {self.target_table}")





