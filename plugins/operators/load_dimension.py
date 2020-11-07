from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 sql_stmt="",
                 append_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # as sql stmt already defines the input table (staging_events, staing_songs)
        # we do not have to input them
        # however, it lacks "INSERT INTO" in the beginning
        # so we need to expand the sql statment
        # we do not need aws_credentials, because this is transformation inside RS
        self.redshift_conn_id = redshift_conn_id
        self.target_table=target_table
        self.sql_stmt=sql_stmt
        self.append_mode=append_mode

    def execute(self, context):
        """ run sql to insert dim table"""
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Inserting data into {self.target_table}")

        formatted_sql=f"""
                    INSERT INTO {self.target_table}
                    {self.sql_stmt}
                    """
        # we need to empty table
        # Dimension loads are often done with the truncate-insert pattern 
        # where the target table is emptied before the load.
        # Thus, you could also have a parameter that allows switching between insert modes when loading dimensions.
        if self.append_mode==True: #direct append
            pass
        else: #insert-truncate
            redshift.run(f"DELETE FROM {self.target_table}" )
        redshift.run(formatted_sql)

        self.log.info(f"Finished inserting data into {self.target_table}")
