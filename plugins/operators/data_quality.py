from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 *args, **kwargs):
        """input: redshift id, target table must be a list of dim tables"""

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table=target_table

    def execute(self, context):
        """check data quality by:
        1. count total number of rows in all dim tables
        2. count total number of nan rows in all dim tables
        3. count number of duplicated rows in dim tables except time
        4. for time table, check all start_time are positive
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        pk={'users':'userid',
            'songs':'song_id',
            'artists':'artist_id',
            'time':'start_time'}
        for tbl in self.target_table:
            # 1. count number of rows
            records = redshift.get_records(f"SELECT COUNT(*) FROM {tbl}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"DQ failed. {tbl} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"DQ failed. {tbl} contained 0 rows")
            self.log.info(f"DQ on table {tbl} check passed with {num_records} records")

            # 2. count number of nan rows in primary key
            nan_table=redshift.get_records(f"""
                SELECT COUNT(*)
                FROM {tbl}
                WHERE {pk[tbl]} IS NULL
                """)
            nan_rows=nan_table[0][0]
            if nan_rows>0:
                raise ValueError(f"DQ failed. {tbl} contained {nan_rows} nan rows")
            self.log.info(f"DQ on table {tbl} check passed with 0 nan rows")

            # 3. count number of duplicated rows in user, song, artist(expected=0)
            #if tbl!="time":
            #    duplicated_tbl=redshift.get_records(f"""
            #    SELECT COUNT(*)
            #    FROM {tbl}
            #    GROUP BY {pk[tbl]}
            #    """)
            #    duplicated_row=duplicated_tbl[0][0]
            #    if duplicated_row>0:
            #        raise ValueError(f"DQ failed. {tbl} contained {duplicated_row} duplicated rows")
            #    self.log.info(f"DQ on table {tbl} check passed with 0 duplicated rows")
        
        # 4. for time table, check the distribution of start_time
        # min(start_time) must >0
        min_st=redshift.get_records(f"SELECT MIN(start_time) FROM time)")[0][0]
        if min_st<0:
            raise ValueError("DQ failed, time table contained negative start_time")  
        self.log.info(f"DQ on table time check passed with 0 negative start time. DQ finished.")
        

        