from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """load json files from S3 to Amazon Redshift
    * create and run a copy_sql to a target table
    * the name of the target table is defined by users 
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}' truncatecolumns
        region 'us-west-2' compupdate off 
        TIMEFORMAT AS 'epochmillisecs'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 target_table="",
                 s3_bucket="",
                 s3_key="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.target_table = target_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
    def execute(self, context):
        """ run copy_sql with Redshift
        aws_hook: parse the aws credentials stored in WebUI, and
        redshift: redshift cluter that runs sql statment"""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.target_table))

        # define s3 path of json files by rendering s3_key
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(s3_path)

        # if we stage events, we need to define a json path.
        # else, we can use json 'auto'
        if self.target_table=="staging_events":
            json="s3://udacity-dend/log_json_path.json"
        else:
            json="auto"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.target_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json
        )
        redshift.run(formatted_sql)





