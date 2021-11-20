from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import boto3


class EMROperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(EMROperator, self).__init__(*args, **kwargs)
        self.__S3_BUCKET_SPARK_CODE = Variable.get("S3_BUCKET_SPARK_CODE")
        self.__S3_KEY = 'spark/capstone-project.py'
        self.__AWS_ACCESS_KEY_ID = Variable.get("USER_ACCESS_KEY_ID")
        self.__AWS_SECRET_ACCESS_KEY = Variable.get("USER_SECRET_ACCESS_KEY")

    def generate_uri(self):
        return 's3a://{bucket}/{key}'.format(bucket=self.__S3_BUCKET_SPARK_CODE, 
                                            key=self.__S3_KEY)

    def client(self):
        client = boto3.client('emr',
                            aws_access_key_id=self.__AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=self.__AWS_SECRET_ACCESS_KEY,
                            region_name='us-east-1')
        return client

    def process_data_to_gold(self):
        emrcluster = self.client.run_job_flow(
            Name='CapstoneCluster',
            LogUri=f's3://{self.__S3_BUCKET_SPARK_CODE}/logs/',
            ReleaseLabel='emr-5.3.0',
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm1.medium',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave nodes",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm1.medium',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': 'capstone-key-pair',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-b4740ad2',
            },
            BootstrapActions=[
                {
                    'Name': 'Maximize Spark Default Config',
                    'ScriptBootstrapAction': {
                        'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
                    }
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            Tags=[
                {
                    'Key': 'Name',
                    'Value': 'EMR with Boto',
                },
                {
                    'Key': 'TerminationVal',
                    'Value': 'OK',
                },
            ],
        )
        self.log.info(
            'ClusterID: {} , DateCreated: {} , RequestId: {}'
            .format(
                emrcluster['JobFlowId'], 
                emrcluster['ResponseMetadata']['HTTPHeaders']['date'], 
                emrcluster['ResponseMetadata']['RequestId']
                )
        )    
    def execute(self, context):
        try:
            self.process_data_to_gold()
        except Exception as exception:
            self.log.info(exception)