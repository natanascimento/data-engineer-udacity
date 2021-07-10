from configparser import ConfigParser

class Config:
    
    def __init__(self):
        self.__config = ConfigParser()
        self.__config.read('dwh.cfg')
    
    @property
    def aws_config(self):
        """
        Manage AWS Parameters
        """
        __aws_config = self.__config['AWS'].values()
        __aws_config = [i for i in __aws_config]
        return {'AWS_KEY': __aws_config[0],
                'AWS_SECRET': __aws_config[1],}        

    @property
    def dwh_config(self):
        """
        Manage Data Warehouse Parameters
        """
        __dwh_config = self.__config['DWH'].values()
        __dwh_config = [i for i in __dwh_config]
        return {'DWH_CLUSTER_TYPE': __dwh_config[0],
                'DWH_NUM_NODES': __dwh_config[1],
                'DWH_NODE_TYPE': __dwh_config[2],
                'DWH_IAM_ROLE_NAME': __dwh_config[3],
                'DWH_CLUSTER_IDENTIFIER': __dwh_config[4],
                'DWH_DB': __dwh_config[5],
                'DWH_DB_USER': __dwh_config[6],
                'DWH_DB_PASSWORD': __dwh_config[7],
                'DWH_PORT': __dwh_config[8],}       

    @property
    def cluster_config(self):
        """
        Manage Cluster Parameters
        """
        __cluster_config = self.__config['CLUSTER'].values()
        __cluster_config = [i for i in __cluster_config]
        return {'HOST': __cluster_config[0],
                'DB_NAME': __cluster_config[1],
                'DB_USER': __cluster_config[2],
                'DB_PASSWORD': __cluster_config[3],
                'DB_PORT': __cluster_config[4]}      

    @property
    def iam_config(self):
        """
        Manage IAM Parameters
        """
        __iam_config = self.__config['IAM_ROLE'].values()
        __iam_config = [i for i in __iam_config]
        return {'ARN': __iam_config[0]}

    @property
    def s3_config(self):
        """
        Manage S3 Parameters
        """
        __s3_config = self.__config['S3'].values()
        __s3_config = [i for i in __s3_config]
        return {'LOG_DATA': __s3_config[0],
                'LOG_JSONPATH': __s3_config[1],
                'SONG_DATA': __s3_config[2]}
