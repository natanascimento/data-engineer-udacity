import configparser

class Config:
    
    def __init__(self):
        self.__config = configparser.ConfigParser().read('dwh.cfg')
    
    @property
    def log_data(self):
        log_data = self.__config.get("S3","LOG_DATA")
        return log_data
    
    @property   
    def log_path(self):
        config = self.configure_dw
        log_path = self.__config.get("S3", "LOG_JSONPATH")
        return log_path   
    
    @property
    def song_data(self):
        config = self.configure_dw
        song_data = self.__config.get("S3", "SONG_DATA")
        return song_data   
    
    @property
    def iam_role(self):
        config = self.configure_dw
        iam_role = self.__config.get("IAM_ROLE","ARN")
        return iam_role