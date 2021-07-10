from configparser import ConfigParser

class Config:
    
    def __init__(self):
        self.__config = ConfigParser()
        self.__config.read('config/dwh.cfg')
        
    @property
    def log_data(self):
        log_data = self.__config.get("S3","LOG_DATA")
        return log_data
    
    @property   
    def log_path(self):
        log_path = self.__config.get("S3", "LOG_JSONPATH")
        return log_path   
    
    @property
    def song_data(self):
        song_data = self.__config.get("S3", "SONG_DATA")
        return song_data   
    
    @property
    def iam_role(self):
        iam_role = self.__config.get("IAM_ROLE","ARN")
        return iam_role