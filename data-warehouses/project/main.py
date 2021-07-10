from config import Config

if __name__ == '__main__':
  config = Config()
  print(config.log_data)
  print(config.log_path)
  print(config.song_data)
  print(config.iam_role)