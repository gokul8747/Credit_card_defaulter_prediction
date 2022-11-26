import logging

class App_logger:
    def __init__(self):
        self.format = logging.Formatter("%(asctime)s %(levelname)s %(message)s",datefmt="%Y-%m-%d %H:%M:%S")

    def log(self,log_file):
        self.handler = logging.FileHandler(log_file)
        self.handler.setFormatter(self.format)
        self.logger = logging.getLogger(None)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)
        return self.logger