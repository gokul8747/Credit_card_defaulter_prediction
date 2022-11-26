import pandas as pd

class Data_getter:
    
    def __init__(self,log_object):
        self.log_instance = log_object
        self.path = "Trainingfile_fromDB/input_data.csv"
        self.log_instance.info("Entered in Data_getter module")

    def get_data(self):
        try:
            self.log_instance.info("Accessing get_data method in Data_getter class for loading data")
            data = pd.read_csv(self.path)
            self.log_instance.info("data loaded successfully")
            return data

        except Exception as E:
            self.log_instance.error("failed to load data")
            self.log_instance.exception(str(E))