from Application_Logging import logger
from File_Operations import file_operations


class prediction:
    def __init__(self, data):
        self.l_inst = logger.App_logger()
        self.log_instance = self.l_inst.log("Logs/Prediction.log")
        self.data = data 
        self.log_instance.info("Entered in prediction module")

    def predict(self):
        try:
            self.log_instance.info("Accessing predict method in prediction class for model prediction from user input data")
            file_inst = file_operations.File_Operations(self.log_instance)
            scaler = file_inst.load_model("scaler")
            x = scaler.transform(self.data)
            kmeans = file_inst.load_model('KMeans')
            cluster = kmeans.predict(x)
            model_name = file_inst.find_correct_model_file(cluster[ 0 ])
            model = file_inst.load_model(model_name)
            result = model.predict(x)
            self.log_instance.info("successfully prediction over")
            return result
        except Exception as E:
            self.log_instance.error("Error occurred while prediction process")
            self.log_instance.exception(str(E))
    