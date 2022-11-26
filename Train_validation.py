from os import listdir
import shutil
import pandas as pd
from Raw_file_validation import Raw_validation


class Train_validator:

    def __init__(self,path,log_object):
        self.log_instance = log_object
        self.path = path #Training_dataset/
        self.log_instance.info("Entered in Train_validator module")
        self.raw = Raw_validation.Raw_validator(self.log_instance)
    
    def train_validate(self):
        try:
            self.log_instance.info("Accessing train_validate method in Train_validator class for train data validation")
            self.raw.delete_goodpath()
            self.raw.delete_badpath()
            self.raw.create_good_bad_path()
            lengthofdatestamp,no_ofcolumns = self.raw.value_from_schema()
            regex = self.raw.manual_regex()
            for file in listdir(self.path):
                if self.raw.validate_filename(regex,file,lengthofdatestamp):
                    data = pd.read_csv(self.path+"/"+file)
                    if self.raw.validate_column(data,no_ofcolumns):
                        if not self.raw.null_check(data):
                            shutil.copy(self.path+"/"+file,"Training_raw_file/good_data/")
                            self.log_instance.info("successfully copied data to good_data path")
                            self.log_instance.info("data is ready for uploading in database")

                        else:
                            self.log_instance.error("Invalid data,data moved to bad_data folder")
                            shutil.move(self.path+"/"+file,"Training_raw_file/bad_data/")
                    else:
                        self.log_instance.error("Invalid data,data moved to bad_data folder")
                        shutil.move(self.path+"/"+file,"Training_raw_file/bad_data/")
                else:
                    self.log_instance.error("Invalid data,data moved to bad_data folder")
                    shutil.move(self.path+"/"+file,"Training_raw_file/bad_data/")
        except Exception as E:
            self.log_instance.info("Error occurred while validating")
            self.log_instance.exception(str(E))
