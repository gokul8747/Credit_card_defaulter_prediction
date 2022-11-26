import re
import os
import shutil
import json
import numpy as np

class Raw_validator:

    def __init__(self,log_object):
        self.log_instance = log_object
        self.path = "Training_dataset"
        self.schema_path = "schema.json"
        self.log_instance.info("Entered in Raw_validator module")


    def create_good_bad_path(self):
        try:
            self.log_instance.info("Accessing create_good_bad_path method in Raw_validator class for creating directory")
            path1 = os.path.join("Training_raw_file/"+"good_data/")
            if not os.path.isdir(path1):
                os.makedirs(path1)
                self.log_instance.info("good_data folder created successfully")

            path2 = os.path.join("Training_raw_file/"+"bad_data/")
            if not os.path.isdir(path2):
                os.makedirs(path2)
                self.log_instance.info("bad_data folder created successfully")

        except Exception as E:
            self.log_instance.info("Error occured while creating directory")
            self.log_instance.exception(str(E))




    def delete_goodpath(self):
        try:
            self.log_instance.info("Accessing delete_goodpath method in Raw_validator class to delete good_data folder if already exists")
            path1 = os.path.join("Training_raw_file/"+"good_data/")
            if os.path.isdir(path1):
                shutil.rmtree(path1)
                self.log_instance.info("good_data folder deleted successfully")

        except Exception as E:
            self.log_instance.info("Error occurred while deletion")
            self.log_instance.exception(str(E))


    def delete_badpath(self):
        try:
            self.log_instance.info("Accessing delete_badpath method in Raw_validator class to delete bad_data folder if already exists")
            path2 = os.path.join("Training_raw_file/"+"bad_data/")
            if os.path.isdir(path2):
                shutil.rmtree(path2)
                self.log_instance.info("bad_data folder deleted successfully")

        except Exception as E:
            self.log_instance.info("Error occurred while deletion")
            self.log_instance.exception(str(E))


    

    def value_from_schema(self):
        try:
            self.log_instance.info("Accessing value_from_schema method in Raw_validator class for getting schema values")
            with open(self.schema_path,"r") as file:
                schema = json.load(file)
                self.log_instance.info("successfully loaded schema file value")

            lengthofdatestamp = schema["LengthOfDateStampInFile"]
            no_ofcolumns = schema["NumberofColumns"]
            return lengthofdatestamp,no_ofcolumns

        except Exception as E:
            self.log_instance.info("Error occurred while loading values from schema file")
            self.log_instance.exception(str(E))

    def manual_regex(self):
        self.log_instance.info("Accessing manual_regex method in Raw_validator class for regex creation")
        regex = "['creditCardFraud']+['\_'']+[\d_]+\.csv"
        self.log_instance.info("regex created successfully")
        return regex

    
    def validate_filename(self,regex,file_name,lengthofdatestamp):
        try:
            self.log_instance.info("Accessing validate_filename method in Raw_validator class for validation of filename")
            if re.match(regex, file_name):
                match = re.split(".csv", file_name)
                match = (re.split("_", match[0]))
                if len(match[1]) == lengthofdatestamp:
                    self.log_instance.info("valid file name")
                    return True

            else:
                self.log_instance.info("Not a valid file format")
                False

        except Exception as E:
            self.log_instance.exception(str(E))
    
 
    def validate_column(self,data,no_column):
        try:
            self.log_instance.info("Accessing validate_column method in Raw_validator class for column validation")
            if data.shape[1] == no_column:
                self.log_instance.info("valid column size")
                return True

            else:
                self.log_instance.info("Invalid column size")
                return False

        except Exception as E:
            self.log_instance.exception(str(E))

    def null_check(self,data):
        try:
            self.log_instance.info("Accessing null_check method in Raw_validator class for checking if any of the column is empty")
            for cols in data:
                value = np.all(data[cols].isnull())
                if value == True:
                    self.log_instance.error("given data have empty column value at column" + cols)
                    return True
            self.log_instance.info("columns are valid")
            return False

        except Exception as E:
            self.log_instance.exception(str(E))