import os
import pickle
import shutil
import re

class File_Operations:

    def __init__(self,log_object):

        self.log_instance = log_object
        self.directory = "Models/"
        self.log_instance.info("Entered in File Operation module")

    def save_model(self,model_object,model_name):
    
        self.log_instance.info("Accessing save_model method in File_operations class for model saving")
        try:
            path = os.path.join(self.directory,model_name)
            if os.path.isdir(path):
                shutil.rmtree(path)
            os.makedirs(path)
            with open(path+"/"+model_name+".pickle","wb") as file:
                pickle.dump(model_object,file)
                self.log_instance.info("Model: "+ model_name + " saved, Exiting from File_operations Module.")

        except Exception as E:
            self.log_instance.exception("Exception occured while saving the model: "+ model_name +", Exception msg: "+ str(E))
            self.log_instance.info("Model is not saved, Exiting from File_operations Module")
        

   
    def load_model(self,model_name):
        
        self.log_instance.info("Accessing load_model method in File_operations class for Loading the model")
        try:
            path = os.path.join(self.directory,model_name)
            with open(path+"/"+model_name+".pickle","rb") as file:
                self.log_instance.info("Model: "+model_name+" has been successfully loaded.")
                return pickle.load(file)
        
        except Exception as E:
            self.log_instance.exception("Exception occured while loading the model: "+ model_name +", Exception msg: "+ str(E))
            self.log_instance.info("Model is not loaded, Exiting from File_operations Module")


    def find_correct_model_file(self,cluster_no):
        try:
            self.log_instance.info("Accessing find_correct_model_file method for finding the correct model file")
            self.cluster_no = cluster_no
            self.lst = os.listdir(self.directory)
            for file in self.lst:
                s = re.compile("\d")
                r = s.findall(file)
                if r:
                    if int(r[0]) == self.cluster_no:
                        self.file_name = file
                        self.log_instance.info("correct model file found")
                        return self.file_name
                else:
                    continue

        except Exception as E:
            self.log_instance.error("Failed to find correct model")
            self.log_instance.exception(str(E))