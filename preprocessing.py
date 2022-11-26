from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE
import pandas as pd

class preprocessor:

    def __init__(self,log_object):
        self.log_instance = log_object
        self.log_instance.info("Entered in preprocessor class module")

    def delete_column(self,data,col_name):
        try:
            self.log_instance.info("Accessing delete_column method for deletion of column")
            data = data.drop(col_name,axis=1)
            self.log_instance.info("Column "+col_name+" dropped successfully")
            return data
        except Exception as E:
            self.log_instance.error("Failed to drop column")
            self.log_instance.exception(str(E))


    def separate_feature_label(self,data,label_column):
        try:
            self.log_instance.info("Accessing separate_feature_label method in preprocessor class for separating features and label")
            self.x = data.drop(label_column,axis = 1)
            self.log_instance.info("feature is separated from data")
            self.y = data[label_column]
            self.log_instance.info("Label is separated from data")
            self.log_instance.info("returning separated feature and label")
            return self.x,self.y

        except Exception as E:
            self.log_instance.error("Failed to separate data ")
            self.log_instance.exception(str(E))


    def standardizer(self,data):
        try:
            self.log_instance.info("Accessing standardizer method in preprocessor class for data standardizing")
            scaler = StandardScaler()
            self.stand_data = scaler.fit_transform(data)
            self.log_instance.info("data standardized successfully")
            return self.stand_data

        except Exception as E:
            self.log_instance.error("Failed to standardize data")
            self.log_instance.exception(str(E))

    def resampler(self,data,label_column):
        try:
            self.log_instance.info("Accessing resampler method in preprocessor class for resampling of imbalanced data")
            smote = SMOTE()
            self.x_smote,self.y_smote = smote.fit_resample(data.iloc[:,0:-1],data[label_column])
            self.log_instance.info("successfully resampled data")
            columns = list(data.columns)
            columns.pop()
            self.log_instance.info("Converting balanced data to dataframe")
            self.balanced_df = pd.DataFrame(self.x_smote,columns=columns)
            self.balanced_df[label_column] = self.y_smote
            self.log_instance.info("successfully converted to dataframe")
            return self.balanced_df

        except Exception as E:
            self.log_instance.error("Failed to resample data")
            self.log_instance.exception(str(E))

        

