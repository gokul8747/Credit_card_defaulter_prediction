from  Database_operations import Cassandra_db
from Application_Logging import logger
from Data_injection import data_injector
from File_Operations import file_operations
from preprocessing import preprocessor
from cluster import KMeansclustering
from Best_model_finder import tuning
from Train_validation import Train_validator
from sklearn.model_selection import train_test_split

class train_model:
    def __init__(self,data):
        self.l_inst = logger.App_logger()
        self.log_instance = self.l_inst.log("Logs/Training.log")
        self.path = data
        self.log_instance.info("Entered in train_model module for model training")

    def training_model(self):
        try:
            self.log_instance.info("Accessing training_model method in train_model class for model training")
            self.validate = Train_validator(self.path,self.log_instance)
            self.validate.train_validate()
            self.db = Cassandra_db.Db_oberation(self.log_instance)
            self.db.delete_table()
            self.db.create_table()
            self.db.insert_into_table()
            self.db.data_from_db()
            self.log_instance.info("Starting of Training")
            self.data_geter = data_injector.Data_getter(self.log_instance)
            data = self.data_geter.get_data()
            self.preprocessor = preprocessor(self.log_instance)
            data = self.preprocessor.delete_column(data,"id")
            balanced_data = self.preprocessor.resampler(data,"is_defaulter")
            x,y = self.preprocessor.separate_feature_label(balanced_data,"is_defaulter")
            self.kmeans = KMeansclustering(self.log_instance)
            n_cluster = self.kmeans.find_elbow(x)
            x = self.kmeans.create_cluster(x,n_cluster)
            x["Labels"] = y
            lst_of_cluster = x["Cluster"].unique()
            for i in lst_of_cluster:
                clustered_data = x[ x[ 'Cluster' ] == i ] 
                cluster_x = clustered_data.drop(['Labels','Cluster'],axis = 1)
                cluster_y = clustered_data["Labels"]
                x_stand = self.preprocessor.standardizer(cluster_x)
                x_train,x_test,y_train,y_test = train_test_split(x_stand,cluster_y,test_size=0.20,random_state=100)
                self.best_model = tuning.Model_finder(self.log_instance)
                model_name,model_object = self.best_model.get_best_model(x_train,y_train,x_test,y_test)
                self.log_instance.info("Best model for cluster: "+str(i)+" is model "+model_name)
                self.file_inst = file_operations.File_Operations(self.log_instance)
                self.file_inst.save_model(model_object,model_name+str(i))
            self.log_instance.info("Training process successfully ended")
        except Exception as E:
            self.log_instance.error("Unsuccessful End of Training")
            self.log_instance.exception(str(E))