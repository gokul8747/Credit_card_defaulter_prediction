import matplotlib.pyplot as plt
from kneed import KneeLocator
from sklearn.cluster import KMeans
from File_Operations import file_operations

class KMeansclustering:
    def __init__(self,log_object):
        self.log_instance = log_object
        self.log_instance.info("Entered KMeansclutering method")
        self.file_inst = file_operations.File_Operations(log_object)

    
    def find_elbow(self,data):
        wcss = [ ]
        try:
            self.log_instance.info("Accessing find_elbow method in Kmeansclustering class for finding optimal k value")
            for i in range(1, 11):
                kmeans = KMeans(n_clusters = i, init = "k-means++", random_state = 50) 
                kmeans.fit(data) 
                wcss.append(kmeans.inertia_)
    
            plt.plot(range(1, 11), wcss)  
            plt.title("The Elbow Method")
            plt.xlabel("Number of clusters")
            plt.ylabel("WCSS")
            plt.savefig("Preprocessing_data/K-Means_Elbow.PNG")  
            self.kn = KneeLocator(range(1, 11), wcss, curve = "convex", direction = 'decreasing')
            self.log_instance.info("Best optimal k value for the given data is:"+str(self.kn.knee))
            self.log_instance.info("Saving the elbow plot as png in Preprocessing_data folder")
            return self.kn.knee

        except Exception as E:
            self.log_instance.error("Failed to find optimal k value")
            self.log_instance.exception(str(E))

    def create_cluster(self,data,optimal_cluster):
        try:
            self.log_instance.info("Accessing create_cluster method in Kmeansclustering class for creating clusters")
            self.data = data
            self.kmeans = KMeans(n_clusters = optimal_cluster, init = "k-means++", random_state = 50)
            self.kmeans_fit = self.kmeans.fit(data.values)
            self.log_instance.info("KMean model is fitted for the given data")
            self.cluster_label = self.kmeans_fit.fit_predict(data.values)
            self.file_inst.save_model(self.kmeans_fit,"KMeans")
            self.data["Cluster"] = self.cluster_label
            self.log_instance.info("Data clustered successfully and assigned with respective cluster lables")
            return self.data

        except Exception as E:
            self.log_instance.error("Failed to cluster the data")
            self.log_instance.exception(str(E))



        