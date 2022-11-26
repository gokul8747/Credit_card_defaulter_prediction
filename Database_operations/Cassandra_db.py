from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from os import listdir
import pandas as pd
import shutil

class Db_oberation:

    def __init__(self,log_object):
        self.log_instance = log_object
        self.client_id = "fxWteUinpmXIMuSonZOdxEHM"
        self.client_secret = "HBmX05xoeZxQWixAd4ktxKQhi-jDjMxO1,k3ps3v8.5b-nIFrZPYM3558B6fF4ypHsCX7NIqg4dfux,vI+5Cg5d,2f2Rd,aMlrkWqwbDaLpPomfjq7y-bxAz-pwsTXvc"
        self.secure_connect_bundle = 'Database_Operations/secure-connect-credit-card-default.zip'
        self.path = "Trainingfile_fromDB/"
        self.badfile = "Training_raw_file/bad_data/"
        self.goodfile = "Training_raw_file/good_data/"
        self.log_instance.info("Entered in Database_operation module")

    def database_connection(self):
        self.log_instance.info("Accessing database_connection method in DB_operation class for database connection")
        try:
            cloud_config = {'secure_connect_bundle': self.secure_connect_bundle}
            auth_provider = PlainTextAuthProvider(self.client_id,self.client_secret)
            cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)
            session = cluster.connect()
            self.log_instance.info("Database connected successfully")
            return session

        except ConnectionError:
            self.log_instance.error("Error occurred while connecting the database")
            raise ConnectionError

    
    def create_table(self):
        
        try:
            session = self.database_connection()
            self.log_instance.info("Accessing create_table method in DB_operation class for table creation")

            try:
                integer = 'int'
                floats = 'float'
                i_d = "ID"
                limit_bal = "LIMIT_BAL" 
                sex = "SEX"
                education = "EDUCATION"
                marriage = "MARRIAGE"
                age = "AGE"
                pay_0 = "PAY_0"
                pay_2 = "PAY_2"
                pay_3 = "PAY_3"
                pay_4 = "PAY_4"
                pay_5 = "PAY_5"
                pay_6 = "PAY_6"
                bill_amt1 = "BILL_AMT1"
                bill_amt2 = "BILL_AMT2"
                bill_amt3 = "BILL_AMT3"
                bill_amt4 = "BILL_AMT4"
                bill_amt5 = "BILL_AMT5"
                bill_amt6 = "BILL_AMT6"
                pay_amt1 = "PAY_AMT1"
                pay_amt2 = "PAY_AMT2"
                pay_amt3 = "PAY_AMT3"
                pay_amt4 = "PAY_AMT4"
                pay_amt5 = "PAY_AMT5"
                pay_amt6 = "PAY_AMT6"
                default_payment = "Is_defaulter"

                session.execute(f"CREATE TABLE good_data.good_raw_data({i_d} {integer} PRIMARY KEY, {limit_bal} {floats}, {sex} {integer}, {education} {integer}, {marriage} {integer},{age} {integer}, {pay_0} {integer}, {pay_2} {integer}, {pay_3} {integer}, {pay_4} {integer}, {pay_5} {integer}, {pay_6} {integer}, {bill_amt1} {floats}, {bill_amt2} {floats}, {bill_amt3} {floats}, {bill_amt4} {floats}, {bill_amt5} {floats}, {bill_amt6} {floats}, {pay_amt1} {floats}, {pay_amt2} {floats}, {pay_amt3} {floats}, {pay_amt4} {floats}, {pay_amt5} {floats}, {pay_amt6} {floats}, {default_payment} {integer});")
                self.log_instance.info("Table created successfully")
                session.shutdown()
                self.log_instance.info("database closed successfully")

            except Exception as E:
                self.log_instance.exception("Error occurred with exception: "+ str(E))
                self.log_instance.info("Table not created")
                session.shutdown()
                self.log_instance.info("database closed successfully")

        except Exception as E:
            self.log_instance.exception("Error occurred with exception: "+ str(E))
            session.shutdown()
            self.log_instace.info("database closed successfully")

    def insert_into_table(self):

        try:
            session = self.database_connection()
            self.log_instance.info("Accessing insert_into_table method in DB_operation class for insertion of data into table")
            goodFilePath = self.goodfile
            badFilePath = self.badfile
            files = [f for f in listdir(goodFilePath)]
            
            i_d = "ID"
            limit_bal = "LIMIT_BAL" 
            sex = "SEX"
            education = "EDUCATION"
            marriage = "MARRIAGE"
            age = "AGE"
            pay_0 = "PAY_0"
            pay_2 = "PAY_2"
            pay_3 = "PAY_3"
            pay_4 = "PAY_4"
            pay_5 = "PAY_5"
            pay_6 = "PAY_6"
            bill_amt1 = "BILL_AMT1"
            bill_amt2 = "BILL_AMT2"
            bill_amt3 = "BILL_AMT3"
            bill_amt4 = "BILL_AMT4"
            bill_amt5 = "BILL_AMT5"
            bill_amt6 = "BILL_AMT6"
            pay_amt1 = "PAY_AMT1"
            pay_amt2 = "PAY_AMT2"
            pay_amt3 = "PAY_AMT3"
            pay_amt4 = "PAY_AMT4"
            pay_amt5 = "PAY_AMT5"
            pay_amt6 = "PAY_AMT6"
            default_payment = "Is_defaulter"
        
            for file in files:
                try:
                    data = pd.read_csv(goodFilePath+"/"+file)
                    self.log_instance.info("Reading data from csv file")
                    self.log_instance.info("Inserting data into table")
                    for row in data.itertuples(index=False,name=None):
                        query = f"insert into good_data.good_raw_data ({i_d},{limit_bal},{sex},{education},{marriage},{age},{pay_0},{pay_2},{pay_3},{pay_4},{pay_5},{pay_6},{bill_amt1},{bill_amt2},{bill_amt3},{bill_amt4},{bill_amt5},{bill_amt6},{pay_amt1},{pay_amt2},{pay_amt3},{pay_amt4},{pay_amt5},{pay_amt6},{default_payment}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        try:
                            session.execute(query,row)
                    

                        except Exception as E:
                            raise E
                    self.log_instance.info("data loaded into table successfully")
                except Exception as E:
                    self.log_instance.error("Error while inserting data into table")
                    self.log_instance.exception(str(E))
                    shutil.move(goodFilePath+"/"+file,badFilePath)
                    self.log_instance.info("data moved to bad_data directory")
                    session.shutdown()
                    self.log_instance.info("database closed successfully")
        except:
            self.log_instance.info("Not able access insert_into_table method")
            session.shutdown()
            self.log_instance.info("database closed successfully")


    
    def data_from_db(self):

        try:
            session = self.database_connection()
            self.log_instance.info("Accessing data_from_db method in DB_operation class for exporting the data from database as csv file")
            data = pd.DataFrame(session.execute("SELECT ID,LIMIT_BAL, SEX,EDUCATION,MARRIAGE,AGE, PAY_0,PAY_2,PAY_3,PAY_4 ,PAY_5,PAY_6,BILL_AMT1,BILL_AMT2,BILL_AMT3,BILL_AMT4,BILL_AMT5,BILL_AMT6,PAY_AMT1,PAY_AMT2,PAY_AMT3,PAY_AMT4,PAY_AMT5,PAY_AMT6,Is_defaulter FROM good_data.good_raw_data;",timeout=None))
            file = "input_data.csv"
            data.to_csv(self.path+"/"+file,index=False)
            self.log_instance.info("data exported successfully from database")
            session.shutdown()
            self.log_instance.info("database closed successfully")
            

        except Exception as E:
            self.log_instance.exception(str(E))
            self.log_instance.error("Query not processed")
            self.log_instance.info("data exporting failed")
            session.shutdown()
            self.log_instance.info("database closed successfully")


    
    def delete_table(self):

        try:
            session = self.database_connection()
            self.log_instance.info("Accessing delete_table method in DB_operation class to drop existing table")
            session.execute("DROP TABLE IF EXISTS good_data.good_raw_data;")
            self.log_instance.info("Table dropped successfully")
            session.shutdown()
            self.log_instance.info("database closed successfully")

        except Exception as E:
            self.log_instance.error("Table Dropping failed")
            self.log_instance.exception(str(E))
            session.shutdown()
            self.log_instance.info("database closed successfully")
    
    

    def truncate_table(self):

        try:
            session = self.database_connection()
            self.log_instance.info("Accessing truncate_table method in DB_operation class for deletion of table")
            session.execute("TRUNCATE TABLE good_data.good_raw_data;")
            self.log_instance.info("Table truncated successfully")
            session.shutdown()
            self.log_instance.info("database closed successfully")

        except Exception as E:
            self.log_instance.error("Table truncate failed")
            self.log_instance.exception(str(E))
            session.shutdown()
            self.log_instance.info("database closed successfully")