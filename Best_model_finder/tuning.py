from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score, roc_auc_score

class Model_finder:
    
    def __init__(self,log_object):
        self.log_instance = log_object
        self.rfc = RandomForestClassifier()
        self.xgb = XGBClassifier(objective = "binary:logistic", use_label_encoder = False, n_jobs = -1)
        self.log_instance.info("Entered Model_finder module")

    def get_best_para_rfc(self,x_train,y_train):
        try:
            self.log_instance.info("Accessing get_best_para_rfc method in model_finder class for getting optimal parameter")
            self.param_grid = {'n_estimators': [100,150,200], 'max_depth': [10,20,30]}
            self.grid_rfc_clf = GridSearchCV(self.rfc, self.param_grid, scoring = 'accuracy', n_jobs = -1, verbose = 3, cv = 3)
            self.grid_rfc_clf.fit(x_train,y_train)
            dict = self.grid_rfc_clf.best_params_
            self.log_instance.info("Optimal parameter found for rfc to the given data")
            self.max_depth = dict["max_depth"]
            self.n_estimators = dict["n_estimators"]
            self.rfc = RandomForestClassifier(max_depth=self.max_depth,n_estimators=self.n_estimators)
            self.rfc.fit(x_train,y_train)
            return self.rfc
        except Exception as E:
            self.log_instance.error("Failed to get best parameter for randomforestclassifer")
            self.log_instance.exception(str(E))

    def get_best_para_xgb(self,x_train,y_train):

        try:
            self.log_instance.info("Accessing get_best_para_xgb method in model_finder class for getting optimal parameter")
            self.param_grid = {'max_depth':range(3,10,2),'min_child_weight':range(1,6,2)}
            self.grid_xgb_clf = GridSearchCV(estimator = XGBClassifier( learning_rate =0.1, n_estimators=140, max_depth=5,min_child_weight=1, gamma=0, subsample=0.8, colsample_bytree=0.8,objective= 'binary:logistic', nthread=4, scale_pos_weight=1, seed=27), param_grid = self.param_grid, scoring='accuracy',n_jobs=-1, cv=3, verbose = 2)
            self.grid_xgb_clf.fit(x_train, y_train)
            dict = self.grid_xgb_clf.best_params_
            self.log_instance.info("Optimal parameter found for xgb to the given data")
            self.max_depth = dict["max_depth"]
            self.min_child_weight = dict["min_child_weight"]
            self.xgb = XGBClassifier(max_depth = self.max_depth,min_child_weight=self.min_child_weight)
            self.xgb.fit(x_train,y_train)
            return self.xgb
        except Exception as E:
            self.log_instance.error("Failed to get best parameter for XGBClassifer")
            self.log_instance.exception(str(E))

    
    def get_best_model(self,x_train,y_train,x_test,y_test):
        try:
            self.log_instance.info("Accessing get_best_model method in Model_finder class for finding best model")
            self.rfc = self.get_best_para_rfc(x_train,y_train)
            self.pred_rfc = self.rfc.predict(x_test) 

            if len(y_test.unique()) == 1: 
                self.rfc_score = accuracy_score(y_test, self.pred_rfc)
            else:
                self.rfc_score = roc_auc_score(y_test, self.pred_rfc)

            self.xgb = self.get_best_para_xgb(x_train, y_train)
            self.pred_xgb = self.xgb.predict(x_test)

            if len(y_test.unique()) == 1: 
                self.xgb_score = accuracy_score(y_test, self.pred_xgb)
            else:
                self.xgb_score = roc_auc_score(y_test, self.pred_xgb)

            if (self.rfc_score > self.xgb_score):
                self.log_instance.info("Best model found successfully")
                return 'Randomforestclassifier',self.rfc
            else:
                self.log_instance.info("Best model found successfully")
                return "XGBClassifier",self.xgb

        except Exception as E:
            self.log_instance.error("Failed to find the best model")
            self.log_instance.exception(str(E))