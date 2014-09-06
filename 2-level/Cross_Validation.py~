import Build_Model
import Testing
import logs
from sklearn import cross_validation
import numpy as np


new_model = logs.logs_object('TeraSort')


MapFeature_list = np.array(new_model.get_MapFeature_list())
RedFeature_list =np.array(new_model.get_RedFeature_list())
MapMean_list = np.array(new_model.get_MapMean_list())
RedMean_list = np.array(new_model.get_RedMean_list())

MapDev_list = np.array(new_model.get_MapDev_list())
RedDev_list = np.array(new_model.get_RedDev_list())

Target_list = np.array(new_model.get_target_list())


for i in range(10):
	MF_train, MF_test,RF_train, RF_test, MM_train, MM_test, RM_train, RM_test,MD_train, MD_test,RD_train, RD_test,T_train, T_test,= cross_validation.train_test_split(MapFeature_list,RedFeature_list,MapMean_list,RedMean_list,MapDev_list,RedDev_list,Target_list, test_size=0.3, random_state=0)
	
	engine = Build_Model.WhatIf_Engine(MF_train,RF_train,MM_train,RM_train,MD_train,RD_train,T_train)
	engine.Build_MapMean_Model()
	engine.Build_MapDev_Model()
	engine.Build_RedMean_Model()
	engine.Build_RedDev_Model()
	engine.Build_Final_Model()
	predict_result = Testing.test(MF_test,RF_test)
	print new_model.get_error_rate(T_test,predict_result)


