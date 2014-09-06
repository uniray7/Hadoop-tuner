import logs
import numpy as np
import cPickle

from sklearn.neighbors import KNeighborsRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.tree import ExtraTreeRegressor
from sklearn import linear_model
from sklearn import ensemble



class WhatIf_Engine:
	def __init__(self,MapFeature_list,RedFeature_list,MapMean_list,RedMean_list,MapDev_list,RedDev_list,Target_list):
		self.MapFeature_list = MapFeature_list
		self.RedFeature_list = RedFeature_list
		self.MapMean_list = MapMean_list
		self.RedMean_list = RedMean_list
		self.MapDev_list = MapDev_list
		self.RedDev_list = RedDev_list
		self.Target_list = Target_list

	def Build_MapMean_Model(self):
		knn_MapMean =  ExtraTreeRegressor()

		knn_MapMean.fit(self.MapFeature_list,self.MapMean_list)
		self.Dump_Model('Model/MapMean.model',knn_MapMean)

	def Build_MapDev_Model(self):
		knn_MapDev =  linear_model.LinearRegression()
		knn_MapDev.fit(self.MapFeature_list,self.MapDev_list)
		self.Dump_Model('Model/MapDev.model',knn_MapDev)


	def Build_RedMean_Model(self):
		DTR_RedMean = ensemble.RandomForestRegressor(n_estimators=20)

		DTR_RedMean.fit(self.RedFeature_list,self.RedMean_list)
		self.Dump_Model('Model/ReduceMean.model',DTR_RedMean)


	def Build_RedDev_Model(self):
		DTR_RedDev =  linear_model.LinearRegression()

		DTR_RedDev.fit(self.RedFeature_list,self.RedDev_list)
		self.Dump_Model('Model/ReduceDev.model',DTR_RedDev)


	def Concate_Task_Feature(self):
		interData_list = []
		for i in range(len(self.MapMean_list)):
			wave = (float(self.MapFeature_list[i][-1])*1024)/(8*float(self.MapFeature_list[i][1])*float(self.MapFeature_list[i][2]))
			interData_list.append(np.hstack(((self.MapMean_list[i]),int(wave),self.RedMean_list[i],self.MapDev_list[i],self.RedDev_list[i])))
		return interData_list


	def Build_Final_Model(self):
		interData_list = self.Concate_Task_Feature()
		#knn_Final = linear_model.LinearRegression()
		#knn_Final = ExtraTreeRegressor()
		#knn_Final=ensemble.RandomForestRegressor(n_estimators=20)
		knn_Final=ensemble.GradientBoostingRegressor()
		knn_Final.fit(interData_list,self.Target_list)
		self.Dump_Model('Model/Job.model',knn_Final)
#		import sys
#		sys.path.append('/home/trend-hadoop/expr/implementation/libsvm-3.18/python')
#		from svm import *
#		from svmutil import *
#		x_dict_list = []
#		for x_array in interData_list:
#			i = 1
#			x_dict = dict()
#			for x_feature in x_array:
#				x_dict.update({i:float(x_feature)})
#				i = i+1
#			x_dict_list.append(x_dict)
#		
#		problem = svm_problem(self.Target_list,x_dict_list)
#
#		param = svm_parameter()
##		param.svm_type=3
#		param.kernel_type=1
#		param.degree=2
#		param.cost=10
#		param.epsilon=100
#		model = svm_train(problem,param)	
#
#		svm_save_model('Final_svm.model',model)

	def Dump_Model(self,path,model):
		with open(path,'wb') as model_outstream:
			cPickle.dump(model,model_outstream)






