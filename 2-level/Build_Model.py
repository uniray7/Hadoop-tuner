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
		MapMean_Model =  ExtraTreeRegressor()

		MapMean_Model.fit(self.MapFeature_list,self.MapMean_list)
		self.Dump_Model('Model/MapMean.model',MapMean_Model)

	def Build_MapDev_Model(self):
		MapDev_Model =  linear_model.LinearRegression()
		MapDev_Model.fit(self.MapFeature_list,self.MapDev_list)
		self.Dump_Model('Model/MapDev.model',MapDev_Model)


	def Build_RedMean_Model(self):
		RedMean_Model = ensemble.RandomForestRegressor(n_estimators=20)
		RedMean_Model.fit(self.RedFeature_list,self.RedMean_list)
		self.Dump_Model('Model/ReduceMean.model',RedMean_Model)


	def Build_RedDev_Model(self):
		RedDev_Model =  linear_model.LinearRegression()
		RedDev_Model.fit(self.RedFeature_list,self.RedDev_list)
		self.Dump_Model('Model/ReduceDev.model',RedDev_Model)


	def Concate_Task_Feature(self):
		interData_list = []
		for i in range(len(self.MapMean_list)):
			wave = (float(self.MapFeature_list[i][-1])*1024)/(8*float(self.MapFeature_list[i][1])*float(self.MapFeature_list[i][2]))
			interData_list.append(np.hstack(((self.MapMean_list[i]),int(wave),self.RedMean_list[i],self.MapDev_list[i],self.RedDev_list[i])))
		return interData_list


	def Build_Final_Model(self):
		interData_list = self.Concate_Task_Feature()
		Final_Model=ensemble.GradientBoostingRegressor()
		Final_Model.fit(interData_list,self.Target_list)
		self.Dump_Model('Model/Job.model',Final_Model)

	def Dump_Model(self,path,model):
		with open(path,'wb') as model_outstream:
			cPickle.dump(model,model_outstream)






