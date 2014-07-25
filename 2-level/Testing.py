import logs
import numpy as np
import cPickle
import math
import sys
sys.path.append('/home/trend-hadoop/expr/implementation/libsvm-3.18/python')
from svm import *
from svmutil import *
							
def test(MapFeature_Test,RedFeature_Test):

	with open('Model/MapMean.model','rb') as MPM:
		knn_MapMean = cPickle.load(MPM)
	
	with open('Model/ReduceMean.model','rb') as RDM:
		DTR_RedMean = cPickle.load(RDM)

	with open('Model/MapDev.model','rb') as MPD:
		knn_MapDev = cPickle.load(MPD)
	with open('Model/ReduceDev.model','rb') as RDD:
		DTR_RedDev = cPickle.load(RDD)

	with open('Model/Job.model','rb') as JOB:
		knn_Final = cPickle.load(JOB)

	MapMean_list = knn_MapMean.predict(MapFeature_Test)
	RedMean_list = DTR_RedMean.predict(RedFeature_Test)
	MapDev_list = knn_MapDev.predict(MapFeature_Test)
	RedDev_list = DTR_RedDev.predict(RedFeature_Test)

	interData_list = []
	for i in range(len(MapMean_list)):
		wave = (float(MapFeature_Test[i][-1])*1024)/(8*float(MapFeature_Test[i][1])*float(MapFeature_Test[i][2]))
		interData_list.append(np.hstack((MapMean_list[i],int(wave),RedMean_list[i],MapDev_list[i],RedDev_list[i])))

	return knn_Final.predict(interData_list)


#new_model = logs.logs_object('TeraSort')

#ratio1 =-200
#MapFeature_list = new_model.get_MapFeature_list()
#RedFeature_list =new_model.get_RedFeature_list()

#MapFeature_list_test = MapFeature_list[ratio1:]
#RedFeature_list_test = RedFeature_list[ratio1:]


#target_list = new_model.get_target_list()
#target_list_test = target_list[ratio1:]


#predict_result = test(MapFeature_list_test,RedFeature_list_test)
#print new_model.get_error_rate(target_list_test,predict_result)


