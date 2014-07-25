import json
import commands
import MongoConn
import math
import numpy as np

dbconn = MongoConn.DbConn()
conn = None



class logs_object:

	def __init__(self,job_name):
		self.job_name = job_name

	
	def connect_DB(self):
		dbconn.connect()
		global conn
		conn = dbconn.getConn()
		BigDB = conn.big
		terasort_coll = BigDB.terasort_model
		return terasort_coll

	def HeapSizeFilter(self,HeapSizeStr):
		HeapSizeStr = HeapSizeStr.replace('-Xmx','')
		HeapSizeStr = HeapSizeStr.replace('M','')
		HeapSizeInt = int(HeapSizeStr)
		return HeapSizeInt


	def get_MapFeature_list(self):
		feature_list = []
		wordcount_coll = self.connect_DB()
		for job in wordcount_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
			
				config_set = job['config_set']
				MapHeapSize =self.HeapSizeFilter(config_set['MapHeapSize'])			
				MapTasksMax = int(config_set['MapTasksMax'])
				SplitSize = int(config_set['SplitSize'])/(2**20)
				SortMB = int(config_set['SortMB'])
				SortPer = float(config_set['SortPer'])
				RecordPer = float(config_set['RecordPer'])
		
				JVMReuse = int(config_set['JVMReuse'])

				feature = [MapHeapSize,MapTasksMax,SplitSize,SortMB,SortPer,RecordPer,JVMReuse,job['data_size']]
				feature_list.append(feature)

		return feature_list

	def get_RedFeature_list(self):
		feature_list = []
		wordcount_coll = self.connect_DB()
		for job in wordcount_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
			
				config_set = job['config_set']
				MapHeapSize =self.HeapSizeFilter(config_set['MapHeapSize'])			
				MapTasksMax = int(config_set['MapTasksMax'])
				SplitSize = int(config_set['SplitSize'])/(2**20)
				SortMB = int(config_set['SortMB'])
				SortPer = float(config_set['SortPer'])
				RecordPer = float(config_set['RecordPer'])
	

				ReduceHeapSize = self.HeapSizeFilter(config_set['ReduceHeapSize'])
				ReduceTasksMax = int(config_set['ReduceTasksMax'])
				ReduceTasksNum = int(config_set['ReduceTasksNum'])
				ShuffleMergePer = float(config_set['ShuffleMergePer'])
				ReduceSlowstart = float(config_set['ReduceSlowstart'])
				inMenMergeThreshold = int(config_set['inMenMergeThreshold'])
				ShuffleInputPer = float(config_set['ShuffleInputPer'])
				ReduceInputPer = float(config_set['ReduceInputPer'])
				OutputCompress = lambda : 1 if config_set['OutputCompress']=='true' else 0
		
				JVMReuse = int(config_set['JVMReuse'])
	
				

			#	feature = [ReduceHeapSize,ReduceTasksMax,ReduceTasksNum,inMenMergeThreshold,ShuffleInputPer,ReduceInputPer,OutputCompress(),JVMReuse,job['data_size']]
				
				feature = [ReduceHeapSize,ReduceTasksMax,ReduceTasksNum,ShuffleMergePer,ReduceSlowstart,inMenMergeThreshold,ShuffleInputPer,ReduceInputPer,OutputCompress(),JVMReuse,job['data_size']]
			


				feature_list.append(feature)

		return feature_list



	def get_AllFeature_list(self):
		feature_list = []
		wordcount_coll = self.connect_DB()
		for job in wordcount_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
			
				config_set = job['config_set']
				MapHeapSize =self.HeapSizeFilter(config_set['MapHeapSize'])			
				MapTasksMax = int(config_set['MapTasksMax'])
				SplitSize = int(config_set['SplitSize'])/(2**20)
				SortMB = int(config_set['SortMB'])
				SortPer = float(config_set['SortPer'])
				RecordPer = float(config_set['RecordPer'])
	

				ReduceHeapSize = self.HeapSizeFilter(config_set['ReduceHeapSize'])
				ReduceTasksMax = int(config_set['ReduceTasksMax'])
				ReduceTasksNum = int(config_set['ReduceTasksNum'])
				ShuffleMergePer = float(config_set['ShuffleMergePer'])
				ReduceSlowstart = float(config_set['ReduceSlowstart'])
				inMenMergeThreshold = int(config_set['inMenMergeThreshold'])
				ShuffleInputPer = float(config_set['ShuffleInputPer'])
				ReduceInputPer = float(config_set['ReduceInputPer'])
				OutputCompress = lambda : 1 if config_set['OutputCompress']=='true' else 0
		
				JVMReuse = int(config_set['JVMReuse'])
			
				feature = [MapHeapSize,MapTasksMax,SplitSize,SortMB,SortPer,RecordPer,ReduceHeapSize,ReduceTasksMax,ReduceTasksNum,ShuffleMergePer,ReduceSlowstart,inMenMergeThreshold,ShuffleInputPer,ReduceInputPer,OutputCompress(),JVMReuse,job['data_size']]
			
				feature_list.append(feature)

		return feature_list


	def get_MapMean_list(self):
		terasort_coll = self.connect_DB()
		interData_list = []
		for job in terasort_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
				interData = job['map_exec_time']
				median = np.median(interData)
				interData_list.append(median)				

		return interData_list

	def get_RedMean_list(self):
		terasort_coll = self.connect_DB()
		interData_list = []
		for job in terasort_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
				interData = job['reduce_exec_time']
				median = np.median(interData)
				interData_list.append(median)				

		return interData_list


	def get_MapDev_list(self):
		terasort_coll = self.connect_DB()
		interData_list = []
		for job in terasort_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
				interData = job['map_exec_time']
				median = np.std(interData)
				interData_list.append(median)				

		return interData_list


	def get_RedDev_list(self):
		terasort_coll = self.connect_DB()
		interData_list = []
		for job in terasort_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
				interData = job['reduce_exec_time']
				median = np.std(interData)
				interData_list.append(median)				

		return interData_list




    	def get_target_list(self):
		target_list = []
		terasort_coll = self.connect_DB()
		for job in terasort_coll.find({"job_name":self.job_name}):
			if job['exec_time'] != -1:
				target_list.append(job['exec_time'])
	
		return target_list


	def get_error_rate(self,real_result,predict_result):

		sum_error_rate = 0
		effective_num = 0
		for i in range(len(real_result)):
			if real_result[i]!=-1:
				error_distance = math.fabs(real_result[i]-predict_result[i])
				effective_num = effective_num+1
				error_rate = error_distance/real_result[i]
				sum_error_rate = sum_error_rate+error_rate		
		avg_error_rate = sum_error_rate/effective_num
		return avg_error_rate

	






