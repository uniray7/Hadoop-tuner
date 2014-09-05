import commands
import linecache
import json
import os
import sys
import MongoConn

EXP = '/home/trend-hadoop/expr'
Hadoop = '/home/trend-hadoop/hadoop-1.2.1'
Des_path =EXP+'/implementation/v1/logs.json'
Rumen_libs = Hadoop+'/hadoop-tools-1.2.1.jar:'+Hadoop+'/hadoop-core-1.2.1.jar:'+Hadoop+'/lib/*'
RawDB = 'terasort'
RefineDB = 'terasort_refine'


dbconn = MongoConn.DbConn()  
conn = None



class Log:
	def __init__(self,log_path,serial_num):
		self.log_path = log_path
		self.current_path = os.getcwd()
		self.log_json_dir_path = self.current_path+'/logs.json'
		self.serial_num = serial_num

		cmd = "ls "+self.log_path+"/history | grep conf.xml | sed 's/_conf.xml//g'"
		self.job_id = commands.getoutput(cmd)




	def GetConfigSet(self):
		return linecache.getline(current_path+'/random_file',int(self.serial_num)+1)
	
	def GetExecTime(self,raw_log_json):
		finishTime = -1	
		for other_task in raw_log_json['otherTasks']:
			if other_task["taskType"] == "SETUP":
				#	print "start"	
				startTime=other_task["startTime"]
				#	print startTime
			if other_task["taskType"] == "CLEANUP":
				#        print "finish"
				finishTime=other_task["finishTime"]
				#	print finishTime

		if finishTime==-1:
			exec_time = -1
		else:	
			exec_time = finishTime-startTime

		return exec_time

	def GetMapExecTime(self,raw_log_json):
		map_execTime_list = []
		for map_task in raw_log_json['mapTasks']:
			if map_task['taskStatus'] == 'SUCCESS':
				map_execTime = map_task['finishTime']-map_task['startTime']
				map_execTime_list.append(map_execTime)
		return map_execTime_list

	def GetRedExecTime(self,raw_log_json):
		reduce_execTime_list = []
		for reduce_task in raw_log_json['reduceTasks']:
			if reduce_task['taskStatus'] == 'SUCCESS':
				reduce_execTime = reduce_task['finishTime']-reduce_task['startTime']
				reduce_execTime_list.append(reduce_execTime)
		return reduce_execTime_list

	def RawtoJson(self):
		cmd = 'java -cp '+Rumen_libs+' org.apache.hadoop.tools.rumen.TraceBuilder file://'+self.log_json_dir_path+'/out_'+self.job_id+'.json file://'+self.log_json_dir_path+'/topo_'+self.job_id+' file://'+log_path+'/history'
		os.system(cmd)


	def RawJsonSaveInDB(self):

		cmd = 'mongoimport --jsonArray --db big --collectio '+RawDB+' --file '+self.log_json_dir_path+'/out_'+self.job_id+'.json'
		os.system(cmd)





	def RefineRawJson(self):
		raw_log_json_fstream = open(self.log_json_dir_path+'/out_'+self.job_id+'.json')
		raw_log_json = json.load(raw_log_json_fstream)
		MapHeapSize = 'mapred.map.child.java.opts'
		MapTasksMax = 'mapred.tasktracker.map.tasks.maximum'
		SplitSize = 'mapred.min.split.size'
		SortMB = 'io.sort.mb'
		SortPer = 'io.sort.spill.percent'
		RecordPer = 'io.sort.record.percent'

		ReduceHeapSize = 'mapred.reduce.child.java.opts'
		ReduceTasksMax = 'mapred.tasktracker.reduce.tasks.maximum'
		ReduceTasksNum = 'mapred.reduce.tasks'
		ShuffleMergePer = 'mapred.job.shuffle.merge.percent'
		ReduceSlowstart = 'mapred.reduce.slowstart.completed.maps'
		inMenMergeThreshold = 'mapred.inmem.merge.threshold'
		ShuffleInputPer = 'mapred.job.shuffle.input.buffer.percent'
		ReduceInputPer = 'mapred.job.reduce.input.buffer.percent'
		OutputCompress = 'mapred.output.compress'

		JVMReuse = 'mapred.job.reuse.jvm.num.tasks'

		random_file = self.GetConfigSet()
		JobProperty = raw_log_json['jobProperties']

		refine_config_set = {'MapHeapSize':JobProperty[MapHeapSize],'MapTasksMax':JobProperty[MapTasksMax],'SplitSize':JobProperty[SplitSize],'SortMB':JobProperty[SortMB],'SortPer':JobProperty[SortPer],'RecordPer':JobProperty[RecordPer],'ReduceHeapSize':JobProperty[ReduceHeapSize],'ReduceTasksMax':JobProperty[ReduceTasksMax],'ReduceTasksNum':JobProperty[ReduceTasksNum],'ShuffleMergePer':JobProperty[ShuffleMergePer],'ReduceSlowstart':JobProperty[ReduceSlowstart],'inMenMergeThreshold':JobProperty[inMenMergeThreshold],'ShuffleInputPer':JobProperty[ShuffleInputPer],'ReduceInputPer':JobProperty[ReduceInputPer],'OutputCompress':JobProperty[OutputCompress],'JVMReuse':JobProperty[JVMReuse]}
		


		exec_time = self.GetExecTime(raw_log_json)				

		map_execTime_list = self.GetMapExecTime(raw_log_json)
		reduce_execTime_list = self.GetRedExecTime(raw_log_json)



		
		refine_log_data = {'job_name':raw_log_json['jobName'],'job_id':self.job_id,'config_set':refine_config_set,'data_size':eval(random_file)[0],'exec_time':exec_time,'map_exec_time':map_execTime_list,'reduce_exec_time':reduce_execTime_list}
		print refine_log_data
		raw_log_json_fstream.close()	
		return refine_log_data





	def RefineJsonSaveInDB(self):
		
		dbconn.connect()
		global conn
		conn = dbconn.getConn()

		BigDB = conn.big
		wordcount_model = BigDB.wordcount_model
		

		refine_log_data = self.RefineRawJson()
		print refine_log_data

		insert_id =wordcount_model.insert(refine_log_data)

	


def ListDir(path):
	return os.listdir(path)



log_list = ListDir(sys.argv[1])
current_path = os.getcwd();

for log_dir in log_list:	
	log_path = current_path+'/logs/'+log_dir
	serial_num = int(log_dir[log_dir.find('_')+1:])
	new_log = Log(log_path,serial_num)
	new_log.RawtoJson()
	new_log.RefineRawJson()
	print '\n'
#	new_log.RefineJsonSaveInDB()	
