#Parameters:


#MapHeapSize:mapred.map.child.java.opts
#MapTasksMax:mapred.tasktracker.map.tasks.max
#SplitSize:mapred.min.split.size
#SortMB:io.sort.mb
#SortPer:io.sort.spill.percent
#RecordPer:io.sort.record.percent
#JVMReuse:mapred.job.reuse.jvm.num.task

import os
import math
import json
import commands

EXP = "/home/trend-hadoop/expr"
Heap1 = "-Xmx"
Heap2 = "M"

MapHeapSize = ' mapred.map.child.java.opts '
MapTasksMax = ' mapred.tasktracker.map.tasks.maximum '
SplitSize = ' mapred.min.split.size '
SortMB = ' io.sort.mb '
SortPer = ' io.sort.spill.percent '
RecordPer = ' io.sort.record.percent '

ReduceHeapSize = ' mapred.reduce.child.java.opts '
ReduceTasksMax = ' mapred.tasktracker.reduce.tasks.maximum '
ReduceTasksNum = ' mapred.reduce.tasks '
ShuffleMergePer = ' mapred.job.shuffle.merge.percent '
ReduceSlowstart = ' mapred.reduce.slowstart.completed.maps '
inMenMergeThreshold = ' mapred.inmem.merge.threshold '
ShuffleInputPer = ' mapred.job.shuffle.input.buffer.percent '
ReduceInputPer = ' mapred.job.reduce.input.buffer.percent '
OutputCompress = ' mapred.output.compress '

JVMReuse = ' mapred.job.reuse.jvm.num.tasks '




def run_job(para_list,num):
		
	cmd = 'cp -r '+EXP+'/conf/conf_default '+EXP+'/conf/conf_new'
	os.system(cmd)
	


	DataSize = para_list[0]
	MapHeapValue = Heap1+str(para_list[1])+Heap2
	ReduceHeapValue = Heap1+str(para_list[7])+Heap2
	SplitValue = para_list[3]*(2**20)
	

	cmd = EXP+'/tinyxml/revise_conf_v1'+MapHeapSize+MapHeapValue+MapTasksMax+str(para_list[2])+SplitSize+str(SplitValue)+SortMB+str(para_list[4])+SortPer+str(para_list[5])+RecordPer+str(para_list[6])+ReduceHeapSize+ReduceHeapValue+ReduceTasksMax+str(para_list[8])+ReduceTasksNum+str(para_list[9])+ShuffleMergePer+str(para_list[10])+ReduceSlowstart+str(para_list[11])+inMenMergeThreshold+str(para_list[12])+ShuffleInputPer+str(para_list[13])+ReduceInputPer+str(para_list[14])+OutputCompress+str(para_list[15])+JVMReuse+str(para_list[16])
	os.system(cmd)

	
	cur_Path = lambda: commands.getoutput('pwd')

	cmd = 'hadoop --config '+EXP+'/conf/conf_new jar '+EXP+'/../hadoop-1*/hadoop-examples* terasort /tera-in'+str(DataSize)+'G /tera-out'
	os.system(cmd)

					
	cmd = 'hadoop fs -get /tera-out/_logs '+cur_Path()+'/logs/eval_'+str(num+100)
	os.system(cmd)	


	cmd = 'hadoop fs -rmr /tera-out'
	os.system(cmd)
	
		
	cmd = 'rm -rf '+EXP+'/conf/conf_new'
	os.system(cmd)
	





ran_file = open('eval_2','r')
num=0
for para_str in ran_file:
		
	data_list = json.loads(para_str)
	run_job(data_list,num)
	print para_str
	num=num+1

ran_file.close()
