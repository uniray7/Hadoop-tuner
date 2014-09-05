import random
import sys
import decimal as dec
import json
#MapHeapSize:mapred.map.child.java.opts
#MapTasksMax:mapred.tasktracker.map.tasks.max
#SplitSize:mapred.min.split.size
#SortMB:io.sort.mb
#SortPer:io.sort.spill.percent
#RecordPer:io.sort.record.percent
#JVMReuse:mapred.job.reuse.jvm.num.task

def random_generator(NumRecord):
	ran_file = open('random_file','w')
	for i in range(NumRecord):


		DataSize = random.randint(1,4)*10

		MapHeapSize = 200+(random.randint(0,8))*100
		MapTasksMax = random.randint(1,4)
		SplitSize = 64*(random.randint(1,4))
		SortMB = 100+100*(random.randint(0,(MapHeapSize/100)/2))
		SortPer = str(dec.Decimal(str(0.5+0.1*(random.randint(0,4)))))
		RecordPer = str(dec.Decimal(str(0.03+0.01*(random.randint(0,6)))))
		
		ReduceHeapSize =200+(random.randint(0,8))*100
		ReduceTasksMax = random.randint(1,4)
		ReduceTasksNum = random.randint(1,8)
		ShuffleMergePer = str(dec.Decimal(str(0.4+0.1*(random.randint(0,4)))))
		ReduceSlowstart = str(dec.Decimal(str(0.03+0.01*(random.randint(0,4)))))
		inMenMergeThreshold = 800+(random.randint(0,4))*100
		ShuffleInputPer = str(dec.Decimal(str(0.5+0.1*(random.randint(0,4)))))
		ReduceInputPer = str(dec.Decimal(str(0.5+0.1*(random.randint(0,4)))))  
		OutputCompress = lambda:"true" if random.randint(0,1)==0 else "false"
	
		
		JVMReuse = lambda:-1 if random.randint(-1,4)<0 else random.randrange(4,17,4)
	
		para_list = [DataSize,MapHeapSize,MapTasksMax,SplitSize,SortMB,SortPer,RecordPer,ReduceHeapSize,ReduceTasksMax,ReduceTasksNum,ShuffleMergePer,ReduceSlowstart,inMenMergeThreshold,ShuffleInputPer,ReduceInputPer,OutputCompress(),JVMReuse()]
		
		data_json = json.dumps(para_list)
		print>>ran_file, data_json
	

	ran_file.close()

random_generator(int(sys.argv[1]))
