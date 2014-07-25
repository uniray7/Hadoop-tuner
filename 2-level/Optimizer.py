import math
import random
import Testing
import numpy as np
import json
import sys

class sample:
 	def __init__(self,config_json,constant_DataSize):
		self.Scaling = 1
		
		json_data = open(config_json) 
		data = json.load(json_data)
		self.sample_standard_list = data["sample_standard_list"]
		self.DataSize = constant_DataSize


	def change_scaling(self,changed_scaling):
		self.Scaling = changed_scaling

	
	
	def get_randSample(self): #sample_standard_list = [[high_bound','low_bound','interval'],[]...]
		x_sample = []
		for sample_standard in self.sample_standard_list:
			if sample_standard['parameter']=='DataSize':
				x_sample.append(int(self.DataSize))
			
#===================================limit=========================================
#=================================================================================



			elif sample_standard['type'] == 'bool':#for true & false
				x = lambda: 1 if random.randint(0,1)==0 else 0
				x_sample.append(x())

			elif sample_standard['type'] == 'reuse':#for JVMReuse
				x = lambda: -1 if random.randint(sample_standard['low-bound'],int(sample_standard['high-bound']/sample_standard['interval']))<0 else random.randrange(4,sample_standard['high-bound'],sample_standard['interval'])
				x_sample.append(x())

			elif sample_standard['type'] == 'int':#for random int
				range =	sample_standard['high-bound']-sample_standard['low-bound']
				SC =((1-self.Scaling)/2)+self.Scaling
				high_bound = sample_standard['high-bound']*SC
				low_bound = sample_standard['high-bound']-range*SC	
				x = random.randint(int(low_bound/sample_standard['interval']),int(high_bound/sample_standard['interval']))*sample_standard['interval']

#================================limit===========================================
#================================================================================

				x_sample.append(x)

			elif  sample_standard['type'] == 'float': #for random float
				range =	sample_standard['high-bound']-sample_standard['low-bound']
				
				SC =((1-self.Scaling)/2)+self.Scaling
				high_bound = sample_standard['high-bound']*SC
				low_bound = sample_standard['high-bound']-range*SC	
				x = random.randint(int(low_bound/sample_standard['interval']),int(high_bound/sample_standard['interval']))*sample_standard['interval']

#=======================================limit=====================================
				if sample_standard['parameter']=='RecordPer':
					x=0.05
				if sample_standard['parameter']=='ShuffleInputPer':
					x=0.7

#=================================================================================
				x_sample.append(x)

		return x_sample

def Optimizer(DataSize):
# Initialize exploration parameter
	p = 0.99
	r = 0.1
	n = int((math.log(1-p))/(math.log(1-r)))
#Intialize exploitation parameter
	q = 0.93
	v = 0.1
	l =int((math.log(1-q))/(math.log(1-v)))
	c = 0.1
	s_t = 0.0000001
	print 'start'	
	sample_ins = sample('config.json',DataSize)
	#Take n Samples in parameter space D
	for i in range(n):
		x = sample_ins.get_randSample()
		#Find the x0 <- argmin(f(xi))
		if i == 0:
			y_r = objective_func(x)
			x0 = x
		else:
			y = objective_func(x)
			if y<y_r:
				y_r = y
				x0 = x
	
	print y_r
	print x0

	sample_set = []
	#Add f(x0) in threshold_set F
	threshold_set = []
	threshold_set.append(y_r)

	#Initialize "exploit_flag" and start exploitation
	i = 0
	exploit_flag = True
	x_opt = x0

	#While stopping criterian is no satisfied
	while i<50 : 
	
		if exploit_flag == True:
			print 'start exploitation'
			#Exploit_flag is set, starting exploitation process
			j = 0
			f_c = objective_func(x0)
			x_l = x0
			rho = r # rho is just like 'p'
			
			while rho > s_t:
				#Take a random sample x' from ND,rho(x_l) 
				n = float(n)
				scope = (math.pow(rho,float(1/n)))
				sample_ins.change_scaling(scope)
				x_plun = sample_ins.get_randSample()
				if objective_func(x_plun) < f_c:
					#find a better point, re-align the center of sample space to the new point
					x_l = x_plun
					f_c = objective_func(x_plun)
					j=0
				else:
					j = j+1

				
				if j == l:
					#fail to find a better point, start shrink
					#print 'start shrink'	
					rho = c*rho
					j = 0

			#end "while rho> s_t"	
			exploit_flag = False
			if objective_func(x_l) < objective_func(x_opt):
				x_opt = x_l

		#end "if exploit_flag == True"
		
		#Take random sample x_0 from S
		#Exploitation restart testing
		sample_ins.change_scaling(1)
		x0 = sample_ins.get_randSample()
		y_of_x0 = objective_func(x0)
		item = {'X':x0,'Y':y_of_x0}
		sample_set.append(item)
		
		if y_of_x0 < y_r:
			exploit_flag = True
			print 'success'
				
		if i == n:
			x1 = sample_set[0]['X']
			y1 = sample_set[0]['Y']
			for item_instance in sample_set:
				if item_instance['Y']<y1:
					x1 = item_instance['X']
					y1 = item_instance['Y']
			threshold_set.append(y1)
			sample_set = []
			y_r = sum(threshold_set)/len(threshold_set)
			i = 0
		if len(threshold_set)>30:
			break
		i = i+1
		print "counter: "+str(i)
		print "y_r: "+str(y_r)
		print "length of threshold: "+str(len(threshold_set))
		print "optimal y: "+str(objective_func(x_opt))
		print "x: "+str(x_opt )+"\n"

def objective_func(x):
	x_map = x[1:7]
	x_map.append(x[-1])
	x_map.append(x[0])
	
	x_reduce = x[7:]
	x_reduce.append(x[0])

	X_map = []
	X_reduce = []
	X_map.append(x_map)
	X_reduce.append(x_reduce)
	return int(Testing.test(X_map,X_reduce))


Optimizer(sys.argv[1])
