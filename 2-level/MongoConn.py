import pymongo

class DbConn:
	conn = None
	server = "mongodb://localhost:27017"
	
	def connect(self):
		self.conn = pymongo.Connection(self.server)
	def close(self):
		return self.conn.disconnect()
	def getConn(self):
		return self.conn


