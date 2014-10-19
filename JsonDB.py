#!/usr/bin/env python3
import sys, json, copy
if __name__ == "__main__":
	print("Python schemaless JSON/YAML database interface")
	print("Do not execute directly")
	sys.exit(1)
class DatabaseNotCreatedException:
	pass
class DatabaseWriteIOErrorException:
	pass
class TableDoesNotExistException:
	pass
class SaveDoesNotExistException:
	pass
class Database:
	def __init__(self, file = False, pretty = False):
		self.master = False
		self.fileObj = None
		self.init = False
		self.pretty = False
		self.saves = False
		self.triggers = False
		if file:
			self.OpenDatabase(file, pretty)
	def OpenDatabase(self, file, pretty = False):
		if self.init == True:
			self.Destroy()
		self.__init__()
		self.pretty = pretty
		try:
			if type(file) == str:
				self.fileObj = open(file,"r+")
			else:
				self.master = json.loads(file)
				self.fileObj = file
			self.fileObj.seek(0,0)
			self.master = json.loads(self.fileObj.read())[0]
			self.fileObj.seek(0,0)
			self.saves = json.loads(self.fileObj.read())[1]
			self.fileObj.seek(0,0)
			self.triggers = json.loads(self.fileObj.read())[2]
			self._write(True)
			#print "Done! Everything working correctly"
		except (IOError, ValueError):
			#print traceback.format_exc()
			#print "Error reading from file, creating new one"
			try:
				self.fileObj = open(file,"w")
				self.master = {}
				self.saves = {}
				self.triggers = []
				#print "Created new file successfully"
			except:
				#print "Failed to create new file"
				raise DatabaseWriteIOErrorException
		except:
			#print "Unknown error"
			#print traceback.format_exc()
			raise DatabaseWriteIOErrorException
			self.master = {}
			self.saves = {}
			self.triggers = []
			return
		self.init = True
	def Destroy(self):
		self._write()
		self.init = False
		self.fileObj.close()
		self.__init__()
	def _matches(self, z, args, kwargs):
		for x,y in kwargs.items():
			if z[x] != y:
				return False
		for a in args:
			if type(a) != type(lambda:True):
				raise TypeError
			if not a(z):
				return False
		return True
	def Create(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TableExists(table):
			self.Truncate(table)
		else:
			self.master[table] = []
	def Drop(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		del self.master[table]
	def TableExists(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if table in self.master:
			return True
		else:
			return False
	def Select(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		results = []
		for z in self.master[table]:
			if self._matches(z, args, kwargs):
				results.append(z)
		return results
	def Update(self, table, olddata, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		for x in olddata:
			for y,z in kwargs.items():
				self._runTrigger("BEFORE UPDATE",table,x)
				self.master[table][self.master[table].index(x)][y] = z
				self._runTrigger("AFTER UPDATE",table,x)
	def Delete(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for z in copy.deepcopy(self.master[table]): # We need a deep copy because we are iterating through it while deleting from it, so some values would get skipped
			if self._matches(z, args, kwargs):
				self._runTrigger("BEFORE DELETE",table,z)
				del self.master[table][self.master[table].index(z)]
				self._runTrigger("AFTER DELETE",table,z)
				results.append(z)
		return results
	def Dump(self, table = False):
		if not self.init:
			raise DatabaseNotCreatedException
		if table:
			if self.TableExists(table):
				return self.master[table]
			else:
				raise TableDoesNotExistException
		return self.master
	def Insert(self, table, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		for x,y in kwargs.items():
			self._runTrigger("BEFORE INSERT",table,dict(**kwargs))
		self.master[table].append(dict(**kwargs))
		for x,y in kwargs.items():
			self._runTrigger("AFTER INSERT",table,dict(**kwargs)) ### FIX THIS
		self._write()
	def Truncate(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TableExists(table):
			self.master[table] = []
		else:
			raise TableDoesNotExistException
		self._write()
	def ListTables(self):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for x, y in self.master.items():
			results.append(x)
		return results
	def Vacuum(self):
		self._write()
	def _write(self, override = False):
		if not self.init:
			if override == False:
				raise DatabaseNotCreatedException
		try:
			self.fileObj.seek(0,0)
			self.fileObj.truncate()
			if self.pretty:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers], indent = self.pretty))
			else:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers]))
			self.fileObj.flush()
		except IOError:
			#print traceback.format_exc()
			raise DatabaseWriteIOErrorException
	def SaveExists(self,name):
		if not self.init:
			raise DatabaseNotCreatedException
		name = str(name)
		return name in self.saves
	def Save(self,name,table = False):
		if not self.init:
			raise DatabaseNotCreatedException
		name = str(name)
		self.saves[name] = {}
		if table:
			self.saves[name][table] = copy.deepcopy(self.master[table])
		else:
			self.saves[name] = copy.deepcopy(self.master)
	def RemoveSave(self,name):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.SaveExists(name):
			raise SaveDoesNotExistException
		del self.saves[str(name)]
	def Load(self,name,table = False):
		if not self.init:
			raise DatabaseNotCreatedException
		name = str(name)
		if self.SaveExists(name):
			if table:
				if not self.TableExists(table):
					raise TableDoesNotExistException
				self.master[table] = copy.deepcopy(self.saves[name][table])
			else:
				for table in self.ListTables():
					if table in self.saves[name]:
						self.master[table] = copy.deepcopy(self.saves[name][table])
		else:
			raise SaveDoesNotExistException
		self._write()
	def GetSave(self,name = False):
		if not self.init:
			raise DatabaseNotCreatedException
		if name:
			name = str(name)
			return self.saves[name]
		else:
			return self.saves
	def ListSaves(self):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for x,y in self.saves.items():
			results.append(x)
		return results
	def PutSave(self, data, name = False):
		if not self.init:
			raise DatabaseNotCreatedException
		if type(data) != dict:
			raise TypeError
		if name:
			name = str(name)
			self.saves[name] = data
		else:
			self.saves = data
	def _runTrigger(self,type,table,datapoint):
		for x in self.triggers:
			if table == x[0] and type == x[1]:
				x[2](datapoint)
	def AddTrigger(self,name,type,table,action):
		if not type in ["BEFORE INSERT","AFTER INSERT","BEFORE DELETE","AFTER DELETE","BEFORE UPDATE","AFTER UPDATE"]:
			raise NotImplementedError
		if not self.init:
			raise DatabaseNotCreatedException
		self.triggers.append([type,table,action,name])
		self._write()
	def RemoveTrigger(self,name):
		for x in self.triggers:
			if x[4] == name:
				del self.triggers[x]
		self._write()