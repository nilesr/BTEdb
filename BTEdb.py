#!/usr/bin/env python3
import sys, json, copy, dill, base64
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
class SavepointDoesNotExistException:
	pass
class TriggerDoesNotExistException:
	pass
class DuplicateTriggerNameExistsException:
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
		self._write()
	def Drop(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		del self.master[table]
		self._write()
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
			self._runTrigger("BEFORE UPDATE",table,x)
			for y,z in kwargs.items():
				self.master[table][self.master[table].index(x)][y] = z
			self._runTrigger("AFTER UPDATE",table,self.master[table][self.master[table].index(x)])
		self._write()
	def Delete(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for z in copy.deepcopy(self.master[table]): # We need a deep copy because we are iterating through it while deleting from it, so every other value would get skipped
			if self._matches(z, args, kwargs):
				self._runTrigger("BEFORE DELETE",table,z)
				del self.master[table][self.master[table].index(z)]
				self._runTrigger("AFTER DELETE",table,z)
				results.append(z)
		self._write()
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
		self._runTrigger("BEFORE INSERT",table,dict(**kwargs))
		self.master[table].append(dict(**kwargs))
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
		self._write()
	def RemoveSave(self,name):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.SaveExists(name):
			raise SavepointDoesNotExistException
		del self.saves[str(name)]
	def Revert(self,name,table = False):
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
			raise SavepointDoesNotExistException
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
		self._write()
	def _runTrigger(self,type,table,datapoint):
		for x in self.triggers:
			if table == x[0] and type == x[1]:
				#print("Calling function " + str(x[3]) + " on action " + x[1])
				dill.loads(base64.b64decode(x[2]))(self,datapoint,table,type)
	def AddTrigger(self,name,type,table,action):
		if not type in ["BEFORE INSERT","AFTER INSERT","BEFORE DELETE","AFTER DELETE","BEFORE UPDATE","AFTER UPDATE"]:
			raise NotImplementedError
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TriggerExists(name):
			raise DuplicateTriggerNameExistsException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		self.triggers.append([table,type,base64.b64encode(dill.dumps(action)).decode("utf-8","replace"),name])
		self._write()
	def RemoveTrigger(self,name):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TriggerExists(name):
			raise TriggerDoesNotExistException
		for x in self.triggers:
			if x[3] == name:
				del self.triggers[self.triggers.index(x)]
				break
		self._write()
	def ListTriggers(self):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for x in self.triggers:
			results.append([x[3],x[1],x[0]])
		return results
	def TriggerExists(self,name):
		if not self.init:
			raise DatabaseNotCreatedException
		for x in self.triggers:
			if x[3] == name:
				return True
		return False