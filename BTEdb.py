#!/usr/bin/env python3
import sys, json, copy, dill, base64, os, itertools
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
	def __init__(self, filename = False, pretty = False):
		self.master = False
		self.fileObj = None
		self.init = False
		self.pretty = False
		self.saves = False
		self.triggers = False
		self.TransactionInProgress = False
		if filename:
			self.OpenDatabase(filename, pretty)
	def __str__(self):
		return str(self.master)
	def __repr__(self):
		temp="<BTEdb Database object. Initialized: "
		if self.init:
			temp += "True, file: "
		else:
			temp += "False file: "
		temp += str(fileObj)
		return temp + ">"
	def OpenDatabase(self, filename, pretty = False):
		if self.init == True:
			self.Destroy()
		self.__init__()
		self.pretty = pretty
		try:
			if type(filename) == str:
				self.fileObj = open(filename,"r+",encoding="utf8")
			else:
				self.master = json.loads(filename.read())
				self.fileObj = filename
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
			#print "Error reading from filename, creating new one"
			try:
				self.fileObj = open(filename,"w")
				self.master = {}
				self.saves = {}
				self.triggers = []
				#print "Created new filename successfully"
			except:
				#print "Failed to create new filename"
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
		self._write(True)
		self.init = False
		self.fileObj.close()
		self.__init__()
	def _matches(self, z, args, kwargs):
		for x,y in kwargs.items():
			if z[x] != y:
				return False
		for a in args:
			if type(a) != type(lambda:True):
				raise triggertypeError
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
	def CreateTable(self,name):
		self.Create(name)
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
	def Update(self, table, olddata, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		for x in olddata:
			self._runTrigger("BEFORE UPDATE",table,x)
			for y,z in kwargs.items():
				self.master[table][self.master[table].index(x)][y] = z
			for arg in args:
				self.master[table][self.master[table].index(x)][arg[0]] = arg[1]
			temp = {}
			for arg in args:
				x[arg[0]] = arg[1]
			self._runTrigger("AFTER UPDATE",table,self.master[table][self.master[table].index(x)])
		self._write()
	def Delete(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for z in copy.deepcopy(self.master[table]): # We need a deep copy because we are iterating through it while deleting from it, so every other value would get skipped
			if self._matches(z, args, kwargs):
				self._runTrigger("BEFORE DELETE",table,z)
				del self.master[table][self.master[table].index(z)] # You can't just use z because that deletes it from the copy
				self._runTrigger("AFTER DELETE",table,z)
				results.append(z)
		self._write()
		return results
	def Dump(self, table = False): 
		if not self.init:
			raise DatabaseNotCreatedException
		if table: # If we were passed a table
			if self.TableExists(table): # If the table exists
				return self.master[table] # Return it
			else:
				raise TableDoesNotExistException
		return self.master # If we were not passed a table, dump all the tables.
	def Insert(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		temp = {} # Empty dictionary
		for arg in args:
			temp[arg[0]] = arg[1] # Turns a list of lists, each with two items, into the dictionary temp
		self._runTrigger("BEFORE INSERT",table,dict(itertools.chain(kwargs.items(), temp.items()))) # the dict() thing basically just combines kwargs with temp
		self.master[table].append(dict(itertools.chain(kwargs.items(), temp.items()))) # This is what actually inserts the datapoint
		self._runTrigger("AFTER INSERT",table,dict(itertools.chain(kwargs.items(), temp.items())))
		self._write()
	def Truncate(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TableExists(table): # If the table exists, reinitialize it to an empty list
			self.master[table] = []
		else:
			raise TableDoesNotExistException
		self._write()
	def ListTables(self):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for x, y in self.master.items(): # Master is a dictionary, and we want a list of the keys. 
			results.append(x) # This is the best way I know to do it
		return results
	def Vacuum(self):
		self._write(True) # This is actually not useless, I promise
	def BeginTransaction(self,makeSave = True):
		self.TransactionInProgress = True # Figure it out yourself
		if makeSave:
			self.Save("transaction") # This isn't required for everything, for example if you just want to have an insert statement in a for loop and only write
			# 						   at the beginning and end of the loop, you would obviously want it to not write out to the disk after each insert statement, so
			#						   you start a transaction, but you don't want to waste the extra ram because you know you aren't going to restore from this
	def CommitTransaction(self):
		self.TransactionInProgress = False
		if self.SaveExists("transaction"): # Self-documenting code
			self.RemoveSave("transaction") # You should learn it sometime
		self._write()
	def RevertTransaction(self):
		if self.SaveExists("transaction"):
			self.Revert("transaction")
			self.RemoveSave("transaction")
		self.TransactionInProgress = False
		self._write()
	def _write(self, override = False):
		if not self.init and not override:
			raise DatabaseNotCreatedException
		if self.TransactionInProgress and not override:
			return
		try:
			self.fileObj.seek(0,0)
			if self.pretty:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers], indent = self.pretty))
			else:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers]))
			if self.fileObj.flush():
				os.fsync(self.fileObj.fileno())
			self.fileObj.truncate()
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
			raise triggertypeError
		if name:
			name = str(name)
			self.saves[name] = data
		else:
			self.saves = data
		self._write()
	def _runTrigger(self,triggertype,table,datapoint):
		for x in self.triggers:
			if table == x[0] and triggertype == x[1]:
				#print("Calling function " + str(x[3]) + " on action " + x[1])
				dill.loads(base64.b64decode(x[2]))(self,datapoint,table,triggertype)
	def AddTrigger(self,name,triggertype,table,action):
		if not triggertype in ["BEFORE INSERT","AFTER INSERT","BEFORE DELETE","AFTER DELETE","BEFORE UPDATE","AFTER UPDATE"]:
			raise NotImplementedError
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TriggerExists(name):
			raise DuplicateTriggerNameExistsException
		if not self.TableExists(table):
			raise TableDoesNotExistException
		self.triggers.append([table,triggertype,base64.b64encode(dill.dumps(action)).decode("utf-8","replace"),name])
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
