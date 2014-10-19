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
		if file:
			self.OpenDatabase(file, pretty)
		self.saves = {}
	def OpenDatabase(self, file, pretty = False):
		if self.init == True:
			self.Destroy()
		self.__init__()
		self.master = {}
		self.pretty = pretty
		try:
			if type(file) == str:
				self.fileObj = open(file,"r+")
				self.master = json.loads(self.fileObj.read())
			else:
				self.master = json.loads(file)
				self.fileObj = file
			self._write(True)
			#print "Done! Everything working correctly"
		except (IOError, ValueError):
			#print "Error reading from file, creating new one"
			try:
				self.fileObj = open(file,"w")
				#print "Created new file successfully"
			except:
				#print "Failed to create new file"
				raise DatabaseWriteIOErrorException
		except:
			#print "Unknown error"
			#print traceback.format_exc()
			raise DatabaseWriteIOErrorException
			self.master = {}
			return
		self.init = True
		self.saves = {}
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
				self.master[table][self.master[table].index(x)][y] = z
	def Delete(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException
		results = []
		for z in self.master[table]:
			if self._matches(z, args, kwargs):
				self.master.remove(z)
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
		self.master[table].append(dict(**kwargs))
		self._write()
	def Truncate(self, table):
		if not self.init:
			raise DatabaseNotCreatedException
		if self.TableExists(table):
			self.master[table] = []
		else:
			raise TableDoesNotExistException
		self._write
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
				self.fileObj.write(json.dumps(self.master, indent = self.pretty))
			else:
				self.fileObj.write(json.dumps(self.master))
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
		