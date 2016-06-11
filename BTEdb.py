#!/usr/bin/env python3
import sys, json, copy, dill, base64, os, itertools
if __name__ == "__main__":
	print("Python schemaless JSON/YAML database interface")
	print("Do not execute directly")
	sys.exit(1)
# Define exceptions
class DatabaseNotCreatedException(BaseException):
	pass
class DatabaseWriteIOErrorException(BaseException):
	pass
class TableDoesNotExistException(BaseException):
	pass
class SavepointDoesNotExistException(BaseException):
	pass
class TriggerDoesNotExistException(BaseException):
	pass
class DuplicateTriggerNameExistsException(BaseException):
	pass
class NoTransactionInProgressException(BaseException):
	pass
class TransactionNotRevertableException(BaseException):
	pass
# Database class
class Database:
	def __init__(self, filename = False, pretty = False): # Init function. 
		# Set initial values
		self.master = False
		self.fileObj = None
		self.init = False
		self.pretty = False
		self.saves = False
		self.triggers = False
		self.TransactionInProgress = False
		if filename: # If a filename was specified
			self.OpenDatabase(filename, pretty) # Open the file
	def __str__(self):
		return str(self.master) # This is useless
	def __repr__(self): # This too
		return "<BTEdb Database object. Initialized: " + str(self.init) + ", file: " + str(fileObj) + ">"
	def OpenDatabase(self, filename, pretty = False): # Open database file
		if self.init == True: # If we are alredy initiated
			self.Destroy() # Destroy
		self.__init__() # Reset everything to the default.
		self.pretty = pretty # Set "pretty" as an instance variable
		try:
			if type(filename) == str: # If we're being called on a string
				self.fileObj = open(filename,"r+", os.O_NONBLOCK, encoding="utf8") # set fileobj to open
			else: # otherwise
				self.master = json.loads(filename.read()) # assume it's an object and try to read from it
				self.fileObj = filename
			self.fileObj.seek(0,0) # This might be able to be improved, but whatever
			self.master, self.saves, self.triggers = json.loads(self.fileObj.read())
			self._write(True) # We haven't yet set self.init to true, so we must specify override
			#print "Done! Everything working correctly"
		except (IOError, ValueError):
			#print traceback.format_exc()
			#print "Error reading from filename, creating new one"
			try:
				self.fileObj = open(filename,"w", os.O_NONBLOCK)
				self.master = {}
				self.saves = {}
				self.triggers = []
				#print "Created new filename successfully"
			except:
				#print "Failed to create new filename"
				raise DatabaseWriteIOErrorException()
		except:
			#print "Unknown error"
			#print traceback.format_exc()
			raise DatabaseWriteIOErrorException()
			self.master = {}
			self.saves = {}
			self.triggers = []
		self.init = True
	def Destroy(self): # Destroy function
		self._write(True) # Write out one last time
		self.init = False # Class is no longer in session
		self.fileObj.close() # Attempt to close the file descriptor.
		self.__init__() # Reset everything to default again
	def _matches(self, z, args, kwargs): # Internal function to test if something matches.
		for x,y in kwargs.items():
			# In the iteration, as soon as we know a single condition doesn't match, return false
			try:
				if z[x] != y:
					return False
			except KeyError:
				return False
		for a in args: # Iterate over the list of callables passed to the parent function
			if type(a) != type(lambda:True): # If it's not a callable, that's a serious problem
				raise TypeError
			if not a(z): # Call the callable with the datapoint
				return False
		return True # Every condition must match in order to get to this point
	def Create(self, table): # Create table function
		if not self.init:
			raise DatabaseNotCreatedException()
		if self.TableExists(table): # If the table exists
			self.Truncate(table) # Truncate it
		else: # Else
			self.master[table] = [] # Make it an empty list
		self._write() # Filesystem write
	def CreateTable(self,name): # CreateTable is an alias to Create
		self.Create(name)
	def Drop(self, table): # Drop table function
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.TableExists(table):
			raise TableDoesNotExistException()
		del self.master[table] # Delete it
		self._write() # Filesystem write
	def TableExists(self, table): # Test if a table exists
		if not self.init:
			raise DatabaseNotCreatedException()
		if table in self.master: # If the table exists
			return True # Return True
		else:
			return False
	def Select(self, table, *args, **kwargs): # Select data
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.TableExists(table):
			raise TableDoesNotExistException()
		results = [] # Results is an empty list
		for z in self.master[table]: # For each datapoint in the table
			if self._matches(z, args, kwargs): # If the datapoint matches the arguments passed
				results.append(z) # Add it to the results list
		return results # Return the results list
	def Update(self, table, olddata, *args, **kwargs): # Update data
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.TableExists(table):
			raise TableDoesNotExistException()
		for x in olddata: # For each datapoint passed
			self._runTrigger("BEFORE UPDATE",table,x) # Run the trigger on that datapoint
			idx = self.master[table].index(x) # Needed because after the first change, .index(x) will no longer return the correct index
			for y,z in kwargs.items(): # For each key,value in the dictionary kwargs (passed like "UID = 12")
				self.master[table][idx][y] = z # set key = value in the datapoint in the table
			for arg in args: # For each argument in the list of non-keyword arguments. 
				self.master[table][idx][arg[0]] = arg[1] # Assume it's a list/indexable and set the first item equal to the second item in the datapoint
			self._runTrigger("AFTER UPDATE",table,self.master[table][idx]) # Run trigger
		self._write() # Filesystem write
	def Delete(self, table, *args, **kwargs): # Delete function
		if not self.init:
			raise DatabaseNotCreatedException()
		results = [] # Results is an empty list
		for z in copy.deepcopy(self.master[table]): # We need a deep copy because we are iterating through it while deleting from it, so every other value would get skipped
			# For each datapoint in the table
			if self._matches(z, args, kwargs): # If it matches the passed arguments
				self._runTrigger("BEFORE DELETE",table,z) # Run a trigger
				del self.master[table][self.master[table].index(z)] # You can't just use z because that deletes it from the copy
				self._runTrigger("AFTER DELETE",table,z) # Run another trigger
				results.append(z) # Append the deleted datapoint to the results list
		self._write() # Filesystem write
		return results # Return the results
	def Dump(self, table = False): 
		if not self.init:
			raise DatabaseNotCreatedException()
		if table: # If we were passed a table
			if self.TableExists(table): # If the table exists
				return self.master[table] # Return it
			else:
				raise TableDoesNotExistException()
		return self.master # If we were not passed a table, dump all the tables.
	def Insert(self, table, *args, **kwargs):
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.TableExists(table):
			raise TableDoesNotExistException()
		for arg in args:
			kwargs[arg[0]] = arg[1]
		self._runTrigger("BEFORE INSERT",table,kwargs)
		self.master[table].append(kwargs) # This is what actually inserts the datapoint
		self._runTrigger("AFTER INSERT",table,kwargs)
		self._write()
	def Truncate(self, table):
		if not self.init:
			raise DatabaseNotCreatedException()
		if self.TableExists(table): # If the table exists, reinitialize it to an empty list
			self.master[table] = []
		else:
			raise TableDoesNotExistException()
		self._write()
	def ListTables(self):
		if not self.init:
			raise DatabaseNotCreatedException()
		return [x for x, y in self.master.items()]
	def Vacuum(self):
		self._write(True) # This is actually not useless, I promise
	def BeginTransaction(self,makeSave = True):
		self.TransactionInProgress = True # Figure it out yourself
		if makeSave:
			self.Save("transaction")
			# This isn't required for everything, for example if you just want to have an insert statement in a for loop and only write
			# at the beginning and end of the loop, you would obviously want it to not write out to the disk after each insert statement, so
			# you start a transaction, but you don't want to waste the extra ram because you know you aren't going to restore from this
		else:
			if self.SaveExists("transaction"):
				self.RemoveSave("transaction") # If we don't do this, it will reset to the old save if you do RevertTransaction
	def CommitTransaction(self):
		if not self.TransactionInProgress:
			raise NoTransactionInProgressException()
		self.TransactionInProgress = False
		if self.SaveExists("transaction"):
			self.RemoveSave("transaction")
		self._write()
	def RevertTransaction(self):
		if not self.TransactionInProgress:
			raise NoTransactionInProgressException()
		if self.SaveExists("transaction"): # If the transaction save exists
			self.Revert("transaction") #	 Revert to it
			self.RemoveSave("transaction") # Delete it
		else:
			raise TransactionNotRevertableException()
		self.TransactionInProgress = False # No longer in a transaction
		self._write() # 					 Write to disk
	def _write(self, override = False): # Write to disk function
		if not self.init and not override:
			raise DatabaseNotCreatedException()
		if self.TransactionInProgress and not override: # Write even if a transaction is in progress if override is set to true
			return
		try:
			self.fileObj.seek(0,0) # The first number is the number of bytes into the file, the second one is the seek mode.
			if self.pretty:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers], indent = self.pretty)) # These lines are pretty much the same
			else:
				self.fileObj.write(json.dumps([self.master,self.saves,self.triggers]))
			self.fileObj.truncate() # Remove everything after the end of where the current write head is, which is at the end of our dump
			if self.fileObj.flush(): # If flush returns false, don't use fsync on the filenumber, so if the user is using an object this function just has to return false
				os.fsync(self.fileObj.fileno())
		except IOError:
			#print traceback.format_exc()
			raise DatabaseWriteIOErrorException()
	def SaveExists(self,name): # Check if a save does or does not exist
		if not self.init:
			raise DatabaseNotCreatedException()
		return name in self.saves # Pretty self explanatory
	def Save(self,name,table = False):
		if not self.init:
			raise DatabaseNotCreatedException()
		self.saves[name] = {} # Initialize that save to an empty dictionary
		if table:
			self.saves[name][table] = copy.deepcopy(self.master[table]) # If table is set to true, copy that table to the save. Else, copy a dictionary of all the tables
		else:
			self.saves[name] = copy.deepcopy(self.master)
		self._write() # Write to the filesystem
	def RemoveSave(self,name): # Remove save function
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.SaveExists(name): # Make sure what we're deleting exists to begin with
			raise SavepointDoesNotExistException()
		del self.saves[name] # A simple del statement to do the deleting
	def Revert(self,name,table = False): # Revert to a savepoint
		if not self.init:
			raise DatabaseNotCreatedException()
		if self.SaveExists(name): # Make sure what we're reverting to exists
			if table: # If they specified a table name, 
				self.master[table] = copy.deepcopy(self.saves[name][table]) # set that table equal to the savepoint
			else:
				for table, unused in self.saves[name].items(): # Iterate through all the tables in the savepoint. This ensure tables in the database but not the savepoint will remain unaffected
					self.master[table] = copy.deepcopy(self.saves[name][table]) 
		else:
			raise SavepointDoesNotExistException()
		self._write() # Filesystem write
	def GetSave(self,name = False, table = False): # Return a savepoint. Equivelent to self.Dump except returns from a savepoint and not the current db
		if not self.init:
			raise DatabaseNotCreatedException()
		if name:
			if table:
				return self.saves[name][table]
			return self.saves[name]
		else: # End users are not expected to use this
			return self.saves
	def ListSaves(self): # List savepoints
		if not self.init:
			raise DatabaseNotCreatedException()
		return [x for x, y in self.saves.items()]
	def PutSave(self, data, name = False): # Takes a savepoint and places it in the savepoint system, overwriting what was already there
		# Do not use this unless you know how this system works. Well.
		if not self.init:
			raise DatabaseNotCreatedException()
		if type(data) != dict:
			raise TypeError
		if name:
			self.saves[name] = data
		else:
			self.saves = data
		self._write()
	def _runTrigger(self,triggertype,table,datapoint): # This is an internal function to run triggers
		for x in self.triggers: # Iterate through all loaded triggers
			if table == x[0] and triggertype == x[1]: # If the triggertype (something like "AFTER INSERT") matches what we need to run this time, and the table matches
				dill.loads(base64.b64decode(x[2]))(self,datapoint,table,triggertype) # Base64 decode the stored data to the dill bytecode, convert the bytecode to a callable, call it with four arguments
	def AddTrigger(self,name,triggertype,table,action): # This registers a new trigger
		if not triggertype in ["BEFORE INSERT","AFTER INSERT","BEFORE DELETE","AFTER DELETE","BEFORE UPDATE","AFTER UPDATE"]: # Verify that the trigger is valid
			raise NotImplementedError
		if not self.init:
			raise DatabaseNotCreatedException()
		if self.TriggerExists(name): # Make sure we're not overwriting a trigger
			raise DuplicateTriggerNameExistsException()
		if not self.TableExists(table): # Make sure the table exists
			raise TableDoesNotExistException()
		self.triggers.append([table,triggertype,base64.b64encode(dill.dumps(action)).decode("utf-8","replace"),name]) # Take the function, serialize it to bytecode, base64 it, convert it to a string, put it in a list along with the table to execute it on, the trigger type, and the name of the trigger, then take that list and add it to the list of triggers
		self._write() # Filesystem write
	def RemoveTrigger(self,name): # Remove a trigger
		if not self.init:
			raise DatabaseNotCreatedException()
		if not self.TriggerExists(name): # Verify that the trigger exists
			raise TriggerDoesNotExistException()
		for x in self.triggers: # Iterate through all triggers
			if x[3] == name: # If the name of the trigger is the name we were passed
				del self.triggers[self.triggers.index(x)] # Delete it
				break # Stop iterating
		self._write() # Filesystem write
	def ListTriggers(self): # List triggers
		if not self.init:
			raise DatabaseNotCreatedException()
		results = []
		for x in self.triggers: # For each trigger
			results.append([x[3],x[1],x[0]]) # Add a list of the name, type and table of the trigger
		return results # Return those lists
	def TriggerExists(self,name): # Test if a trigger exists
		if not self.init:
			raise DatabaseNotCreatedException()
		for x in self.triggers: # Iterate through triggers
			if x[3] == name: # If the name equals the name we were passed
				return True # return that it does exist
		return False # If we finish iteration, return that it does not exist
