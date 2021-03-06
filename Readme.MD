# BTEdb
BTEdb (Better Than Ethan's database) is a project created because Ethan thought he could write a database in Python, and mine has to be better than his

It is a schemaless database like MongoDB that serializes to JSON. It can either serialize to a file, or an object that you pass it.

An example:

    db = BTEdb.Database("filename.json")

Or:

    class MyClass:
        def seek(self,position,mode):
            pass
        def truncate(self):
            return False
        def __init__(self):
            self.data = ""
        def write(self,data):
            self.data = data
        def flush(self):
            pass
        def read(self):
            return self.data
        def close(self):
            pass
    MyObject = MyClass()
    db = BTEdb.Database(MyObject)

You don't need to specify an object at all when initiating a database, but you will still need to create one. For example:

    db = BTEdb.Database("filename.json")

does the same thing as:

    db = BTEdb.Database()
    db.OpenDatabase("filename.json")

This allows you to switch files on-the-fly

BTEdb also supports pretty-print. Specify any number of spaces for the database to use, for example:

    db = BTEdb.Database("filename.json", 4)

or:

    db = BTEdb.Database()
    db.OpenDatabase("filename.json", 4)


You can safely close file descriptors and return the database to a just-created state like this:

    db.Destroy()

Creating a table looks like this:

    db.Create("Table name")

If the table exists, it will be truncated

If the table does not exist, most of the following methods will raise a TableDoesNotExistException

Drop the bass:

    db.Drop("Table name")

Truncate a table like this:

    db.Truncate("Table name")

Dump the entire database:

    db.Dump()

Or a specific table:

    db.Dump("Table name")

Check if a table exists like this:

    db.TableExists("Table name")

Insert like this:

    db.Insert("Table name", Username = "Niles", UID = 1000, favColour = 0xffffff)

As with all schemaless databases, you are responsible for sanitizing the input and output to the database. It is entirely possible to store that entry and {x = 12, y = 24, pointColour = "green"} in the same table. 

Select data like this:

    db.Select("Table name", UID = 1000)

This selects all entries with the UID of 1000 and the favourite colour of white:

    db.Select("Table name", UID = 1000, favColour = 0xffffff)

This selects all entries with the favourite colour of white and who has an E in their username, case insensitive:

    db.Select("Table name", favColour = 0xffffff, lambda x: "e" in x["username"].lower())
	
You may use a lambda or a pre-defined function. For example:

    import urllib2
	def CheckUser(Datapoint):
		values = { "UID" : Datapoint["UID"], "username" : Datapoint["username"] }
		postdata = urllib2.encode(values)
		request = urllib2.Request("http://example.com/CheckUser.php",postdata)
		response = request.read()
		if response == "User accepted":
			return True
		else:
			return False
	db.Select("Table name", CheckUser)
	
That would query an external server to check the username and UID to determine if each row should be selected or not. Be warned that this will generate a LOT of network traffic

Deleting is similar. The following will delete any user with the UID of 1000:

    db.Delete("Table name", UID = 1000)

Lambdas and functions may also be used here.

It is important to note that the first non-keyword argument is the table name. Non-function or lambda non-keyword arguments after the table name will raise a TypeError

Update is a little bit more complicated. Here is an example to get you going

    db.Update("Table name", db.Select("Table name", favColour = 0xffffff), UID = 12, Username = "test")

This is the same as the SQL statement:

    UPDATE `Table name` SET `UID` = 12, `Username` = 'test' WHERE `favColour` = 0xffffff;

Lambdas may be used in the select statement, but not the update statement. To achieve something similar, you can do this:

    for x in db.Select("Table name", favColour = 0xffffff):
        db.Update("Table name", [x], UID = x["UID"] + 1)

That would increment the UIDs of every user with the favourite colour of white.

It can also be combined with the Dump method

    db.Update("Table name", db.Dump("Table name"), username = "newusername")
	
That would change every user's username to "newusername"

Saving a savepoint is as easy as this:

    db.Save("Savepoint name")
	
Reverting back to a savepoint:

    db.Revert("Savepoint name")
	
Saving a specific table:

	db.Save("Savepoint name", "Table name")
	
Recovering a specific table from a savepoint:

    db.Revert("Savepoint name", "Table name")
	
Deleting a savepoint

	db.RemoveSave("Savepoint name")
	
If you only saved a few tables, only those tables will be recovered if you attempt to recover the entire save.

You can manually retrieve or insert a savepoint with these methods:

    db.GetSave("Savepoint name")
	db.GetSave()
    db.PutSave(data,"Savepoint name")
	db.PutSave(data)
	
Under most circumstances, you should never need to use those methods. If for some reason you do, DO NOT confuse whether you are putting a save requested with a savepoint name or without one.

Triggers are identified by name, which can be a string, integer, float, or a few other datatypes. The name must be unique, however you may have infinite triggers.

You can add a trigger something like this:

    import time
    def handler(db, datapoint, table, event):
		print("Handler being called on event " + event + " on datapoint " + str(datapoint) + " in table " + table)
		db.Save(str(time.time()),table)
    db.AddTrigger("Trigger Name", "AFTER UPDATE", "Table name", handler)

That will make any update to something in the "Table name" table print a message and make a new savepoint. Beware that this will run for each individual datapoint.

You may also use lambda expressions.

Valid events are "BEFORE INSERT", "AFTER INSERT", "BEFORE DELETE", "AFTER DELETE", "BEFORE UPDATE" and "AFTER UPDATE"

With INSERT and DELETE, the same datapoint is passed to both BEFORE and AFTER handlers. With UPDATE, the old and new datapoints are passed to the BEFORE and AFTER handlers, respectively.

Remove like this:

    db.RemoveTrigger("Trigger name")

You may have multiple handlers for a single event.

Transactions are necessary for an efficient database. 

While not inside a transaction, methods that modify the database write out the database to the disk before they return. This is good if you don't have a lot of requests, but if you are going to execute two or more statements that modify data in a row, you should execute them within a transaction. 

Begin a transaction like this:

	db.BeginTransaction()
	
However, that will create a savepoint called "transaction". To begin a transaction without that, (you will be unable to revert the transaction) do this:

	db.BeginTransaction(False)

To commit the transaction and write to the disk:

	db.CommitTransaction()
	
If you chose to make a save, you can revert a transaction:

	db.RevertTransaction()

You can always tell if a transaction is in progress with the boolean value ``db.TransactionInProgress``

Other less used methods:

    db.Vacuum()                 # Forces a write to the disk
	db.ListSaves()              # Lists savepoints
    db.ListTables()             # Guess
	db.ListTriggers()           # Figure it out yourself
	db.TriggerExists(name)      # Who even reads these
