# JsonDB
JsonDB is a project created because Ethan thought he could write a database in Python, and mine has to be better than his

It is a schemaless database like MongoDB that serializes to JSON. It can either serialize to a file, or an object that you pass it.

An example:

    db = JsonDB.Database("filename.json")

Or:

    class MyClass:
        def seek(self,position,mode):
            pass
        def truncate(self):
            self.data = ""
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
    db = JsonDB.Database(MyObject)

You don't need to specify an object at all when initiating a database, but you will still need to create one. For example:

    db = JsonDB.Database("filename.json")

does the same thing as:

    db = JsonDB.Database()
    db.OpenDatabase("filename.json")

This allows you to switch files on-the-fly

JsonDB also supports pretty-print. Specify any number of spaces for the database to use, for example:

    db = JsonDB.Database("filename.json", 4)

or:

    db = JsonDB.Database()
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

That can be very powerful, but only if you know what you're doing.

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
	
Loading a savepoint:

    db.Load("Savepoint name")
	
Saving a specific table:

	db.Save("Savepoint name", "Table name")
	
Loading a specific table:

    db.Load("Savepoint name", "Table name")
	
Deleting a savepoint

	db.RemoveSave("Savepoint name")
	
If you only saved a few tables, only those tables will be loaded if you attempt to load the entire save.

You can manually retrieve or insert a savepoint with these methods:

    db.GetSave("Savepoint name")
	db.GetSave()
    db.PutSave(data,"Savepoint name")
	db.PutSave(data)
	
Under most circumstances, you should never need to use those methods. If for some reason you do, DO NOT confuse whether you are putting a save requested with a savepoint name or without one.

Other less used methods:

    db.Vacuum() # Forces a write to the disk
	db.ListSaves() # Lists savepoints
    db.ListTables() # Guess
	
Planned features:

*   Triggers