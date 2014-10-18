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

Drop the bass:

    db.Drop("Table name")

Truncate a table like this:

    db.Truncate("Table name")

Dump the entire database:

    db.Dump()

Or a specific table:

    db.Dump("Table name")

If the table does not exist, it will raise a TableDoesNotExistException

Check if a table exists like this:

    db.TableExists("Table name")

Insert like this:

    db.Insert("Table name", Username = "Niles", UID = 1000, favColor = 0xffffff)

As with all schemaless databases, you are responsible for sanitizing the input and output to the database. It is entirely possible to store that entry and {x = 12, y = 24, pointColor = "green"} in the same table. 

That can be very powerful, but only if you know what you're doing.

Select data like this:

    db.Select("Table name", UID = 1000)

This selects all entries with the UID of 1000 and the favourite color of white:

    db.Select("Table name", UID = 1000, favColor = 0xffffff)

This selects all entries with the favourite colour of white and who has an E in their username, case insensitive:

    db.Select("Table name", favColor = 0xffffff, lambda x: "e" in x["username"].lower())

Deleting is similar. The following will delete any user with the UID of 1000:

    db.Delete("Table name", UID = 1000)

Lambdas and functions may also be used here.

It is important to note that the first non-keyword argument is the table name. Non-function or lambda non-keyword arguments after the table name will raise a TypeError

Update is a little bit more complicated. Here is an example to get you going

    db.Update("Table name", db.Select(favColor = 0xffffff), UID = 12, Username = "test")

This is the same as the SQL statement:

    UPDATE `Table name` SET `UID` = 12, `Username` = 'test' WHERE `favColor` = 0xffffff;

Lambdas may be used in the select statement, but not the update statement. 

Other less used methods:

    db.Vacuum() # Forces a write to the disk
    db.ListTables() # Guess

Planned features:
	Savepoints
	Transactions
	Triggers