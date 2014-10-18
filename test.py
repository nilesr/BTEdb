import sys, json
sys.path += "."
import database
db = database.Database("x.json", 4)
def putData():
    db.Create("main table")
    db.Create("birthdays")
    db.Insert("main table", UID = 0, username = "root",favColor = 0xffffff)
    db.Insert("main table", UID = 1, username = "Ethan",favColor = 0xffffff)
    db.Insert("main table", UID = 2, username = "Niles",favColor = 0xffffff)
    db.Insert("birthdays", username = "root", bday = 1992)
    db.Insert("birthdays", username = "Ethan", bday = 1999)
    db.Insert("birthdays", username = "Niles", bday = 1998)
def printData():
    print json.dumps(db.Dump(), indent = 2)
db.Dump()

