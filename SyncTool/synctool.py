import os
import hashlib
import _pickle as cpickle
import sqlite3
from platform import system

print('Sup dudes')

# #######  Globals and user vars  ##########


# ####### Class Defs ###########


class Config:
    def __init__(self):
        self.cwd = os.path.abspath(os.getcwd())
        self.history = {}

    def load(self):
        try:
            with open(_configfile, 'rb') as f:
                d = cpickle.load(f)
                self.cwd = d["cwd"]
                self.history = d["history"]
        except IOError as e:
            if e.errno == 2:
                print("No previous config found.  Using default config.")
                self.save()
            else:
                print("Using default config.  Could not load config.  I/O error({0}): {1}".format(e.errno, e.strerror))
        return self

    def save(self):
        try:
            with open(_configfile, 'wb') as f:
                d = {
                    "cwd": self.cwd,
                    "history": self.history
                }
                cpickle.dump(d, f, protocol=3)
        except IOError as e:
            print("Unable to save " + _configfile + ": I/O error({0}): {1}".format(e.errno, e.strerror))
        # except:  # handle other exceptions such as attribute errors
        #   print("Unable to save " + _configfile + ": ", sys.exc_info()[0])

    def reset(self):
        self.__init__()


# Setting up file class for each file/dir on disk
# State: 0=no change, 1=updated, 2=new, 3=deleted
class File:
    def __init__(self, name, parent, objecttype, size, modified=None, state=2):
        self.name = name
        self.parent = parent
        self.objecttype = objecttype
        self.size = size
        if modified is not None:
            self.modified = modified
        if state is not None:
            self.state = state
        self.fullpath = parent + '\\' + name

# #######   Globals  ###########


_configfile = "config.db"
_config = Config()

# #let's not go off the handle until we're prod ready.  limit 100k results.
_handbrake = 100000

newfiles = {}
oldfiles = {}


# ####### Generators ###########


# Recursive generator for getting all files and directories under a path
# Pass the path under which you want to list all files/dirs
# Returns objects of class File
def files(path):
    if path:
        for p in os.listdir(path):
            fullpath = os.path.join(path, p)
            if os.path.isdir(fullpath):
                yield File(p, path, 'DIR', os.path.getsize(fullpath), os.path.getmtime(fullpath))
                yield from files(fullpath)
            else:
                yield File(p, path, 'FILE', os.path.getsize(fullpath), os.path.getmtime(fullpath))

# ####### Function Defs ###########


# #Get human-readable size on disk
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


# #Return hash of a filename for saving unique dbs
def hashedfilename(filename):
    return str(hashlib.md5(filename.encode()).hexdigest()) + ".db"


# #Open DB for read/write, returns connection object, None if fails
def opendb(filename):
    if os.path.isfile(filename):
        try:
            return sqlite3.connect(filename)
        except sqlite3.DatabaseError as e:
            print("Could not open existing database: " + str(e))
            return None
    else:
        try:
            connection = sqlite3.connect(filename)
            connection.cursor().execute(
                '''CREATE TABLE files(
                      fullpath TEXT PRIMARY KEY,
                      name TEXT,
                      parent TEXT,
                      type TEXT,
                      size REAL,
                      modified REAL,
                      state INT)''')
            connection.commit()
            return connection
        except sqlite3.DatabaseError as e:
            print("Could not create database: " + str(e))
            return None


# #Insert, update, or delete file from DB
def filetodb(f: File, connection: sqlite3.Connection):
    modes = ["UPDATE files set name=?, parent=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "UPDATE files set name=?, parent=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "INSERT INTO files (name, parent, type, size, modified, state, fullpath) " +
             "values(?,?,?,?,?,?,?)",
             "DELETE FROM files where fullpath=?"]
    if f.state < 3:
        connection.cursor().execute(modes[f.state],
                                    (f.name, f.parent, f.objecttype, f.size, f.modified, f.state, f.fullpath))
    elif f.state == 3:
        connection.cursor().execute(modes[f.state],
                                    (f.fullpath,))


# #Read File from DB
def dbtodict(connection: sqlite3.Connection, mode="all"):
    d = {}
    if mode == "updates":
        query = '''SELECT fullpath, name, parent, type, size, modified, state
            from files WHERE state > 0'''
    else:
        query = '''SELECT fullpath, name, parent, type, size, modified, state
            from files'''
    for row in connection.cursor().execute(query):
        d[row[0]] = File(row[1], row[2], row[3], row[4], row[5], row[6])
    return d


# #Set directory to scan
def setdir():
    global _config
    b = True
    while b:
        directory = input("Enter path to recursively search, c to cancel:  ")
        if directory == "c":
            b = False
        elif os.path.isdir(directory):
            _config.cwd = os.path.abspath(directory)
            _config.save()
            b = False
        else:
            print(directory + " doesn't appear to be a directory or access is denied.")
    return True


# #List past directories
def updatehistory():
    global _config
    print("Previous Scans:")
    for k, v in _config.history.items():
        if os.path.isfile(k):
            del _config.history[k]
        else:
            print("%s :    ID - %s" % (v, k))
    _config.save()
    _ = input("Press any key to continue")
    return True


# #New Scan Logic:
def scan():
    global _config
    if os.path.isdir(_config.cwd):
        print("Open existing db for current working directory...")
        with opendb(hashedfilename(_config.cwd)) as dbconn:
            tempold = dbtodict(dbconn)
            input("Hit enter to continue")
            # run generator into newfiles
            print("Generating list of files under current working directory...")
            tempnew = scantodict(_config.cwd)
            input("Hit enter to continue")
            # run file compare on newfiles, remove all nochanges
            print("Scanning for changes...")
            tempnew = filecompare(tempold, tempnew)
            printfiles(tempnew)
            input("Hit enter to continue")
            # iterate filetodb on each newfiles, commit each time (or every 10 maybe?) test commit each time vs at end
            print("Saving updates...")
            for f in tempnew:
                filetodb(tempnew[f], dbconn)
            dbconn.commit()
            _config.history[_config.cwd] = hashedfilename(_config.cwd)
            _config.save()
            input("Hit enter to continue")
            # close connection
            print("Scan complete.  Run upload to begin uploading to S3.")
            del tempnew
            del tempold
    return True


# #Scan selected directory
def scantodict(cwd) -> dict:
    # #Init generator
    tempfiles = {}
    if os.path.isdir(cwd):
        getfiles = files(cwd)
        # #Run generator with handbrake, fill dict with full path as key, file objects as values, then delete generator
        try:
            for x in range(0, _handbrake):
                f = (next(getfiles))
                tempfiles[f.fullpath] = f
                print(f.fullpath + ",    " + f.objecttype + ", " + str(f.modified))
        except StopIteration:
            pass
        finally:
            del getfiles
    else:
        print(cwd + " either is not a directory, doesn't exist, or denies access.")
    return tempfiles


# #Compare file dicts for differences
def filecompare(tempold: dict, tempnew: dict) -> dict:
    if len(tempold) > 0:
        if len(tempnew) > 0:
            remove = []
            # State: 0=no change, 1=updated, 2=new, 3=deleted
            for c in tempnew:
                if c in tempold:
                    if (tempnew[c].modified > tempold[c].modified) and tempold[c].state in [0, 1]:
                        tempnew[c].state = 1
                    else:
                        tempnew[c].state = 0
                        remove.append(c)
                    del (tempold[c])
                else:
                    tempnew[c].state = 2
            for c in remove:
                    del tempnew[c]
            del remove
            for c in tempold:
                tempold[c].state = 3
                tempnew[c] = tempold[c]
            del tempold
        else:
            print("No files in directory.")
    else:
        print("First scan of directory.  All files will be uploaded.")
    return tempnew


# #Show last scan of current directory, if exists
def showlast():
    if os.path.isfile(hashedfilename(_config.cwd)):
        with opendb(hashedfilename(_config.cwd)) as dbconn:
            tempfiles = dbtodict(dbconn)
            print('Loaded ' + _config.cwd)
            printfiles(tempfiles)
            del tempfiles
        return True
    else:
        input("%s has no previous scan data or DB file is inaccessible. Hit enter to continue." % _config.cwd)
        return True


# #Sync a file to S3
def syncfile(file: File) -> bool:
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    # If updated, overwrite
    if file.state == 1:
        print("Updated %s" % file.fullpath)
    # If new, upload
    elif file.state == 2:
        print("Added %s" % file.fullpath)
    # If deleted, delete from S3
    elif file.state == 3:
        print("Deleted %s" % file.fullpath)
    else:
        return False
    return True


# #Run sync for all files in DB, update if sync'd
def sync():
    global _config
    if os.path.isdir(_config.cwd):
        print("Open existing db for current working directory...")
        with opendb(hashedfilename(_config.cwd)) as dbconn:
            files = dbtodict(dbconn, "updates")
            input("Hit enter to continue")
            print("Updating S3:")
            for f in files:
                print("Processing %s:" % f)
                if syncfile(files[f]):
                    files[f].state = 0
                    filetodb(files[f], dbconn)
                    dbconn.commit()
                else:
                    print("File %s not uploaded." % f)
            input("Hit enter to continue")
            # close connection
            print("Sync to S3 complete.")
            del files
    return True

# #S3 verify function goes here
# load oldfiles for _cwd
# for each in oldfiles, check S3
#  update oldfiles


# #Handy one-shot for clearing CLI
def clear():
    if system() == "Windows":
        _ = os.system('cls')
    else:
        _ = os.system('clear')


# #Garbage function for debugging
def cwd():
    print("Current working directory is " + _config.cwd)
    return True


# #Garbage function for debugging
def printfiles(files):
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    state = ['No Change', 'Updated', 'New', 'Deleted']
    if len(files) > 0:
        for c in files:
            print(c + "\t\t\t|\t\t\t" +
                  files[c].objecttype + "\t\t\t|\t\t\t" +
                  str(files[c].size) + "\t\t\t|\t\t\t" +
                  str(files[c].modified) + "\t\t\t|\t\t\t" +
                  state[files[c].state] + "|")
    return True


# #Garbage function for debugging
def showhashed():
    print(hashedfilename(_config.cwd))
    return True


# #Exit
def done():
    return False


# #switchplate; handy way for switch logic progcontrol
def switchplate(argument):
    sp = {
        "setdir": setdir,
        "cwd": cwd,
        "history": updatehistory,
        "scan": scan,
        "sync": sync,
        "showlast": showlast,
        "exit": done,
        "showhashed": showhashed
    }
    func = sp.get(argument, lambda: True)
    return func()


# #Damenu
def menu():
    clear()
    print("Available commands:")
    print('\tsetdir\t\t\t\tSet directory of content')
    print('\tscan\t\t\t\tScan currently selected directory')
    print('\tsync\t\t\t\tSync or resume S3 upload of currently selected directory')
    print('\thistory\t\t\t\tList previously scanned directories')
    print('\tshowlast\t\t\tShow last scan and stats for currently selected directory')
    print('\texit\t\t\t\tExit\n')
    cwd()
    return input('\nType a command above to continue:  ')


# #######  Inits   ###########


# ####### Go time   ###########

_config.load()

# Show menu until selection is made, execute selection, then save config updates.
# Exits when a function returns false, e.g. the done() function.
b = True
while b is True:
    select = menu().lower()
    b = switchplate(select)

_config.save()
# #######  Dunzo  ##############

