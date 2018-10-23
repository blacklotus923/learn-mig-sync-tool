import os
import hashlib
import _pickle as cpickle
import re
import sqlite3
import boto3
from boto3 import exceptions
from platform import system

print('Sup dudes')

# #######  Globals and user vars  ##########


# ####### Class Defs ###########


class Config:
    def __init__(self):
        self.cwd = os.path.abspath(os.getcwd())
        self.accesskey = None
        self.secretkey = None
        self.s3region = None
        self.s3user = None
        self.history = {}

    def load(self):
        try:
            with open(_configfile, 'rb') as f:
                d = cpickle.load(f)
                self.cwd = d["cwd"]
                self.accesskey = d["ak"]
                self.secretkey = d["sk"]
                self.s3region = d["s3region"]
                self.s3user = d["s3user"]
                self.history = d["history"]
        except IOError as e:
            if e.errno == 2:
                print("No previous config found.  Using default config.")
                self.save()
            else:
                print("Using default config.  Could not load config.  I/O error({0}): {1}".format(e.errno, e.strerror))
        except (KeyError, EOFError):
            print("Config corrupt or missing values.  Resetting to default.")
            self.reset()
            self.save()

    def save(self):
        try:
            with open(_configfile, 'wb') as f:
                d = {
                    "cwd": self.cwd,
                    "ak": self.accesskey,
                    "sk": self.secretkey,
                    "s3region": self.s3region,
                    "s3user": self.s3user,
                    "history": self.history
                }
                cpickle.dump(d, f, protocol=3)
        except IOError as e:
            print("Unable to save " + _configfile + ": I/O error({0}): {1}".format(e.errno, e.strerror))

    def reset(self):
        self.__init__()

    def s3path(self) -> str:
        return "client/%s/" % self.s3user

    def s3bucket(self) -> str:
        return "blackboard-learn-data-transfer-%s" % self.s3region

    def print(self):
        print("CWD: %s" % self.cwd)
        print("Access Key: %s" % self.accesskey)
        print("Secret Key: %s" % self.secretkey)
        print("S3 Path: %s" % self.s3path)
        print("History: %s" % self.history)


# Setting up file class for each file/dir on disk
# State: 0=no change, 1=updated, 2=new, 3=deleted
class File:
    def __init__(self, name, parent, s3path, objecttype, size, modified=None, state=2):
        self.name = name
        self.parent = parent
        self.s3path = s3path
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
def files(path: str, basedir: str):
    if path:
        for p in os.listdir(path):
            fullpath = os.path.join(path, p)
            s3path = fullpath.replace(basedir, basedir.rsplit('\\', 1)[-1])
            s3path = s3path.replace("\\", "/")
            if os.path.isdir(fullpath):
                yield File(p, path, s3path, 'DIR', os.path.getsize(fullpath), os.path.getmtime(fullpath))
                yield from files(fullpath, basedir)
            else:
                yield File(p, path, s3path, 'FILE', os.path.getsize(fullpath), os.path.getmtime(fullpath))

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


# #Configure AWS parameters
def awsconfig():
    global _config
    regions = ("us-west-2", "us-west-1", "us-east-2", "us-east-1",
               "ap-south-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
               "ap-northeast-1", "ca-central-1", "cn-north-1", "eu-central-1",
               "eu-west-1", "eu-west-2", "eu-west-3", "sa-east-1", "us-gov-west-1")
    clear()
    ak = re.search(r"^(\w{20})$", input("Enter AWS Access Key: ").upper(), re.M)
    while ak is None:
        ak = re.search(r"^(\w{20})$",
                       input("Doesn't appear to be a "
                             "valid access key, please re-enter: ").upper(), re.M)
    _config.accesskey = ak.group(1)
    sk = input("Enter AWS Secret Key: ")
    while len(sk) is not 40:
        sk = input("Doesn't appear to be a valid secret key, please re-enter: ")
    _config.secretkey = sk
    region = input("Enter AWS region: ").lower()
    while region not in regions:
        region = input("Doesn't appear to be a valid AWS region, please re-enter: ")
    _config.s3region = region
    user = re.search(r"^(\d{6}-[\w|-]+)$", input("Enter AWS User Name: ").lower(), re.M)
    while user is None:
        user = re.search(r"^(\d{6}-[\w|-]+)$",
                         input("Doesn't appear to be a "
                               "valid AWS User Name, please re-enter: ").lower(), re.M)
    _config.s3user = user.group(1)
    _config.save()
    return True


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
                      s3path TEXT,
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
    modes = ["UPDATE files set name=?, parent=?, s3path=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "UPDATE files set name=?, parent=?, s3path=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "INSERT INTO files (name, parent, s3path, type, size, modified, state, fullpath) " +
             "values(?,?,?,?,?,?,?,?)",
             "DELETE FROM files where fullpath=?"]
    if f.state < 3:
        connection.cursor().execute(modes[f.state],
                                    (f.name, f.parent, f.s3path, f.objecttype, f.size, f.modified, f.state, f.fullpath))
    elif f.state == 3:
        connection.cursor().execute(modes[f.state],
                                    (f.fullpath,))


# #Read File from DB
def dbtodict(connection: sqlite3.Connection, mode="all"):
    d = {}
    if mode == "updates":
        query = '''SELECT fullpath, name, parent, s3path, type, size, modified, state
            from files WHERE state > 0'''
    else:
        query = '''SELECT fullpath, name, parent, s3path, type, size, modified, state
            from files'''
    for row in connection.cursor().execute(query):
        d[row[0]] = File(row[1], row[2], row[3], row[4], row[5], row[6], row[7])
    return d


# #Set directory to scan
def setdir():
    global _config
    valid = True
    while valid:
        directory = input("Enter path to recursively search, c to cancel:  ")
        if directory == "c":
            valid = False
        elif os.path.isdir(directory):
            _config.cwd = os.path.abspath(directory)
            _config.save()
            valid = False
        else:
            print(directory + " doesn't appear to be a directory or access is denied.")
    return True


# #List past directories
def updatehistory():
    global _config
    todelete = []
    print("Previous Scans:")
    for k, v in _config.history.items():
        if os.path.isfile(v):
            print("%s :    ID - %s" % (k, v))
        else:
            todelete.append(k)
    for k in todelete:
        del _config.history[k]
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
            # run generator into newfiles
            print("Generating list of files under current working directory...")
            tempnew = scantodict(_config.cwd)
            # run file compare on newfiles, remove all nochanges
            tempnew = filecompare(tempold, tempnew)
            printfiles(tempnew)
            # iterate filetodb on each newfiles, commit each time (or every 10 maybe?) test commit each time vs at end
            print("Saving updates...")
            for f in tempnew:
                filetodb(tempnew[f], dbconn)
            dbconn.commit()
            _config.history[_config.cwd] = hashedfilename(_config.cwd)
            _config.save()
            print("Scan complete.  Run upload to begin uploading to S3.")
            del tempnew
            del tempold
    return True


# #Scan selected directory
def scantodict(directory) -> dict:
    # #Init generator
    tempfiles = {}
    if os.path.isdir(directory):
        getfiles = files(directory, _config.cwd)
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
        print(directory + " either is not a directory, doesn't exist, or denies access.")
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
def syncfile(file: File, s3conn) -> bool:
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    filename = _config.s3path() + file.s3path
    if file.objecttype == "DIR":
        filename = filename + "/"
    if file.state in (1, 2):
        r = False
        if file.objecttype == "DIR":
            try:
                s3conn.meta.client.put_object(Bucket=_config.s3bucket(), Key=filename)
                r = True
            except s3conn.meta.client.exceptions.ClientError as e:
                print(e)
        else:
            try:
                s3conn.meta.client.upload_file(file.fullpath, _config.s3bucket(), filename)
                r = True
            except boto3.exceptions.S3UploadFailedError as e:
                print(e)
        return r
    elif file.state == 3:
        try:
            s3conn.meta.client.delete_object(Bucket=_config.s3bucket(), Key=filename)
            return True
        except s3conn.meta.client.exceptions.ClientError as e:
            print(e)
            return False
    else:
        return False


# #Run sync for all files in DB, update if sync'd
def sync():
    global _config
    if (
        _config.accesskey is not None and
        _config.secretkey is not None and
        _config.s3region is not None and
        _config.s3user is not None
    ):
        if os.path.isdir(_config.cwd):
            s3 = boto3.Session(
                aws_access_key_id=_config.accesskey,
                aws_secret_access_key=_config.secretkey,
            ).resource('s3')
            print("Open existing db for current working directory...")
            with opendb(hashedfilename(_config.cwd)) as dbconn:
                flist = dbtodict(dbconn, "updates")
                input("Hit enter to continue")
                print("Updating S3:")
                for f in flist:
                    print("Processing %s:" % f)
                    if syncfile(flist[f], s3):
                        flist[f].state = 0
                        filetodb(flist[f], dbconn)
                        dbconn.commit()
                    else:
                        print("File %s not uploaded." % f)
                input("Hit enter to continue")
                # close connection
                print("Sync to S3 complete.")
                del flist
    else:
        print("Misconfiguration or missing parameters in AWS Settings, "
              "please run awsconfig to configure AWS settings.")
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
def printfiles(listtoprint):
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    state = ['No Change', 'Updated', 'New', 'Deleted']
    if len(listtoprint) > 0:
        for c in listtoprint:
            print(c + "\t\t\t|\t\t\t" +
                  listtoprint[c].objecttype + "\t\t\t|\t\t\t" +
                  listtoprint[c].s3path + "\t\t\t|\t\t\t" +
                  str(listtoprint[c].size) + "\t\t\t|\t\t\t" +
                  str(listtoprint[c].modified) + "\t\t\t|\t\t\t" +
                  state[listtoprint[c].state] + "|")
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
        "awsconfig": awsconfig,
        "history": updatehistory,
        "scan": scan,
        "sync": sync,
        "showlast": showlast,
        "exit": done
    }
    func = sp.get(argument, lambda: True)
    return func()


# #Damenu
def menu():
    clear()
    print("Available commands:")
    print('\tsetdir\t\t\t\tSet directory of content')
    print('\tawsconfig\t\t\tConfigure AWS keys and path')
    print('\tscan\t\t\t\tScan currently selected directory')
    print('\tsync\t\t\t\tSync or resume S3 upload of currently selected directory')
    print('\thistory\t\t\t\tList previously scanned directories')
    print('\tshowlast\t\t\tShow last scan and stats for currently selected directory')
    print('\texit\t\t\t\tExit\n')
    cwd()
    return input('\nType a command above to continue:  ')


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
