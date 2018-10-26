import os
import hashlib
import _pickle as cpickle
import re
import sqlite3
import boto3
import logging
from logging.handlers import RotatingFileHandler
from boto3 import exceptions
from platform import system
from sys import stdout


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
                logging.debug("Config Loaded.")
        except IOError as e:
            if e.errno == 2:
                logging.info("No previous config found.  Using default config.")
                self.save()
            else:
                logging.warning("Using default config.  Could not load config.  "
                                "I/O error({0}): {1}".format(e.errno, e.strerror))
        except (KeyError, EOFError):
            logging.warning("Config corrupt or missing values.  Resetting to default.")
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
                logging.debug("Config saved.")
        except IOError as e:
            logging.error("Unable to save {0}: I/O error({1}): {2}".format(_configfile, e.errno, e.strerror))

    def reset(self):
        self.__init__()

    def s3path(self) -> str:
        return "client/%s/" % self.s3user

    def s3bucket(self) -> str:
        return "blackboard-learn-data-transfer-%s" % self.s3region

    def logawsconfig(self):
        ak, sk, user, bucket = "None", "None", "None", "None"
        if self.accesskey:
            ak = "%s...%s" % (self.accesskey[0], self.accesskey[-1])
        if self.secretkey:
            sk = "%s...%s" % (self.secretkey[0], self.secretkey[-1])
        if self.s3user:
            user = self.s3user
        if self.s3region:
            bucket = self.s3bucket()
        logging.debug("AWSCONFIG: Accesskey={0}, Secretkey={1}, User={2}, Bucket={3}"
                      .format(ak, sk, user, bucket))


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
        self.fullpath = os.path.join(parent, name)

# #######   Globals  ###########


_logpath = os.path.join('logs', 'synctool.log')
_configfile = "config.db"
_config = Config()

# #let's not go off the handle until we're prod ready.  limit 100k results.
_handbrake = 100000


# ####### Generators ###########


# Recursive generator for getting all files and directories under a path
# Pass the path under which you want to list all files/dirs
# Returns objects of class File
def files(path: str, basedir: str):
    if path:
        for p in os.listdir(path):
            fullpath = os.path.join(path, p)
            ospath = fullpath.replace("\\", "/")
            osbase = basedir.replace("\\", "/")
            s3path = ospath.replace(osbase, osbase.rsplit('/', 1)[-1])
            if os.path.isdir(fullpath):
                yield File(p, path, s3path, 'DIR', os.path.getsize(fullpath), os.path.getmtime(fullpath))
                yield from files(fullpath, basedir)
            else:
                yield File(p, path, s3path, 'FILE', os.path.getsize(fullpath), os.path.getmtime(fullpath))

# ####### Function Defs ###########


# #Create Logger
def create_logger(path):
    try:
        if not os.path.exists('logs'):
            os.makedirs('logs')
        logfilehandler = RotatingFileHandler(path, maxBytes=1048576 * 10, backupCount=5)
        logfilehandler.setFormatter(logging.Formatter(
            fmt='%(asctime)s: %(levelname)s:\t%(message)s', datefmt='%m/%d/%Y %H:%M:%S'))
        logfilehandler.setLevel(logging.DEBUG)
        streamhandler = logging.StreamHandler(stdout)
        streamhandler.setFormatter(logging.Formatter(fmt='%(message)s'))
        streamhandler.setLevel(logging.INFO)
        logging.basicConfig(
            level=logging.DEBUG,
            handlers=[logfilehandler,
                      streamhandler
                      ])
    except IOError as e:
        print("Could not create log at %s, ERROR: %s" % (path, e.strerror))
        exit(-1)


# #Get human-readable size on disk
def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


# #Lame progress bar
def update_progress(progress):
    bar = int(round(progress*50))
    stdout.write("\r %.2f%% [%s%s]" % ((progress*100), '#'*bar, ' '*(50-bar)))
    if (progress*100) >= 100:
        stdout.write("\n")


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
    _config.logawsconfig()
    _config.save()
    _ = input("AWS Config saved.  Hit enter to continue.")
    return True


# #Open DB for read/write, returns connection object, None if fails
def opendb(filename):
    if os.path.isfile(filename):
        try:
            return sqlite3.connect(filename)
        except sqlite3.DatabaseError as e:
            logging.error("Could not open existing database: %s" % e)
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
            logging.error("Could not create database: %s" % e)
            return None


# #Insert, update, or delete file from DB
def filetodb(f: File, connection: sqlite3.Connection):
    modes = ["UPDATE files set name=?, parent=?, s3path=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "UPDATE files set name=?, parent=?, s3path=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "INSERT INTO files (name, parent, s3path, type, size, modified, state, fullpath) " +
             "values(?,?,?,?,?,?,?,?)",
             "UPDATE files set name=?, parent=?, s3path=?, type=?, size=?, modified=?, state=? where fullpath=?",
             "DELETE FROM files where fullpath=?"]
    if f.state <= 3:
        connection.cursor().execute(modes[f.state],
                                    (f.name, f.parent, f.s3path, f.objecttype, f.size, f.modified, f.state, f.fullpath))
    elif f.state == 4:
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
            logging.debug("Current working directory changed to %s" % _config.cwd)
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
    _ = input("Hit enter to continue.")
    return True


# #New Scan Logic:
def scan():
    global _config
    if os.path.isdir(_config.cwd):
        logging.info("Opening existing db for %s" % _config.cwd)
        with opendb(hashedfilename(_config.cwd)) as dbconn:
            tempold = dbtodict(dbconn)
            # run generator into file dict
            logging.info("Generating list of files under current working directory...")
            tempnew = scantodict(_config.cwd)
            # run file compare on file dict, remove all nochanges
            tempnew = filecompare(tempold, tempnew)
            # iterate filetodb on each file dict, commit each time (or every 10 maybe?) test commit each time vs at end
            logging.info("Saving updates...")
            total = len(tempnew)
            progress = 0
            for f in tempnew:
                filetodb(tempnew[f], dbconn)
                progress += 1
                update_progress(progress/total)
            dbconn.commit()
            logging.debug("Scan of %s saved in %s" % (_config.cwd, hashedfilename(_config.cwd)))
            _config.history[_config.cwd] = hashedfilename(_config.cwd)
            _config.save()
            logging.info("Scan complete.  Run upload to begin uploading to S3.")
            del tempnew
            del tempold
    _ = input("Hit enter to continue.")
    return True


# #Scan selected directory
def scantodict(directory) -> dict:
    # #Init generator
    tempfiles = {}
    scanprog = 0
    if os.path.isdir(directory):
        getfiles = files(directory, _config.cwd)
        # #Run generator with handbrake, fill dict with full path as key, file objects as values, then delete generator
        try:
            while True:
                f = (next(getfiles))
                tempfiles[f.fullpath] = f
                scanprog += 1
                stdout.write("\r%d Objects Scanned" % scanprog)
        except StopIteration:
            pass
        finally:
            stdout.write("\n")
            del getfiles
    else:
        logging.warning("%s either is not a directory, doesn't exist, or denies access." % directory)
    return tempfiles


# #Compare file dicts for differences
def filecompare(tempold: dict, tempnew: dict) -> dict:
    if len(tempold) > 0:
        if len(tempnew) > 0:
            total = len(tempnew)
            progress = 0
            remove = []
            # State: 0=no change, 1=updated, 2=new, 3=deleted
            logging.info("Processing Updates...")
            for c in tempnew:
                if c in tempold:
                    if (tempnew[c].modified > tempold[c].modified) and tempold[c].state in [0, 1, 3]:
                        tempnew[c].state = 1
                        logging.debug("Found Update:\t%s" % c)
                    else:
                        tempnew[c].state = 0
                        remove.append(c)
                    del (tempold[c])
                else:
                    tempnew[c].state = 2
                    logging.debug("Found New:\t%s" % c)
                progress += 1
                update_progress(progress/total)
            for c in remove:
                    del tempnew[c]
            del remove
            logging.info("Processing Deletes...")
            total = len(tempold)
            progress = 0
            for c in tempold:
                if tempold[c].state == 2:
                    tempold[c].state = 4
                else:
                    tempold[c].state = 3
                tempnew[c] = tempold[c]
                logging.debug("Found Delete:\t%s" % c)
                progress += 1
                update_progress(progress/total)
            del tempold
        else:
            logging.info("No files in directory.")
    else:
        logging.info("First scan of directory.  All files will be uploaded.")
    return tempnew


# #Show last scan of current directory, if exists
def showlast():
    if os.path.isfile(hashedfilename(_config.cwd)):
        with opendb(hashedfilename(_config.cwd)) as dbconn:
            tempfiles = dbtodict(dbconn)
            print('Loaded ' + _config.cwd)
            printfiles(tempfiles)
            del tempfiles
    else:
        logging.info("%s has no previous scan data or DB file is inaccessible." % _config.cwd)
    _ = input("Hit enter to continue.")
    return True


# #Sync a file to S3
def syncfile(file: File, s3conn) -> int:
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    filename = _config.s3path() + file.s3path
    if file.objecttype == "DIR":
        filename = filename + "/"
    if file.state in (1, 2):
        r = -1
        if file.objecttype == "DIR":
            try:
                s3conn.meta.client.put_object(Bucket=_config.s3bucket(), Key=filename)
                r = 0
            except s3conn.meta.client.exceptions.ClientError as e:
                stdout.write("\r")
                logging.warning(e)
        else:
            try:
                s3conn.meta.client.upload_file(file.fullpath, _config.s3bucket(), filename)
                r = 0
            except boto3.exceptions.S3UploadFailedError as e:
                stdout.write("\r")
                logging.warning(e)
            except OSError as e:
                if e.errno == 2:
                    r = 4
                stdout.write("\r")
                logging.warning("UPLOAD FAILED - File: %s, ErrorNo %d: %s" % (e.filename, e.errno, e.strerror))
        return r
    elif file.state == 3:
        try:
            s3conn.meta.client.delete_object(Bucket=_config.s3bucket(), Key=filename)
            return 4
        except s3conn.meta.client.exceptions.ClientError as e:
            stdout.write("\r")
            logging.warning(e)
            return -1
    else:
        return -1


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
            _config.logawsconfig()
            s3 = boto3.Session(
                aws_access_key_id=_config.accesskey,
                aws_secret_access_key=_config.secretkey,
            ).resource('s3')
            logging.info("Open existing db for current working directory...")
            with opendb(hashedfilename(_config.cwd)) as dbconn:
                flist = dbtodict(dbconn, "updates")
                logging.info("Updating S3...")
                total = len(flist)
                progress = 0
                for f in flist:
                    logging.debug("Processing %s:" % f)
                    dostate = syncfile(flist[f], s3)
                    if dostate >= 0:
                        flist[f].state = dostate
                        filetodb(flist[f], dbconn)
                        dbconn.commit()
                    progress += 1
                    update_progress(progress/total)
                # close connection
                logging.info("Sync to S3 complete.")
                del flist
    else:
        logging.warning("Misconfiguration or missing parameters in AWS Settings, "
                        "please run awsconfig to configure AWS settings.")
    _ = input("Hit enter to continue.")
    return True


# #S3 verify function goes here
# load oldfiles for _cwd
# for each in oldfiles, check S3
#  update oldfiles


# #Nuke a prefix in S3 using paginated delete (very fast for large numbers of deletes
# #Putting this here for later: to filter objects by tag, must use JMESPath expressions:
# #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html#filtering-results
def s3delete() -> bool:
    if (
            _config.accesskey is not None and
            _config.secretkey is not None and
            _config.s3region is not None and
            _config.s3user is not None
    ):
        _config.logawsconfig()
        s3conn = boto3.Session(
            aws_access_key_id=_config.accesskey,
            aws_secret_access_key=_config.secretkey,
        ).resource('s3')
        client = s3conn.meta.client
        paginator = client.get_paginator('list_objects_v2')
        deleted = 0
        path = None
        while path is None:
            path = input("Enter path in S3 to delete: ")
            path = re.search("^([\w+/?]+)$", path, re.M)
        path = _config.s3path() + path.group(1)
        yn = input("Are you absolutely sure you want to delete "
                   "the following path and all sub-objects? (y/n)\n%s:   " % path)
        if yn.lower() == "y":
            logging.info("Deleting %s..." % path)
            try:
                pages = paginator.paginate(Bucket=_config.s3bucket(), Prefix=path)
                delete_us = dict(Objects=[])
                for item in pages.search('Contents'):
                    delete_us['Objects'].append(dict(Key=item['Key']))
                    deleted += 1
                    # flush once aws limit reached
                    if len(delete_us['Objects']) >= 1000:
                        client.delete_objects(Bucket=_config.s3bucket(), Delete=delete_us)
                        delete_us = dict(Objects=[])
                        stdout.write("\r")
                        logging.info("%d objects deleted." % deleted)

                # flush rest
                if len(delete_us['Objects']):
                    client.delete_objects(Bucket=_config.s3bucket(), Delete=delete_us)
                    stdout.write("\r")
                    logging.info("%d objects deleted." % deleted)
            except client.exceptions.ClientError as e:
                logging.warning(e)
            except TypeError:
                logging.info("Directory %s does not exist in S3 or nothing to delete." % path)
        else:
            print("Delete canceled.")
    else:
        logging.warning("Misconfiguration or missing parameters in AWS Settings, "
                        "please run awsconfig to configure AWS settings.")
    _ = input("Hit enter to continue.")
    return True


# #Handy one-shot for clearing CLI
def clear():
    if system() == "Windows":
        _ = os.system('cls')
    else:
        _ = os.system('clear')


# #Garbage function for debugging
def printfiles(listtoprint):
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    state = ['No Change', 'Updated', 'New', 'Deleted']
    if len(listtoprint) > 0:
        for c in listtoprint:
            print(c + "\t|\t" +
                  sizeof_fmt(listtoprint[c].size) + "\t|\t" +
                  state[listtoprint[c].state])
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
        "delete": s3delete,
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
    print('\texit\t\t\t\tExit\n')
    print("Current working directory is " + _config.cwd)
    return input('\nType a command above to continue:  ')


# ####### Go time   ###########
def main():
    create_logger(_logpath)
    logging.debug("Logging started.")
    _config.load()
    # Show menu until selection is made, execute selection, then save config updates.
    # Exits when a function returns false, e.g. the done() function.
    b = True
    while b is True:
        select = menu().lower()
        clear()
        b = switchplate(select)
    _config.save()
    logging.debug("Synctool finished successfully.")


# #######  Dunzo  ##############
if __name__ == "__main__":
    main()
