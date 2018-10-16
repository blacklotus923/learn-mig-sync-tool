import os
import hashlib
import _pickle as cpickle
import sys
from platform import system

print('Sup dudes')

# #######  Globals and user vars  ##########
_configfile = "config.db"

# #let's not go off the handle until we're prod ready.  limit 100k results.
handbrake = 100000

configuration = {
    "_cwd": os.path.abspath(os.getcwd()),
    "_fname": os.path.abspath(os.getcwd()),
    "history": {}
}

newfiles = {}
oldfiles = {}

# ####### Class Defs ###########


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

# No need for this function as we concatinate the full path for dict key later
#    def fullpath(self):
#        return self.parent + '\\' + self.name

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


# #Save dict to disk, True if saved, False if not
def savedict(filename, dictionary):
    try:
        with open(filename, 'wb') as f:
            cpickle.dump(dictionary,f, protocol=3)
        return True
    except IOError as e:
        print("Unable to save " + filename + ": I/O error({0}): {1}".format(e.errno, e.strerror))
        raise e
    except:  # handle other exceptions such as attribute errors
        print("Unexpected error: ", sys.exc_info()[0])
        raise


# #Load dict from disk
def loaddict(filename):
    try:
        with open(filename, 'rb') as f:
            return cpickle.load(f)
    except IOError as e:
        raise e
    except:  # handle other exceptions such as attribute errors
        print("Unexpected error: ", sys.exc_info()[0])
        raise


# #Compare file dicts for differences
def filecompare():
    global oldfiles
    global newfiles

    if hashedfilename(configuration["_fname"]) in configuration["history"]:
        try:
            oldfiles = loaddict(hashedfilename(configuration["_fname"]))
        except:
            oldfiles = {}
    else:
        oldfiles = {}

    if len(oldfiles)>0:
        if len(newfiles)>0:
            # State: 0=no change, 1=updated, 2=new, 3=deleted
            for c in newfiles:
                if c in oldfiles:
                    if newfiles[c].modified > oldfiles[c].modified:
                        newfiles[c].state = 1
                    else:
                        newfiles[c].state = 0
                    del (oldfiles[c])
                else:
                    newfiles[c].state = 2

            for c in oldfiles:
                oldfiles[c].state = 3
                newfiles[c] = oldfiles[c]
            oldfiles.clear()
            return True
        else:
            print("No scan loaded for current directory, please run scanfiles first.")
            return True
    else:
        print("Since this is the first scan of this directory, make sure to save with savelist.")
        return True


# #Set directory to scan
def setdir():
    global configuration
    b = True
    while b:
        directory = input("Enter path to recursively search, c to cancel:  ")
        if directory == "c":
            b = False
        elif os.path.isdir(directory):
            directory = os.path.abspath(directory)
            configuration["_cwd"] = directory
            b = False
        else:
            print(directory + " doesn't appear to be a directory or access is denied.")
    return True


# #List past directories
def listdirs():
    for k, v in configuration["history"].items():
        print("%s : ID - %s" % (v, k))
    _ = input("Press any key to continue")
    return True


# #Scan selected directory
def scandir():
    # #Init generator
    global configuration
    global newfiles
    if os.path.isdir(configuration["_cwd"]):
        getfiles = files(configuration["_cwd"])
        totalsize = 0
        newfiles.clear()
        # #Run generator with handbrake, fill dict with full path as key, file objects as values, then delete generator
        try:
            for x in range(0, handbrake):
                f = (next(getfiles))
                newfiles[f.parent + '\\' + f.name] = f
                totalsize += os.path.getsize(f.parent + '\\' + f.name)
                print(f.parent + '\\' + f.name + ",    " + f.type + ", " + str(f.modified))
                # #Stats
            print('\nIterated ' + str(x) + ' times. File dictionary is ' + str(len(newfiles)) + ' in length.')
            print('\nSize of dict in memory is ' + sizeof_fmt(sys.getsizeof(newfiles)))
            print('\nSize of all files: ' + sizeof_fmt(totalsize))
        except StopIteration:
            pass
        finally:
            configuration["_fname"] = configuration["_cwd"]
            del getfiles
    else:
        print(configuration["_cwd"] + " either is not a directory, doesn't exist, or denies access.")

    return True


# #Save Updates after current scan
def savelist():
    global configuration
    hfn = hashedfilename(configuration["_fname"])
    if savedict(hfn, newfiles):
        configuration["history"][hfn] = configuration["_fname"]
        print('Scan of ' + configuration["_fname"] + ' saved.')
    else:
        print('Error saving ' + configuration["_fname"])
    return True


# #Show last scan of current directory, if exists
def showlast():
    global oldfiles
    if hashedfilename(configuration["_cwd"]) in configuration["history"]:
        try:
            oldfiles = loaddict(hashedfilename(configuration["_cwd"]))
            print('Loaded ' + configuration["_cwd"])
            for c in oldfiles:
                print('|' + c + '        |   ' + oldfiles[c].type + '|      ' + str(oldfiles[c].modified) + '|    ' + str(
                    oldfiles[c].size))
            oldfiles.clear()
            return True
        except:
            print("Could not load last scan for " + configuration["_cwd"])
            return True
    else:
        print("%s has no previous scan data. Returning to menu."%configuration["_cwd"])
        return True


# #Sync function goes here
# def sync(File)
# If new, upload
# If existing, overwrite
# If deleted, delete from S3


# #S3 verify function goes here
# load oldfiles for _cwd
# for each in oldfiles, check S3
#  update oldfiles


# #Handy one-shot for clearing CLI
def clear():
    if system() == "Windows":
        _=os.system('cls')
    else:
        _=os.system('clear')


# #Garbage function for debugging
def cwd():
    print("Current working directory is " + configuration["_cwd"])
    return True


# #Garbage function for debugging
def printupdates():
    # State: 0=no change, 1=updated, 2=new, 3=deleted
    state = ['No Change', 'Updated', 'New', 'Deleted']
    if len(newfiles) > 0:
        for c in newfiles:
            if newfiles[c].state >= 0:
                print('|' + c + '       |   ' + str(newfiles[c].modified) + '|\t\t' + state[newfiles[c].state])
    else:
        print('No state changes detected or scan not yet done.')
    return True


# #Garbage function for debugging
def showhashed():
    print(hashedfilename(configuration["_cwd"]))
    return True


# #Exit
def done():
    return False


# #switchplate; handy way for switch logic progcontrol
def switchplate(argument):
    sp = {
        "setdir": setdir,
        "cwd": cwd,
        "listdirs": listdirs,
        "scandir": scandir,
        "printupdates": printupdates,
        "filecompare": filecompare,
        "savelist": savelist,
        "showlast": showlast,
        "done": done,
        "showhashed": showhashed
    }
    func = sp.get(argument, lambda: True)
    return func()


# #Damenu
def menu():
    clear()
    print("Available commands:")
    print('\tsetdir\t\t\t\tSet directory of content')
    print('\tlistdirs\t\t\tList previously scanned directories')
    print('\tscandir\t\t\t\tScan currently selected directory')
    print('\tsavelist\t\t\tSave current scan, if available')
    print('\tshowlast\t\t\tShow last scan and stats for currently selected directory')
    print('\tdone\t\t\t\tExit\n')
    cwd()
    return input('\nType a command above to continue:  ')


# #######  Inits   ###########


# ####### Go time   ###########

# if errorno=2 (file not found) then make it with default values; otherwise, panic.
try:
    configuration = loaddict(_configfile)
except IOError as e:
    if e.errno == 2:
        print("No previous config found.  Using default config.")
    else:
        print("Could not load config.  I/O error({0}): {1}".format(e.errno, e.strerror))
        _ = input("Press enter to panic.")
        quit(0)
except:
    print("Unforseen error loading config.")
    _ = input("Press enter to panic.")
    quit(0)

# Show menu until selection is made, execute selection, then save config updates.
# Exits when a function returns false, e.g. the done() function.
b = True
while b is True:
    select = menu().lower()
    b = switchplate(select)
    try:
        savedict(_configfile, configuration)
    except:
        print("Could not save config.  Check directory permissions on " + str(os.path.abspath(os.getcwd())))


# #######  Dunzo  ##############

