# learn-mig-sync-tool

This is a simple command-line GUI to sync a directory and all its contents with a predefined AWS S3 bucket and subdirectory in one of several predefined AWS regions.  Uses the boto3 library to support multipart and concurrent uploads for performance, and defines an AWS session for re-use to avoid connection errors and timeouts.  Supported on either RHEL/CentOS 6.5+ or Windows x64.

## Usage
1.  Download either synctool (*nix) or Synctool.exe (Windows)
	- if using *nix, make sure to chmod 775 or chmod -x and change owner to the appropriate user, e.g. bbuser
2.  Make sure the directory in which the executable resides has **write permission** for the current user
3.  From CLI, run the executable
4.  From the menu options, run **setdir** to set the working directory (this will be the directory you'll be scanning/syncing)
5.  Afterwards, run **awsconfig** to set your AWS session parameters
6.  Next, run **scan** to perform a scan of the directory
7.  Finally, run **sync** to start syncing files to S3.  This process can be interrupted and resumed later with another **sync**
8.  Once fully synced, run **scan** and to each time to pick up changes under the selected directory and **sync** to push the changes

Changes are tracked per OS directory and stored in a unique DB file.  To sync another directory, simply switch with **setdir**.  To return to syncing the first directory, just **setdir** back to the first directory.

Directories are uploaded to S3 such that the folder name being synced becomes the root folder in S3, so make sure the directory name set via **setdir** is unique (e.g. don't try to sync (/usr/local/myuser1/uploads and /usr/local/myuser2/uploads as they will both be uploaded as /uploads/* and collide).
