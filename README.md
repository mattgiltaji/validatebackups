# validatebackups
Validates some personal google cloud backups

This utility is intended to download a sample of random files from backup locations.
These files can then be manually verified to make sure the backups are working.

This is a port of
[my Python script of the same name](https://github.com/mattgiltaji/miscutils/tree/master/validatebackups)
to Go, fixing some bugs and design issues along the way.

## High level procedure
* Connect to google cloud storage
* Run validation procedure for known buckets
* These validation procedures are similar but not identical
* Report success/failure for each bucket

## Things to be addressed
* Python script needs to be restarted from the beginning if a download fails partway through
  * Catch a failing download partway through and just restart for that file
  * Serialize progress somehow so restarting the utility resumes from where it left off
* 