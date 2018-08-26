# validatebackups
Validates some personal google cloud backups

[![Build Status](https://travis-ci.org/mattgiltaji/validatebackups.svg?branch=master)](https://travis-ci.org/mattgiltaji/validatebackups)
[![Build status](https://ci.appveyor.com/api/projects/status/sliy4g7kdjr2cxis/branch/master?svg=true)](https://ci.appveyor.com/project/mattgiltaji/validatebackups/branch/master)
[![Coverage Status](https://coveralls.io/repos/github/mattgiltaji/validatebackups/badge.svg?branch=master)](https://coveralls.io/github/mattgiltaji/validatebackups?branch=master)
[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

This utility is intended to download a sample of random files from backup locations.
These files can then be manually verified to make sure the backups are working.

This is a port of
[my Python script of the same name](https://github.com/mattgiltaji/miscutils/tree/master/validatebackups)
to Go, fixing some bugs and design issues along the way.

## High level procedure
- [x] Connect to google cloud storage
- [x] Run validation procedure for known buckets
- [x] These validation procedures are similar but not identical
- [ ] Download files that need to be manually validated
- [ ] Report success/failure for each bucket

## Things to be addressed
* Python script needs to be restarted from the beginning if a download fails partway through.
  * Catch a failing download partway through and just restart for that file.
  * Serialize progress somehow so restarting the utility resumes from where it left off.
* ~~Python script's unit test suite runs integration tests that depend on accessing google cloud.~~
  * Go's mocking couldn't quite handle abstracting this away, so we connect to google cloud too ¯\\_(ツ)_/¯