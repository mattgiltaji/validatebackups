package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

//separated out to exclude from coverage calculations as it's not testable
func main() {
	configPath := flag.String("config",
		`D:\Matt\go\src\github.com\mattgiltaji\validatebackups\config.json`,
		"path to config file")
	flag.Parse()

	const inProgressFilename = "./downloadsInProgress.json"

	//load config from file
	config, err := loadConfigurationFromFile(*configPath)
	logFatalIfErr(err, "Unable to load configuration from file.")

	//connect to gcs
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(config.GoogleAuthFileLocation))
	logFatalIfErr(err, "Unable to connect to google cloud storage.")

	//loop over relevant buckets for validating
	success, err := validateBucketsInConfig(ctx, client, config)
	logFatalIfErr(err, "Unable to validate all buckets.")
	if success {
		fmt.Println("All buckets have passed validation.")
	}

	//now see if we have files to download already
	_, err = os.Stat(inProgressFilename)
	if os.IsNotExist(err) {
		//we don't have any in progress files, so make it
		bucketToFilesMapping, err := getObjectsToDownloadFromBucketsInConfig(ctx, client, config)
		logFatalIfErr(err, "Unable to get objects to download from all buckets.")
		//serialize bucketToFilesMapping to json file
		err = saveInProgressFile(inProgressFilename, bucketToFilesMapping)
		logFatalIfErr(err, "Unable to get save in progress file.")
	}

	//now go over the file contents and download the objects locally
	//ideally give some progress indicator : downloading X/Y files for bucket X
	return
}

func logFatalIfErr(err error, msg string) {
	if err != nil {
		log.Fatal(msg, " Error: ", err.Error())
	}
}
