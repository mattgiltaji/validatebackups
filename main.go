package main

import (
	"context"
	"encoding/json"
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

	//load config from file
	config, err := loadConfigurationFromFile(*configPath)
	logFatalIfErr(err, "Unable to load configuration from file.")

	//connect to gcs
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(config.GoogleAuthFileLocation))
	logFatalIfErr(err, "Unable to connect to google cloud storage.")

	//loop over relevant buckets for validating
	success, err := validateBucketsInConfig(client, ctx, config)
	logFatalIfErr(err, "Unable to validate all buckets.")
	if success {
		fmt.Println("All buckets have passed validation.")
	}

	//now see if we have files to download already
	//TODO: add check for in progress file

	//if not, make that file
	//TODO: refactor into getObjectsToDownloadFromBucketsInConfig method
	//TODO: move to validateBackups and add test coverage
	bucketToFilesMapping := make([]BucketAndFiles, len(config.Buckets))
	for i, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		//validate the bucket, if the type merits it
		files, err := getObjectsToDownloadFromBucket(bucket, ctx, config)
		logFatalIfErr(err, fmt.Sprintf("Bucket %s failed validation.", bucketConfig.Name))
		bucketToFilesMapping[i] = BucketAndFiles{BucketName: bucketConfig.Name, Files: files}
	}
	//serialize bucketToFilesMapping to json file
	saveInProgressFile(bucketToFilesMapping)

	//now go over the file contents and download the objects locally
	//ideally give some progress indicator : downloading X/Y files for bucket X
	return
}

func saveInProgressFile(data []BucketAndFiles) {
	//TODO: clean up error handling to do annotate calls and raise error up chain
	//TODO: move to validateBackups and add test coverage
	jsonData, err := json.Marshal(data)
	logFatalIfErr(err, "Unable to marshal file mapping to json")
	jsonFile, err := os.Create("./downloadsInProgress.json")
	logFatalIfErr(err, "Unable to open downloadsInProgress file for saving data.")
	defer jsonFile.Close()
	_, err = jsonFile.Write(jsonData)
	logFatalIfErr(err, "Unable to save data to downloadsInProgress file.")
}

func logFatalIfErr(err error, msg string) {
	if err != nil {
		log.Fatal(msg, " Error: ", err.Error())
	}
}
