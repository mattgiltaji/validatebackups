package main

import (
	"context"
	"flag"
	"fmt"
	"log"

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
	for _, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		//validate the bucket, if the type merits it
		err = validateBucket(bucket, ctx, config)
		logFatalIfErr(err, fmt.Sprintf("Bucket %s failed validation.", bucketConfig.Name))

		fmt.Printf("Bucket %s has passed validation", bucketConfig.Name)
	}

	//now see if we have files to download already
	//if not, make that file
	bucketToFilesMapping := make([]BucketAndFiles, len(config.Buckets))
	for i, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		//validate the bucket, if the type merits it
		files, err := getObjectsToDownloadFromBucket(bucket, ctx, config)
		logFatalIfErr(err, fmt.Sprintf("Bucket %s failed validation.", bucketConfig.Name))
		bucketToFilesMapping[i] = BucketAndFiles{BucketName: bucketConfig.Name, Files: files}
	}
	//serialize bucketToFilesMapping to json file

	//now go over the file contents and download the objects locally
	//ideally give some progress indicator : downloading X/Y files for bucket X
	return
}

func logFatalIfErr(err error, msg string) {
	if err != nil {
		log.Fatal(msg, " Error: ", err.Error())
	}
}
