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

	//loop over relevant buckets
	for _, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		//validate the bucket, if the type merits it
		err = validateBucket(bucket, ctx, config)
		logFatalIfErr(err, fmt.Sprintf("Bucket %s failed validation.", bucketConfig.Name))

		//get the list of objects we'll download later and save it to a file
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
