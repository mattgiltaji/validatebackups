package main

import (
	"context"
	"encoding/json"
	"os"

	"cloud.google.com/go/storage"
	"github.com/juju/errors"
	"google.golang.org/api/iterator"
)

func loadConfigurationFromFile(filePath string) (config Config, err error) {
	configFile, err := os.Open(filePath)
	defer configFile.Close()
	if err != nil {
		err = errors.Annotatef(err, "Unable to open config file at %s", filePath)
		return
	}
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	return
}

func validateBucket(bucket *storage.BucketHandle, ctx context.Context, config Config) (err error) {
	//match bucket with appropriate validator from config
	return
}

func getBucketTopLevelDirs(bucket *storage.BucketHandle, ctx context.Context) (dirs []string, err error) {
	bucketAttrs, err := bucket.Attrs(ctx)
	if err != nil {
		err = errors.Annotate(err, "Unable to determine bucket name when getting top level dirs.")
		return
	}
	bucketName := bucketAttrs.Name

	topLevelDirQuery := storage.Query{Delimiter: "/", Versions: false}
	it := bucket.Objects(ctx, &topLevelDirQuery)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotatef(err2, "Unable to get top level dirs of bucket %s", bucketName)
			return
		}
		dirs = append(dirs, objAttrs.Prefix)
	}
	return
}
