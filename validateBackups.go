package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	bucketName, err := getBucketName(bucket, ctx)
	if err != nil {
		err = errors.Annotate(err, "Unable to determine bucket name when validating.")
		return
	}
	_, err = getBucketValidationTypeFromNameAndConfig(bucketName, config.Buckets)

	return
}

func getBucketName(bucket *storage.BucketHandle, ctx context.Context) (name string, err error) {
	bucketAttrs, err := bucket.Attrs(ctx)
	if err != nil {
		err = errors.Annotate(err, "Unable to determine bucket name.")
		return
	}
	name = bucketAttrs.Name
	return
}

func getBucketValidationTypeFromNameAndConfig(name string, configs []BucketToProcess) (string, error) {
	for _, config := range configs {
		if name == config.Name {
			return config.Type, nil
		}
	}
	return "", errors.New(fmt.Sprintf("Unable to find validation type for bucket named %s in config %v", name, configs))
}

func getBucketTopLevelDirs(bucket *storage.BucketHandle, ctx context.Context) (dirs []string, err error) {
	bucketName, err := getBucketName(bucket, ctx)
	if err != nil {
		err = errors.Annotate(err, "Unable to determine bucket name when getting top level dirs.")
		return
	}

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
