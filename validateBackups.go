package main

import (
	"context"
	"encoding/json"
	"os"

	"cloud.google.com/go/storage"
	"github.com/juju/errors"
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
	return
}
