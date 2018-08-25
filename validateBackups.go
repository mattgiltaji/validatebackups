package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

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
	validationType, err := getBucketValidationTypeFromNameAndConfig(bucketName, config.Buckets)
	switch validationType {
	case "media": //no validations for this type
	case "photo": //no validations for this type
	case "server-backup":
		err = validateServerBackups(bucket, ctx, config.ServerBackupRules)
		if err != nil {
			err = errors.Annotatef(err, "Error validating bucket %s as type %s", bucketName, validationType)
			return
		}
	default:
		err = errors.New(fmt.Sprintf("No matching validation logic for bucket %s with validation type %s", bucketName, validationType))
	}
	return
}

func getObjectsToDownloadFromBucket(bucket *storage.BucketHandle, ctx context.Context, config Config) (objects []string, err error) {
	bucketName, err := getBucketName(bucket, ctx)
	if err != nil {
		err = errors.Annotate(err, "Unable to determine bucket name when validating.")
		return
	}
	validationType, err := getBucketValidationTypeFromNameAndConfig(bucketName, config.Buckets)
	switch validationType {
	case "media":
		objects, err = getMediaFilesToDownload(bucket, ctx, config.FilesToDownload)
		if err != nil {
			err = errors.Annotatef(err, "Error getting list of media files to download from %s", bucketName)
			return
		}
	case "photo":
		objects, err = getPhotosToDownload(bucket, ctx, config.FilesToDownload)
		if err != nil {
			err = errors.Annotatef(err, "Error getting list of photos to download from %s", bucketName)
			return
		}
	case "server-backup":
		objects, err = getServerBackupsToDownload(bucket, ctx, config.FilesToDownload)
		if err != nil {
			err = errors.Annotatef(err, "Error getting list of server backups to download from %s", bucketName)
			return
		}
	default:
		err = errors.New(fmt.Sprintf("No matching objects to download logic for bucket %s with validation type %s", bucketName, validationType))
	}
	return
}

func validateServerBackups(bucket *storage.BucketHandle, ctx context.Context, rules ServerFileValidationRules) (err error) {

	oldestObjAttrs, err := getOldestObjectFromBucket(bucket, ctx)
	if err != nil || oldestObjAttrs == nil {
		return errors.Annotate(err, "Unable to get oldest object in bucket")
	}
	oldestFileMaxValidTimestamp := time.Now().AddDate(0, 0, rules.OldestFileMaxAgeInDays)
	if oldestObjAttrs.Created.After(oldestFileMaxValidTimestamp) {
		return errors.New(fmt.Sprintf("Oldest file %s was created on %v, too long in the past. Check backup file archiving.", oldestObjAttrs.Name, oldestObjAttrs.Created))
	}

	newestObjAttrs, err := getNewestObjectFromBucket(bucket, ctx)
	if err != nil || newestObjAttrs == nil {
		return errors.Annotate(err, "Unable to get newest object in bucket")
	}
	newestFileMaxValidTimestamp := time.Now().AddDate(0, 0, rules.NewestFileMaxAgeInDays)
	if newestObjAttrs.Created.After(newestFileMaxValidTimestamp) {
		return errors.New(fmt.Sprintf("Newest file %s was created on %v, too long in the past. Make sure backups are running", newestObjAttrs.Name, newestObjAttrs.Created))
	}

	//TODO: should this return a bool up the chain instead of an err?
	return nil
}

func getMediaFilesToDownload(bucket *storage.BucketHandle, ctx context.Context, rules FileDownloadRules) (mediaFiles []string, err error) {
	shows, err := getBucketTopLevelDirs(bucket, ctx) //each top level directory in a media bucket represents a show
	if err != nil {
		err = errors.Annotate(err, "Unable to determine shows in media bucket")
		return
	}
	for _, show := range shows {
		//get rules.EpisodesFromEachShow objects from this directory in the bucket, randomly selected
		fmt.Printf("Getting %d episodes from show %s", rules.EpisodesFromEachShow, show)
		//TODO: actually get the shows to download
	}
	return
}

func getPhotosToDownload(bucket *storage.BucketHandle, ctx context.Context, rules FileDownloadRules) (photos []string, err error) {
	//loop over years from 2010 to present,
	//each year, get rules.PhotosFromEachYear photos from that yeah, randomly selected
	//for this month, get rules.PhotosFromThisMonth photos from this month, randomly selected
	//TODO: actually get the photos to download
	return
}

func getServerBackupsToDownload(bucket *storage.BucketHandle, ctx context.Context, rules FileDownloadRules) (backups []string, err error) {
	//get the most recent rules.ServerBackups backup files
	//TODO: actually get the backups to download
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

func getBucketTopLevelDirs(bucket *storage.BucketHandle, ctx context.Context) (dirs []string, err error) {
	topLevelDirQuery := storage.Query{Delimiter: "/", Versions: false}
	it := bucket.Objects(ctx, &topLevelDirQuery)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get top level dirs of bucket")
			return
		}
		dirs = append(dirs, objAttrs.Prefix)
	}
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

func getNewestObjectFromBucket(bucket *storage.BucketHandle, ctx context.Context) (newestObjectAttrs *storage.ObjectAttrs, err error) {
	it := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get newest object from bucket")
			return
		}
		if newestObjectAttrs == nil || objAttrs.Created.After(newestObjectAttrs.Created) {
			newestObjectAttrs = objAttrs
		}
	}
	return
}

func getOldestObjectFromBucket(bucket *storage.BucketHandle, ctx context.Context) (oldestObjectAttrs *storage.ObjectAttrs, err error) {
	it := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get oldest object from bucket")
			return
		}
		if oldestObjectAttrs == nil || objAttrs.Created.Before(oldestObjectAttrs.Created) {
			oldestObjectAttrs = objAttrs
		}
	}
	return
}
