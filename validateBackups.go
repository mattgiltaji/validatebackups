package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
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
	oldestFileAge := time.Since(oldestObjAttrs.Created)
	oldestFileAgeInDays := int(oldestFileAge / (time.Hour * 24)) //this may not be 100% accurate due to daylight savings time and whatnot, but close enough
	if oldestFileAgeInDays >= rules.OldestFileMaxAgeInDays {
		return errors.New(fmt.Sprintf("Oldest file %s was created on %v, too long in the past. Check backup file archiving.", oldestObjAttrs.Name, oldestObjAttrs.Created))
	}

	newestObjAttrs, err := getNewestObjectFromBucket(bucket, ctx)
	if err != nil || newestObjAttrs == nil {
		return errors.Annotate(err, "Unable to get newest object in bucket")
	}
	newestFileAge := time.Since(newestObjAttrs.Created)
	newestFileAgeInDays := int(newestFileAge / (time.Hour * 24)) //this may not be 100% accurate due to daylight savings time and whatnot, but close enough
	if newestFileAgeInDays >= rules.NewestFileMaxAgeInDays {
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
		partialFiles, err2 := getRandomFilesFromBucket(bucket, ctx, rules.EpisodesFromEachShow, show)
		if err2 != nil {
			err = errors.Annotatef(err2, "Unable to get %d random files from show %s in media bucket", rules.EpisodesFromEachShow, show)
			return
		}
		mediaFiles = append(mediaFiles, partialFiles...)
	}
	return
}

func getPhotosToDownload(bucket *storage.BucketHandle, ctx context.Context, rules FileDownloadRules) (photos []string, err error) {
	currYear := time.Now().Year()

	//each year, get rules.PhotosFromEachYear photos from that yeah, randomly selected
	for year := 2010; year <= currYear; year++ {
		partialPhotos, err2 := getRandomFilesFromBucket(bucket, ctx, rules.PhotosFromEachYear, fmt.Sprintf("%d-", year))
		if err2 != nil {
			err = errors.Annotatef(err2, "Unable to get %d random files from year %d in photo bucket", rules.EpisodesFromEachShow, year)
			return
		}
		photos = append(photos, partialPhotos...)
	}

	//for this month, get rules.PhotosFromThisMonth photos from this month, randomly selected
	partialPhotos, err := getRandomFilesFromBucket(bucket, ctx, rules.PhotosFromThisMonth, fmt.Sprintf("%d-%02d", currYear, time.Now().Month()))
	if err != nil {
		err = errors.Annotatef(err, "Unable to get %d random files from this month %s in photo bucket",
			rules.PhotosFromThisMonth, fmt.Sprintf("%d-%02d", currYear, time.Now().Month()))
		return
	}
	photos = append(photos, partialPhotos...)

	return
}

func getServerBackupsToDownload(bucket *storage.BucketHandle, ctx context.Context, rules FileDownloadRules) (backups []string, err error) {
	//get the most recent rules.ServerBackups backup files
	//get all the files
	it := bucket.Objects(ctx, nil)

	files := make([]*storage.ObjectAttrs, rules.ServerBackups)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get random sample from bucket")
			return
		}
		//if they are part of the nth most recent, save them
		//TODO: optimize by checking last slot in files and don't loop if objAttrs don't have a chance of getting in
		for i, file := range files {
			if file == nil { //this spot is empty, objAttrs is recent by default
				files[i] = objAttrs
				break
			}
			if objAttrs.Created.After(files[i].Created) {
				//objAttrs is more recent, so swap spots so whatever was in files[i] can try for the next slot up
				files[i], objAttrs = objAttrs, files[i]
			}
		}
	}
	//some error handling
	if files[rules.ServerBackups-1] == nil {
		err = errors.New(fmt.Sprintf(
			"Unable to find %d most recent files because there were not enough files in bucket", rules.ServerBackups))
		return
	}

	//now that everything is done, convert to filenames
	for _, file := range files {
		backups = append(backups, file.Name)
	}
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

// GetRandomFilesFromBucket gets a random sample of objects from a bucket with no replacement.
// The Prefix parameter will filter the objects so all selections will have that prefix; when prefix == nil, objects will be chosen from the entire bucket.
// Randomness is not cryptographic strength.
func getRandomFilesFromBucket(bucket *storage.BucketHandle, ctx context.Context, num int, prefix string) (fileNames []string, err error) {
	if num < 0 {
		err = errors.New(fmt.Sprintf("Cannot return negative number of random files."))
		return
	}
	if num == 0 {
		//no files wanted, nothing to do
		return
	}
	//get the list of matching objects

	var q storage.Query
	if len(prefix) == 0 {
		q = storage.Query{Versions: false}
	} else {
		q = storage.Query{Prefix: prefix, Versions: false}
	}
	it := bucket.Objects(ctx, &q)

	//put them into a massive slice
	var objects []*storage.ObjectAttrs
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get random sample from bucket")
			return
		}
		objects = append(objects, objAttrs)
	}
	population := len(objects)
	if num > population {
		err = errors.New(fmt.Sprintf("Not enough files in bucket to return requested sample size %d.", num))
		return
	}

	files := make([]string, num)
	//figure out which indices will be selected
	if num == population {
		// no need to do randomness, whole population will be returned
		for i, obj := range objects {
			files[i] = obj.Name
		}
		return files, nil
	}
	selections := getRandomSampleFromPopulation(num, population)

	for i := 0; i < num; i++ {
		files[i] = objects[selections[i]].Name
	}
	return files, nil
}

func getRandomSampleFromPopulation(sampleSize, population int) []int {
	if sampleSize > population || sampleSize <= 0 {
		//this will get stuck in an infinite loop if we don't exit early
		return nil
	}
	sample := make([]int, sampleSize)
	i := 0
	for { //deconstructed for loop so we can repeat iterations until we have a non-dupe
		if i >= sampleSize {
			break
		}
		selection := rand.Int() % population
		//make sure this is not already in the previous selections
		dupe := false
		for j := 0; j < i; j++ {
			if selection == sample[j] {
				dupe = true
				break
			}
		}
		if dupe {
			continue
		}
		sample[i] = selection
		i++
	}
	return sample
}
