package main

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"cloud.google.com/go/storage"
	"github.com/juju/errors"
	"google.golang.org/api/iterator"
	"gopkg.in/cheggaaa/pb.v1"
)

func loadConfigurationFromFile(filePath string) (config Config, err error) {
	configFile, openErr := os.Open(filePath)
	defer func() {
		closeErr := configFile.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close config file at %s: %w", filePath, closeErr))
		}
	}()
	if openErr != nil {
		err = fmt.Errorf("unable to open config file at %s: %v", filePath, openErr)
		return
	}
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	return
}

func validateBucketsInConfig(ctx context.Context, client *storage.Client, config Config) (success bool, err error) {
	totalBuckets := len(config.Buckets)
	for i, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		//validate the bucket, if the type merits it
		fmt.Println(fmt.Sprintf("Validating files in bucket %d of %d, %s", i+1, totalBuckets, bucketConfig.Name))
		err = validateBucket(ctx, bucket, config)
		//TODO: have this function return success/failure so we only stop processing on an error and not just a failed validation
		if err != nil {
			return false, fmt.Errorf("unable to validate bucket %s: %w", bucketConfig.Name, err)
		}
	}
	return true, nil
}

func getObjectsToDownloadFromBucketsInConfig(ctx context.Context, client *storage.Client, config Config) ([]BucketAndFiles, error) {
	totalBuckets := len(config.Buckets)
	bucketToFilesMapping := make([]BucketAndFiles, len(config.Buckets))
	for i, bucketConfig := range config.Buckets {
		bucket := client.Bucket(bucketConfig.Name)
		fmt.Println(fmt.Sprintf("Getting files to download from bucket %d of %d, %s", i+1, totalBuckets, bucketConfig.Name))
		files, err := getObjectsToDownloadFromBucket(ctx, bucket, config)
		if err != nil {
			return nil, fmt.Errorf("could not get objects to download from bucket %s: %w", bucketConfig.Name, err)
		}
		bucketToFilesMapping[i] = BucketAndFiles{BucketName: bucketConfig.Name, Files: files}
	}
	return bucketToFilesMapping, nil
}

func saveInProgressFile(filePath string, data []BucketAndFiles) (err error) {
	jsonFile, createErr := os.Create(filePath)
	defer func() {
		closeErr := jsonFile.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close in progress file at %s: %w", filePath, closeErr))
		}
	}()
	if createErr != nil {
		err = fmt.Errorf("unable to open downloadsInProgress file %s for saving data: %w", filePath, createErr)
	}

	jsonEncoder := json.NewEncoder(jsonFile)
	err = jsonEncoder.Encode(data)
	return
}

func loadInProgressFile(filePath string) (data []BucketAndFiles, err error) {
	inProgressFile, openErr := os.Open(filePath)
	defer func() {
		closeErr := inProgressFile.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close in progress file at %s: %w", filePath, closeErr))
		}
	}()
	if openErr != nil {
		err = fmt.Errorf("unable to open in progress file at %s: %w", filePath, openErr)
		return
	}
	jsonParser := json.NewDecoder(inProgressFile)
	err = jsonParser.Decode(&data)
	return
}

func downloadFilesFromBucketAndFiles(ctx context.Context, client *storage.Client, config Config, mapping []BucketAndFiles) (err error) {
	totalBuckets := len(mapping)
	for i, bucketAndFiles := range mapping {
		bucket := client.Bucket(bucketAndFiles.BucketName)
		fmt.Println(fmt.Sprintf("Downloading files in bucket %d of %d, %s", i+1, totalBuckets, bucketAndFiles.BucketName))
		err := downloadFilesFromBucket(ctx, bucket, bucketAndFiles.Files, config)
		if err != nil {
			return fmt.Errorf("error while downloading files for bucket %s: %w", bucketAndFiles.BucketName, err)
		}
	}
	return
}

func validateBucket(ctx context.Context, bucket *storage.BucketHandle, config Config) (err error) {
	//match bucket with appropriate validator from config
	bucketName, err := getBucketName(ctx, bucket)
	if err != nil {
		err = fmt.Errorf("unable to determine bucket name when validating: %w", err)
		return
	}
	validationType, err := getBucketValidationTypeFromNameAndConfig(bucketName, config.Buckets)
	switch validationType {
	case "media": //no validations for this type
	case "photo": //no validations for this type
	case "server-backup":
		err = validateServerBackups(ctx, bucket, config.ServerBackupRules)
		if err != nil {
			err = fmt.Errorf("error validating bucket %s as type %s: %w", bucketName, validationType, err)
			return
		}
	default:
		err = errors.NotFoundf(
			"No matching validation logic for bucket %s with validation type %s", bucketName, validationType)
	}
	return
}

func getObjectsToDownloadFromBucket(ctx context.Context, bucket *storage.BucketHandle, config Config) (objects []string, err error) {
	bucketName, err := getBucketName(ctx, bucket)
	if err != nil {
		err = fmt.Errorf("unable to determine bucket name when validating: %w", err)
		return
	}
	validationType, err := getBucketValidationTypeFromNameAndConfig(bucketName, config.Buckets)
	switch validationType {
	case "media":
		objects, err = getMediaFilesToDownload(ctx, bucket, config.FilesToDownload)
		if err != nil {
			err = fmt.Errorf("error getting list of media files to download from %s: %w", bucketName, err)
			return
		}
	case "photo":
		objects, err = getPhotosToDownload(ctx, bucket, config.FilesToDownload)
		if err != nil {
			err = fmt.Errorf("error getting list of photos to download from %s: %w", bucketName, err)
			return
		}
	case "server-backup":
		objects, err = getServerBackupsToDownload(ctx, bucket, config.FilesToDownload)
		if err != nil {
			err = fmt.Errorf("error getting list of server backups to download from %s: %v", bucketName, err)
			return
		}
	default:
		err = errors.NotFoundf(
			"No matching objects to download logic for bucket %s with validation type %s", bucketName, validationType)
	}
	return
}

func downloadFilesFromBucket(ctx context.Context, bucket *storage.BucketHandle, filesToDownload []string, config Config) (err error) {
	bucketName, err := getBucketName(ctx, bucket)
	if err != nil {
		err = fmt.Errorf("unable to load bucket name for determining destination directory: %w", err)
	}
	totalFiles := len(filesToDownload)
	photoFileNameRegex, _ := regexp.Compile("([0-9][0-9][0-9][0-9])-[0-9][0-9]/(.*)")
	for i, remoteFile := range filesToDownload {

		var localFile string
		//for photos downloads, put them locally in yyyy, not in yyyy-mm
		if photoFileNameRegex.MatchString(remoteFile) {
			localFileParts := photoFileNameRegex.FindStringSubmatch(remoteFile)
			localFile = filepath.Join(config.FileDownloadLocation, bucketName, localFileParts[1], localFileParts[2])
		} else {
			localFile = filepath.Join(config.FileDownloadLocation, bucketName, remoteFile)
		}

		retryCount := 0
		fmt.Println(fmt.Sprintf("Downloading %d of %d, %s", i+1, totalFiles, remoteFile))
		for {
			err2 := downloadFile(ctx, bucket, remoteFile, localFile)
			if err2 == nil {
				//download successful!
				break
			}
			if errors.Is(err2, errors.AlreadyExists) {
				//download successful!
				fmt.Println("Skipping already downloaded file.")
				break
			}
			if errors.Is(err2, errors.NotFound) {
				//no sense retrying if we can't find the file
				err = fmt.Errorf("could not find %s to download it: %w", remoteFile, err2)
				return
			}
			retryCount++
			if retryCount > config.MaxDownloadRetries {
				err = fmt.Errorf("could not download %s after retrying max number of times: %w", remoteFile, err2)
				return
			}
			fmt.Println(fmt.Sprintf("Failed, retry %d of %d.", retryCount, config.MaxDownloadRetries))
		}
	}
	return
}

func validateServerBackups(ctx context.Context, bucket *storage.BucketHandle, rules ServerFileValidationRules) (err error) {

	oldestObjAttrs, err := getOldestObjectFromBucket(ctx, bucket)
	if err != nil || oldestObjAttrs == nil {
		return fmt.Errorf("unable to get oldest object in bucket %w", err)
	}
	oldestFileAge := time.Since(oldestObjAttrs.Created)
	oldestFileAgeInDays := int(oldestFileAge / (time.Hour * 24)) //this may not be 100% accurate due to daylight savings time and whatnot, but close enough
	if oldestFileAgeInDays >= rules.OldestFileMaxAgeInDays {
		return errors.NotValidf(
			"Oldest file %s was created on %v, too long in the past. Check backup file archiving.", oldestObjAttrs.Name, oldestObjAttrs.Created)
	}

	newestObjAttrs, err := getNewestObjectFromBucket(ctx, bucket)
	if err != nil || newestObjAttrs == nil {
		return fmt.Errorf("unable to get newest object in bucket: %w", err)
	}
	newestFileAge := time.Since(newestObjAttrs.Created)
	newestFileAgeInDays := int(newestFileAge / (time.Hour * 24)) //this may not be 100% accurate due to daylight savings time and whatnot, but close enough
	if newestFileAgeInDays >= rules.NewestFileMaxAgeInDays {
		return errors.NotValidf(
			"Newest file %s was created on %v, too long in the past. Make sure backups are running", newestObjAttrs.Name, newestObjAttrs.Created)
	}

	//TODO: should this return a bool up the chain instead of an err?
	return nil
}

func getMediaFilesToDownload(ctx context.Context, bucket *storage.BucketHandle, rules FileDownloadRules) (mediaFiles []string, err error) {
	shows, err := getBucketTopLevelDirs(ctx, bucket) //each top level directory in a media bucket represents a show
	if err != nil {
		err = fmt.Errorf("unable to determine shows in media bucket: %w", err)
		return
	}
	for _, show := range shows {
		partialFiles, err2 := getRandomFilesFromBucket(ctx, bucket, rules.EpisodesFromEachShow, show)
		if err2 != nil {
			err = fmt.Errorf("unable to get %d random files from show %s in media bucket: %w", rules.EpisodesFromEachShow, show, err2)
			return
		}
		mediaFiles = append(mediaFiles, partialFiles...)
	}
	return
}

func getPhotosToDownload(ctx context.Context, bucket *storage.BucketHandle, rules FileDownloadRules) (photos []string, err error) {
	currYear := time.Now().Year()

	//each year, get rules.PhotosFromEachYear photos from that yeah, randomly selected
	for year := 2010; year <= currYear; year++ {
		partialPhotos, err2 := getRandomFilesFromBucket(ctx, bucket, rules.PhotosFromEachYear, fmt.Sprintf("%d-", year))
		if err2 != nil {
			err = fmt.Errorf("unable to get %d random files from year %d in photo bucket: %w", rules.EpisodesFromEachShow, year, err2)
			return
		}
		photos = append(photos, partialPhotos...)
	}

	//for this month, get rules.PhotosFromThisMonth photos from this month, randomly selected
	partialPhotos, err := getRandomFilesFromBucket(ctx, bucket, rules.PhotosFromThisMonth, fmt.Sprintf("%d-%02d", currYear, time.Now().Month()))
	if err != nil {
		err = fmt.Errorf("unable to get %d random files from this month %s in photo bucket: %w",
			rules.PhotosFromThisMonth, fmt.Sprintf("%d-%02d", currYear, time.Now().Month()), err)
		return
	}
	photos = append(photos, partialPhotos...)

	return
}

func getServerBackupsToDownload(ctx context.Context, bucket *storage.BucketHandle, rules FileDownloadRules) (backups []string, err error) {
	//get the most recent rules.ServerBackups backup files
	//get all the files
	it := bucket.Objects(ctx, nil)

	files := make([]*storage.ObjectAttrs, rules.ServerBackups)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if errors.Is(err2, iterator.Done) {
			break
		}
		if err2 != nil {
			err = fmt.Errorf("unable to get random sample from bucket: %w", err2)
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
		err = errors.NotFoundf(
			"Unable to find %d most recent files because there were not enough files in bucket", rules.ServerBackups)
		return
	}

	//now that everything is done, convert to file names
	for _, file := range files {
		backups = append(backups, file.Name)
	}
	return
}

func getBucketName(ctx context.Context, bucket *storage.BucketHandle) (name string, err error) {
	bucketAttrs, err := bucket.Attrs(ctx)
	if err != nil {
		err = fmt.Errorf("unable to determine bucket name: %w", err)
		return
	}
	name = bucketAttrs.Name
	return
}

func getBucketTopLevelDirs(ctx context.Context, bucket *storage.BucketHandle) (dirs []string, err error) {
	topLevelDirQuery := storage.Query{Delimiter: "/", Versions: false}
	it := bucket.Objects(ctx, &topLevelDirQuery)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if errors.Is(err2, iterator.Done) {
			break
		}
		if err2 != nil {
			err = fmt.Errorf("unable to get top level dirs of bucket: %w", err)
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
	return "", errors.NotFoundf("Unable to find validation type for bucket named %s in config %v", name, configs)
}

func getNewestObjectFromBucket(ctx context.Context, bucket *storage.BucketHandle) (newestObjectAttrs *storage.ObjectAttrs, err error) {
	it := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if errors.Is(err2, iterator.Done) {
			break
		}
		if err2 != nil {
			err = fmt.Errorf("unable to get newest object from bucket: %w", err2)
			return
		}
		if newestObjectAttrs == nil || objAttrs.Created.After(newestObjectAttrs.Created) {
			newestObjectAttrs = objAttrs
		}
	}
	return
}

func getOldestObjectFromBucket(ctx context.Context, bucket *storage.BucketHandle) (oldestObjectAttrs *storage.ObjectAttrs, err error) {
	it := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if errors.Is(err2, iterator.Done) {
			break
		}
		if err2 != nil {
			err = fmt.Errorf("unable to get oldest object from bucket %w", err2)
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
func getRandomFilesFromBucket(ctx context.Context, bucket *storage.BucketHandle, num int, prefix string) (fileNames []string, err error) {
	if num < 0 {
		err = errors.NotValidf("Cannot return negative number of random files.")
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
	bannedNameRegex := regexp.MustCompile(".*[aA][aA][eE]")
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if errors.Is(err2, iterator.Done) {
			break
		}
		if err2 != nil {
			err = fmt.Errorf("unable to get random sample from bucket: %w", err2)
			return
		}
		if bannedNameRegex.MatchString(objAttrs.Name) {
			continue
		}
		objects = append(objects, objAttrs)
	}
	population := len(objects)
	if num > population {
		err = errors.NotFoundf("Not enough files in bucket to return requested sample size %d.", num)
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

func downloadFile(ctx context.Context, bucket *storage.BucketHandle, remoteFilePath string, localFilePath string) (err error) {
	obj := bucket.Object(remoteFilePath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return errors.NotFoundf("Unable to find file in bucket at %s", remoteFilePath)
	}

	//if the file already exists and is valid, skip it
	err = verifyDownloadedFile(attrs, localFilePath)
	if err == nil {
		//file already downloaded
		return errors.AlreadyExistsf("File %s has already been downloaded successfully.", localFilePath)
	}

	rc, err := obj.NewReader(ctx)
	defer func() {
		closeErr := rc.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close remote reader at %s: %w", remoteFilePath, closeErr))
		}
	}()
	if err != nil {
		return errors.NotFoundf("Unable to download file at %s", remoteFilePath)
	}

	//prep file
	err = os.MkdirAll(filepath.Dir(localFilePath), os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to make directory %s: %w", localFilePath, err)
	}

	localFile, err := os.Create(localFilePath)
	defer func() {
		closeErr := localFile.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close file at %s: %w", localFilePath, closeErr))
		}
	}()
	if err != nil {
		return fmt.Errorf("unable to open file %s for saving data from bucket: %w", localFilePath, err)
	}

	//prep progress bar
	bar := pb.New(int(attrs.Size)).SetUnits(pb.U_BYTES)
	bar.Start()
	reader := bar.NewProxyReader(rc)
	//download it

	_, err = io.Copy(localFile, reader)
	bar.Finish()
	if err != nil {
		return fmt.Errorf("error saving data to file %s: %w", localFilePath, err)
	}

	return verifyDownloadedFile(attrs, localFilePath)
}

func verifyDownloadedFile(objAttrs *storage.ObjectAttrs, filePath string) (err error) {
	if objAttrs == nil {
		return errors.NotValidf("Cannot validate file %s against an invalid object attr record.", filePath)
	}

	//compare expected size vs actual
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return errors.NotFoundf("Cannot validate file that doesn't exist.")
	}

	if objAttrs.Size != fileInfo.Size() {
		return errors.NotValidf("Size mismatch, expected %d found %d", objAttrs.Size, fileInfo.Size())
	}

	//compare CRC32C expected vs actual
	localCRC, err := getCrc32CFromFile(filePath)
	remoteCRC := objAttrs.CRC32C
	if remoteCRC != localCRC {
		return errors.NotValidf("Bad CRC, expected %d found %d", remoteCRC, localCRC)
	}
	return
}

// getCrc32CFromFile calculates theCRC32 checksum of the file's contents using the Castagnoli93 polynomial
func getCrc32CFromFile(filePath string) (crc uint32, err error) {
	//originally from http://mrwaggel.be/post/generate-crc32-hash-of-a-file-in-golang-turorial/
	//modified to new golang error handling since then
	file, err := os.Open(filePath)
	defer func() {
		closeErr := file.Close()
		if closeErr != nil {
			err = stderrors.Join(err, fmt.Errorf("unable to close file at %s: %w", filePath, closeErr))
		}
	}()
	if err != nil {
		err = fmt.Errorf("unable to open file %s to calculate CRC32C: %w", filePath, err)
		return
	}

	tablePolynomial := crc32.MakeTable(crc32.Castagnoli)
	hash := crc32.New(tablePolynomial)

	_, err = io.Copy(hash, file)
	if err != nil {
		err = fmt.Errorf("unable to hash file %s to calculate CRC32C: %w", filePath, err)
		return
	}

	crc = hash.Sum32()
	return
}
