package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// ***** Helpers *****
func getTestClient(t *testing.T, ctx context.Context) (client *storage.Client) {
	var err error
	googleAuthFileName := "test-backup-validator-auth.json"
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory to load test auth file")
	}
	googleAuthFileLocation := filepath.Join(workingDir, googleAuthFileName)
	client, err = storage.NewClient(ctx, option.WithCredentialsFile(googleAuthFileLocation))
	if err != nil {
		t.Error("Could not connect to test storage instance")
	}
	return
}

func deleteExistingObjectsFromBucket(bucket *storage.BucketHandle, ctx context.Context) (err error) {
	it := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := it.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil || objAttrs == nil {
			return errors.Annotate(err2, "Unable to get object from bucket to delete it.")
		}
		object := bucket.Object(objAttrs.Name)
		if object == nil {
			return errors.Annotate(err2, "Unable to get object handle from bucket to delete it.")
		}
		object.Delete(ctx)
	}
	return
}

func uploadFreshServerBackupFile(bucket *storage.BucketHandle, ctx context.Context) (err error) {
	currFiles := bucket.Objects(ctx, nil)
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		objAttrs, err2 := currFiles.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get existing photos when preparing photos bucket")
			return
		}
		objAge := time.Since(objAttrs.Created)
		objAgeInDays := int(objAge / (time.Hour * 24)) //close enough

		if objAgeInDays <= 1 {
			//fresh enough file already exists
			return
		}
	}

	err = deleteExistingObjectsFromBucket(bucket, ctx)
	if err != nil {
		return errors.Annotate(err, "Unable to delete existing files when preparing backup bucket")
	}
	workingDir, err := os.Getwd()
	if err != nil {
		return errors.Annotate(err, "Could not determine current directory to prepare backup bucket")
	}
	filePath := filepath.Join(workingDir, "testdata", "newest.txt")
	err = uploadFileToBucket(bucket, ctx, filePath, "newest.txt")
	if err != nil {
		return errors.Annotate(err, "Unable to upload file when preparing backup bucket")
	}
	return
}

func uploadThisMonthPhotos(bucket *storage.BucketHandle, ctx context.Context) (err error) {
	const numPhotosToUpload = 10

	workingDir, err := os.Getwd()
	if err != nil {
		return errors.Annotate(err, "Could not determine current directory to prepare photos bucket")
	}
	filePath := filepath.Join(workingDir, "testdata", "Red_1x1.gif")
	baseUploadPath := fmt.Sprintf("%d-%02d", time.Now().Year(), time.Now().Month())
	currFiles := bucket.Objects(ctx, &storage.Query{Prefix: baseUploadPath, Versions: false})
	numFiles := 0
	for {
		//TODO: use ctx to cancel this mid-process if requested?
		_, err2 := currFiles.Next()
		if err2 == iterator.Done {
			break
		}
		if err2 != nil {
			err = errors.Annotate(err2, "Unable to get existing photos when preparing photos bucket")
			return
		}
		numFiles++
	}
	if numFiles >= numPhotosToUpload {
		return
	}

	for i := 1; i <= numPhotosToUpload; i++ {
		uploadPath := fmt.Sprintf("%s/IMG_%02d.gif", baseUploadPath, i)
		err = uploadFileToBucket(bucket, ctx, filePath, uploadPath)
		if err != nil {
			return errors.Annotate(err, "Unable to upload file when preparing photos bucket")
		}
	}
	//wait until they are uploaded successfully (if we had to upload them
	time.Sleep(10 * time.Second)
	return
}

func uploadFileToBucket(bucket *storage.BucketHandle, ctx context.Context, filePath string, uploadPath string) (err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return errors.Annotate(err, "Unable to open local file to upload it.")
	}
	defer f.Close()

	wc := bucket.Object(uploadPath).NewWriter(ctx)
	_, err = io.Copy(wc, f)
	if err != nil {
		return errors.Annotate(err, "Unable to open upload local file. Error in copying.")
	}
	err = wc.Close()
	if err != nil {
		return errors.Annotate(err, "Unable to close remote file after upload.")
	}
	return
}

// ***** Tests *****
var testFileConfigCases = []struct {
	filename string
	expected Config
}{
	//we should be able to handle every value being filled
	{"fullConfig.json", Config{
		GoogleAuthFileLocation: "over-there",
		FileDownloadLocation:   "where-should-the-files-go",
		ServerBackupRules: ServerFileValidationRules{
			OldestFileMaxAgeInDays: 32,
			NewestFileMaxAgeInDays: 17,
		},
		FilesToDownload: FileDownloadRules{
			ServerBackups:        1,
			EpisodesFromEachShow: 2,
			PhotosFromThisMonth:  3,
			PhotosFromEachYear:   4,
		},
		Buckets: []BucketToProcess{
			{Name: "bucket-one", Type: "media"},
			{Name: "bucket-two", Type: "photo"},
			{Name: "bucket-three", Type: "server-backup"},
		}},
	},
	//handle values added in any order in the config file
	{"differentOrderConfig.json", Config{
		GoogleAuthFileLocation: "over-there",
		FileDownloadLocation:   "where-should-the-files-go",
		ServerBackupRules: ServerFileValidationRules{
			OldestFileMaxAgeInDays: 32,
			NewestFileMaxAgeInDays: 17,
		},
		FilesToDownload: FileDownloadRules{
			ServerBackups:        1,
			EpisodesFromEachShow: 2,
			PhotosFromThisMonth:  3,
			PhotosFromEachYear:   4,
		},
		Buckets: []BucketToProcess{
			{Name: "bucket-one", Type: "media"},
			{Name: "bucket-two", Type: "photo"},
			{Name: "bucket-three", Type: "server-backup"},
		}},
	},
	//handle if some values are missing in the config file
	{"partialConfig.json", Config{
		GoogleAuthFileLocation: "over-here",
		ServerBackupRules: ServerFileValidationRules{
			OldestFileMaxAgeInDays: 10,
			NewestFileMaxAgeInDays: 2,
		},
		Buckets: []BucketToProcess{
			{Name: "bucket-a", Type: "photo"},
		}},
	},
	//handle an entirely empty config file
	{"emptyConfig.json", Config{}},
}

func TestLoadConfigurationFromFile(t *testing.T) {
	is := assert.New(t)

	//figure out path to the testdata directory
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}
	testDataDir := filepath.Join(workingDir, "testdata")

	for _, tc := range testFileConfigCases {
		expected := tc.expected
		actual, err := loadConfigurationFromFile(filepath.Join(testDataDir, tc.filename))
		is.Nil(err)
		is.Equal(expected.GoogleAuthFileLocation, actual.GoogleAuthFileLocation)
		is.Equal(expected.FileDownloadLocation, actual.FileDownloadLocation)
		is.Equal(expected.FilesToDownload, actual.FilesToDownload)
		is.Equal(expected.ServerBackupRules, actual.ServerBackupRules)
		is.Equal(expected.Buckets, actual.Buckets)
	}

	_, err = loadConfigurationFromFile(filepath.Join(testDataDir, "doesNotExist.json"))
	is.Error(err, "Should error out when reading config from a file that doesn't exist.")

	_, err = loadConfigurationFromFile(filepath.Join(testDataDir, "parseErrorConfig.json"))
	is.Error(err, "Should error out if the config file cannot be parsed.")
}

func TestValidateBucketsInConfig(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)

	config := Config{
		ServerBackupRules: ServerFileValidationRules{
			OldestFileMaxAgeInDays: 10,
			NewestFileMaxAgeInDays: 2,
		},
		Buckets: []BucketToProcess{
			{Name: "test-matt-media", Type: "media"},
			{Name: "test-matt-photos", Type: "photo"},
			{Name: "test-matt-server-backups-fresh", Type: "server-backup"},
		}}
	backupBucket := testClient.Bucket("test-matt-server-backups-fresh")
	err := uploadFreshServerBackupFile(backupBucket, ctx)
	if err != nil {
		t.Error("Could not prep test case for validating server backup bucket.")
	}
	photosBucket := testClient.Bucket("test-matt-photos")
	err = uploadThisMonthPhotos(photosBucket, ctx)
	if err != nil {
		t.Error("Could not prep test case for validating photos bucket.")
	}

	actual, err := validateBucketsInConfig(testClient, ctx, config)
	is.NoError(err, "Should not error when validating good bucket types")
	is.True(actual, "Should return true when validations are successful")

	missingBucketName := "does-not-exist"
	config.Buckets = []BucketToProcess{{Name: missingBucketName, Type: "media"}}
	actual, missingBucketErr := validateBucketsInConfig(testClient, ctx, config)
	is.Error(missingBucketErr, "Should error when config has a bucket that doesn't exist")
	is.False(actual, "Should return false if there is an error during validation")

}

func TestValidateBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)

	config := Config{
		ServerBackupRules: ServerFileValidationRules{
			OldestFileMaxAgeInDays: 10,
			NewestFileMaxAgeInDays: 2,
		},
		Buckets: []BucketToProcess{
			{Name: "test-matt-media", Type: "media"},
			{Name: "test-matt-photos", Type: "photo"},
			{Name: "test-matt-server-backups-fresh", Type: "server-backup"},
		}}
	backupBucket := testClient.Bucket("test-matt-server-backups-fresh")
	err := uploadFreshServerBackupFile(backupBucket, ctx)
	if err != nil {
		t.Error("Could not prep test case for validating server backup bucket.")
	}

	for _, tb := range config.Buckets {
		bucket := testClient.Bucket(tb.Name)
		err := validateBucket(bucket, ctx, config)
		is.NoError(err, "Should not error when validating a bucket type that passes validations")
	}

	missingBucketName := "does-not-exist"
	missingBucket := testClient.Bucket(missingBucketName)
	missingBucketErr := validateBucket(missingBucket, ctx, config)
	is.Error(missingBucketErr, "Should error when validating a bucket that doesn't exist")

	missingValidationTypeBucketName := "test-matt-empty"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: missingValidationTypeBucketName, Type: "empty"})
	missingValidationTypeBucket := testClient.Bucket(missingValidationTypeBucketName)
	missingValidationTypeErr := validateBucket(missingValidationTypeBucket, ctx, config)
	is.Error(missingValidationTypeErr, "Should error when validation type doesn't have matching validation logic")

	failBucketName := "test-matt-server-backups"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: failBucketName, Type: "server-backup"})
	failBucket := testClient.Bucket(failBucketName)
	failBucketErr := validateBucket(failBucket, ctx, config)
	is.Error(failBucketErr, "Should error when validations fail")
}

func TestGetObjectsToDownloadFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)

	config := Config{
		FilesToDownload: FileDownloadRules{
			ServerBackups:        4,
			EpisodesFromEachShow: 3,
			PhotosFromThisMonth:  5,
			PhotosFromEachYear:   10,
		},
		Buckets: []BucketToProcess{
			{Name: "test-matt-media", Type: "media"},
			{Name: "test-matt-photos", Type: "photo"},
			{Name: "test-matt-server-backups", Type: "server-backup"},
		}}

	for _, tb := range config.Buckets {
		bucket := testClient.Bucket(tb.Name)
		_, err := getObjectsToDownloadFromBucket(bucket, ctx, config)
		is.NoError(err, "Should not error when getting objects from valid buckets")
	}

	missingBucketName := "does-not-exist"
	missingBucket := testClient.Bucket(missingBucketName)
	_, missingBucketErr := getObjectsToDownloadFromBucket(missingBucket, ctx, config)
	is.Error(missingBucketErr, "Should error when trying to get objects from bucket that doesn't exist")

	missingValidationTypeBucketName := "test-matt-empty"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: missingValidationTypeBucketName, Type: "empty"})
	missingValidationTypeBucket := testClient.Bucket(missingValidationTypeBucketName)
	_, missingValidationTypeErr := getObjectsToDownloadFromBucket(missingValidationTypeBucket, ctx, config)
	is.Error(missingValidationTypeErr, "Should error when validation type doesn't have matching get objects logic")

	tooFewFilesNucketName := "test-matt-empty"
	tooFewFilesBucket := testClient.Bucket(tooFewFilesNucketName)
	config.Buckets = []BucketToProcess{{Name: tooFewFilesNucketName, Type: "photo"}}
	_, tooFewFilesErr := getObjectsToDownloadFromBucket(tooFewFilesBucket, ctx, config)
	is.Error(tooFewFilesErr, "Should error when bucket doesn't have enough files to get")

	config.Buckets = []BucketToProcess{{Name: tooFewFilesNucketName, Type: "server-backup"}}
	_, tooFewFilesErr = getObjectsToDownloadFromBucket(tooFewFilesBucket, ctx, config)
	is.Error(tooFewFilesErr, "Should error when bucket doesn't have enough files to get")

	//TODO: add test case for unable to get enough files for media bucket (up the episodes per show)
}

func TestValidateServerBackups(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	rules := ServerFileValidationRules{
		OldestFileMaxAgeInDays: 10,
		NewestFileMaxAgeInDays: 5,
	}
	happyPathBucket := testClient.Bucket("test-matt-server-backups-fresh")
	err := uploadFreshServerBackupFile(happyPathBucket, ctx)
	if err != nil {
		t.Error("Could not prep test case for validating server backups.")
	}
	happyPathErr := validateServerBackups(happyPathBucket, ctx, rules)
	is.NoError(happyPathErr, "Should not error when bucket has a freshly uploaded file")

	badBucket := testClient.Bucket("does-not-exist")
	badBucketErr := validateServerBackups(badBucket, ctx, rules)
	is.Error(badBucketErr, "Should error when validating a non existent bucket")

	//TODO: figure out why empty bucket is not failing validation as expected
	/*
		emptyBucket := testClient.Bucket("test-matt-empty")
		emptyErr := validateServerBackups(emptyBucket, ctx, rules)
		is.Error(emptyErr, "Should error when validating a bucket with no objects")
	*/
	veryOldFileBucket := testClient.Bucket("test-matt-server-backups-old")
	veryOldFileErr := validateServerBackups(veryOldFileBucket, ctx, rules)
	is.Error(veryOldFileErr, "Should error when bucket has oldest file past archive cutoff")

	rules.NewestFileMaxAgeInDays = 0
	newFileTooOldErr := validateServerBackups(happyPathBucket, ctx, rules)
	is.Error(newFileTooOldErr, "Should error when bucket has newest file past cutoff")

	//TODO: somehow make checking oldest file pass but fail on figuring out the newest file... how is this branch testable?
}

func TestGetMediaFilesToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-media")
	actual, err := getMediaFilesToDownload(happyPathBucket, ctx, rules)
	is.Equal(9, len(actual))
	is.NoError(err, "Should not error when getting files to download from valid media bucket")

	rules.EpisodesFromEachShow = 4
	_, notEnoughShowsErr := getMediaFilesToDownload(happyPathBucket, ctx, rules)
	is.Error(notEnoughShowsErr, "Should error when there are not enough episodes to get of each show")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getMediaFilesToDownload(badBucket, ctx, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")

}

func TestGetPhotosToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-photos")
	err := uploadThisMonthPhotos(happyPathBucket, ctx)
	if err != nil {
		t.Error("Could not prep test case for getting photos to download.")
	}
	years := time.Now().Year() - 2009 //
	expected := years*rules.PhotosFromEachYear + rules.PhotosFromThisMonth
	actual, err := getPhotosToDownload(happyPathBucket, ctx, rules)
	is.Equal(expected, len(actual))
	is.NoError(err, "Should not error when getting files to download from valid photos bucket")

	rules.PhotosFromThisMonth = 11
	_, notEnoughMonthPhotosErr := getPhotosToDownload(happyPathBucket, ctx, rules)
	is.Error(notEnoughMonthPhotosErr, "Should error when there are not enough photos to get of this month")

	rules.PhotosFromEachYear = 11
	_, notEnoughYearPhotosErr := getPhotosToDownload(happyPathBucket, ctx, rules)
	is.Error(notEnoughYearPhotosErr, "Should error when there are not enough photos to get of each year")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getPhotosToDownload(badBucket, ctx, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")
}

func TestGetServerBackupsToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-server-backups")
	expected := []string{"newest.txt", "new2.txt", "new3.txt", "new4.txt"}
	actual, err := getServerBackupsToDownload(happyPathBucket, ctx, rules)
	is.Equal(expected, actual)
	is.NoError(err, "Should not error when getting files to download from valid server backup bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getServerBackupsToDownload(badBucket, ctx, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")

	emptyBucket := testClient.Bucket("test-matt-empty")
	_, emptyBucketErr := getServerBackupsToDownload(emptyBucket, ctx, rules)
	is.Error(emptyBucketErr, "Should error when getting files to download from an empty bucket")
}

var testBucketTopLevelDirsCases = []struct {
	bucketName string
	expected   []string
}{
	{"test-matt-media", []string{"show 1/", "show 2/", "show 3/"}},
}

func TestGetBucketTopLevelDirs(t *testing.T) {

	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)

	for _, tc := range testBucketTopLevelDirsCases {
		expected := tc.expected
		bucket := testClient.Bucket(tc.bucketName)
		actual, err := getBucketTopLevelDirs(bucket, ctx)
		is.NoError(err, "Should not error when reading from a populated test bucket")
		is.Equal(expected, actual)
	}

	emptyBucket := testClient.Bucket("test-matt-empty")
	actual, err := getBucketTopLevelDirs(emptyBucket, ctx)
	is.Empty(actual, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getBucketTopLevelDirs(badBucket, ctx)
	is.Error(err, "Should error when reading from a non existent bucket")

}

var testGetBucketValidationTypeFromNameAndConfigCases = []struct {
	name     string
	expected string
}{
	{"bucket-one", "media"},
	{"bucket-two", "photo"},
	{"bucket-three", "server-backup"},
}

func TestGetBucketValidationTypeFromNameAndConfig(t *testing.T) {
	is := assert.New(t)
	configs := []BucketToProcess{
		{Name: "bucket-one", Type: "media"},
		{Name: "bucket-two", Type: "photo"},
		{Name: "bucket-three", Type: "server-backup"},
	}
	for _, tc := range testGetBucketValidationTypeFromNameAndConfigCases {
		expected := tc.expected
		actual, err := getBucketValidationTypeFromNameAndConfig(tc.name, configs)
		is.NoError(err)
		is.Equal(expected, actual)
	}
	_, err := getBucketValidationTypeFromNameAndConfig("name-does-not-exist", configs)
	is.Error(err, "Should error when unable to find matching config")

}

func TestGetNewestObjectFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	bucket := testClient.Bucket("test-matt-server-backups")
	actual, err := getNewestObjectFromBucket(bucket, ctx)
	is.NoError(err, "Should not error when getting latest object from bucket")
	is.Equal("newest.txt", actual.Name)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getNewestObjectFromBucket(emptyBucket, ctx)
	is.Nil(actualEmpty, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getNewestObjectFromBucket(badBucket, ctx)
	is.Error(err, "Should error when reading from a non existent bucket")
}

func TestGetOldestObjectFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)
	bucket := testClient.Bucket("test-matt-server-backups")
	actual, err := getOldestObjectFromBucket(bucket, ctx)
	is.NoError(err, "Should not error when getting latest object from bucket")
	is.Equal("oldest.txt", actual.Name)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getOldestObjectFromBucket(emptyBucket, ctx)
	is.Nil(actualEmpty, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getOldestObjectFromBucket(badBucket, ctx)
	is.Error(err, "Should error when reading from a non existent bucket")
}

func TestGetRandomFilesFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(t, ctx)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getRandomFilesFromBucket(emptyBucket, ctx, 0, "")
	is.Nil(actualEmpty, "Should not find any files in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getRandomFilesFromBucket(badBucket, ctx, 1, "")
	is.Error(err, "Should error when reading from a non existent bucket")

	goodBucketFewFiles := testClient.Bucket("test-matt-server-backups-old")
	_, err = getRandomFilesFromBucket(goodBucketFewFiles, ctx, -1, "")
	is.Error(err, "Should error when requesting a negative number of files")
	_, err = getRandomFilesFromBucket(goodBucketFewFiles, ctx, 10, "")
	is.Error(err, "Should error when requesting more files than are available")

	goodBucketManyFiles := testClient.Bucket("test-matt-media")
	manyFiles, err := getRandomFilesFromBucket(goodBucketManyFiles, ctx, 5, "")
	is.NoError(err, "Should not error when requesting fewer files than are available")
	is.Equal(5, len(manyFiles), "Should get 5 file names back when requesting 5 files")
}

func TestGetRandomSampleFromPopulation(t *testing.T) {
	is := assert.New(t)
	actual := getRandomSampleFromPopulation(1, 100)
	is.Equal(1, len(actual), "Should return 1 value when requesting sample size of 1")

	actual = getRandomSampleFromPopulation(100, 10000)
	is.Equal(100, len(actual), "Should return 100 values when requesting sample size of 100")

	actual = getRandomSampleFromPopulation(100, 10)
	is.Nil(actual, "Should return nil when requesting large sample size than population")

	actual = getRandomSampleFromPopulation(-1, 10)
	is.Nil(actual, "Should return nil when requesting negative sample size")

}
