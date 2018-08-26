package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	//TODO: don't upload fresh file if we have one from today

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

	//TODO: add test case where getting objects fails for photo, media, and backup buckets

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
	actuals, err := getMediaFilesToDownload(happyPathBucket, ctx, rules)
	is.Equal(9, len(actuals))
	is.NoError(err, "Should not error when getting files to download from valid media bucket")

	rules.EpisodesFromEachShow = 4
	_, notEnoughShowsErr := getMediaFilesToDownload(happyPathBucket, ctx, rules)
	is.Error(notEnoughShowsErr, "Should error when there are not enough episodes to get of each show")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getMediaFilesToDownload(badBucket, ctx, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")

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

	/*
		emptyBucket := testClient.Bucket("test-matt-empty")
		emptyErr := validateServerBackups(emptyBucket, ctx, rules)
		is.Error(emptyErr, "Should error when validating a bucket with no objects")

		veryOldFileBucket := testClient.Bucket("test-matt-server-backups-old")
		veryOldFileErr := validateServerBackups(veryOldFileBucket, ctx, rules)
		is.Error(veryOldFileErr, "Should error when bucket has oldest file past archive cutoff")
	*/

	//TODO: figure out why empty bucket is not failing validation as expected
	//TODO: figure out why very old bucket is not failing validation as expected
	//TODO: not new enough test case: upload fresh file in prep, change rules.NewestFileMaxAgeInDays to 0 to make sure it fails
	//TODO: somehow make checking oldest file pass but fail on figuring out the newest file... how is this branch testable?
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
	is.Equal(5, len(manyFiles), "Should get 5 filenames back when requesting 5 files")
}

func TestGetRandomSampleFromPopulation(t *testing.T) {
	is := assert.New(t)
	actual := getRandomSampleFromPopulation(1, 100)
	is.Equal(1, len(actual), "Should return 1 value when requesting sample size of 1")

	actual = getRandomSampleFromPopulation(100, 10000)
	is.Equal(100, len(actual), "Should return 100 valuse when requesting sample size of 100")

	actual = getRandomSampleFromPopulation(100, 10)
	is.Nil(actual, "Should return nil when requesting large sample size than population")

	actual = getRandomSampleFromPopulation(-1, 10)
	is.Nil(actual, "Should return nil when requesting negative sample size")

}
