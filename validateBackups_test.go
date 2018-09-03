package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/juju/errors"
	"github.com/stretchr/testify/assert"
	"github.com/udhos/equalfile"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// ***** Helpers *****
func getTestClient(ctx context.Context, t *testing.T) (client *storage.Client) {
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

func deleteExistingObjectsFromBucket(ctx context.Context, bucket *storage.BucketHandle) (err error) {
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

func uploadFreshServerBackupFile(ctx context.Context, bucket *storage.BucketHandle) (err error) {
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

	err = deleteExistingObjectsFromBucket(ctx, bucket)
	if err != nil {
		return errors.Annotate(err, "Unable to delete existing files when preparing backup bucket")
	}
	workingDir, err := os.Getwd()
	if err != nil {
		return errors.Annotate(err, "Could not determine current directory to prepare backup bucket")
	}
	filePath := filepath.Join(workingDir, "testdata", "newest.txt")
	err = uploadFileToBucket(ctx, bucket, filePath, "newest.txt")
	if err != nil {
		return errors.Annotate(err, "Unable to upload file when preparing backup bucket")
	}
	return
}

func uploadThisMonthPhotos(ctx context.Context, bucket *storage.BucketHandle) (err error) {
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
		err = uploadFileToBucket(ctx, bucket, filePath, uploadPath)
		if err != nil {
			return errors.Annotate(err, "Unable to upload file when preparing photos bucket")
		}
	}
	//wait until they are uploaded successfully (if we had to upload them
	time.Sleep(10 * time.Second)
	return
}

func uploadFileToBucket(ctx context.Context, bucket *storage.BucketHandle, filePath string, uploadPath string) (err error) {
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
		MaxDownloadRetries:     42,
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
		MaxDownloadRetries:     18,
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
	testClient := getTestClient(ctx, t)

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
	err := uploadFreshServerBackupFile(ctx, backupBucket)
	if err != nil {
		t.Error("Could not prep test case for validating server backup bucket.")
	}
	photosBucket := testClient.Bucket("test-matt-photos")
	err = uploadThisMonthPhotos(ctx, photosBucket)
	if err != nil {
		t.Error("Could not prep test case for validating photos bucket.")
	}

	actual, err := validateBucketsInConfig(ctx, testClient, config)
	is.NoError(err, "Should not error when validating good bucket types")
	is.True(actual, "Should return true when validations are successful")

	missingBucketName := "does-not-exist"
	config.Buckets = []BucketToProcess{{Name: missingBucketName, Type: "media"}}
	actual, missingBucketErr := validateBucketsInConfig(ctx, testClient, config)
	is.Error(missingBucketErr, "Should error when config has a bucket that doesn't exist")
	is.False(actual, "Should return false if there is an error during validation")

}

func TestGetObjectsToDownloadFromBucketsInConfig(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)

	config := Config{
		FilesToDownload: FileDownloadRules{
			ServerBackups:        4,
			EpisodesFromEachShow: 3,
			PhotosFromThisMonth:  5,
			PhotosFromEachYear:   10,
		},
		Buckets: []BucketToProcess{
			{Name: "test-matt-media", Type: "media"},
			{Name: "test-matt-server-backups", Type: "server-backup"},
		}}

	expected := []BucketAndFiles{
		{"test-matt-media", []string{
			"show 1/season 1/01x01 episode.ogv",
			"show 1/season 1/S01E22 episode.ogv",
			"show 1/season 2/s02e02 - episode.ogv",
			"show 2/season 3/03x03 - episode.ogv",
			"show 2/season 5/05x01 episode.ogv",
			"show 2/season 7/S07E77 episode.ogv",
			"show 3/season 1000/s1000e947 - episode.ogv",
			"show 3/specials/00x01 making of episode.ogv",
			"show 3/specials/s00e03 - holiday special.ogv",
		}},
		{"test-matt-server-backups", []string{
			"newest.txt", "new2.txt", "new3.txt", "new4.txt",
		}},
	}
	actual, err := getObjectsToDownloadFromBucketsInConfig(ctx, testClient, config)
	is.NoError(err, "Should not error when getting objects from valid buckets")
	is.Equal(expected, actual)

	missingBucketName := "does-not-exist"
	config.Buckets = []BucketToProcess{{Name: missingBucketName, Type: "photo"}}
	_, missingBucketErr := getObjectsToDownloadFromBucketsInConfig(ctx, testClient, config)
	is.Error(missingBucketErr, "Should error when trying to get objects from bucket that doesn't exist")

	missingValidationTypeBucketName := "test-matt-empty"
	config.Buckets = []BucketToProcess{{Name: missingValidationTypeBucketName, Type: "empty"}}
	_, missingValidationTypeErr := getObjectsToDownloadFromBucketsInConfig(ctx, testClient, config)
	is.Error(missingValidationTypeErr, "Should error when validation type doesn't have matching get objects logic")
}

func TestSaveInProgressFile(t *testing.T) {
	is := assert.New(t)
	cmp := equalfile.New(nil, equalfile.Options{}) // compare using single mode
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}
	expectedFileName := filepath.Join(workingDir, "testdata", "inProgressData.json")

	tempDir, err := ioutil.TempDir("", "TestSaveInProgressFile")
	if err != nil {
		t.Error("Could not create temporary directory")
	}
	tempF, err := ioutil.TempFile(tempDir, "")
	if err != nil {
		t.Error("Could not create temporary file")
	}
	tempFileName := tempF.Name()
	defer os.RemoveAll(tempDir)

	data := []BucketAndFiles{
		{"test-matt-media", []string{
			"show 1/season 1/01x01 episode.ogv",
			"show 1/season 1/S01E22 episode.ogv",
			"show 1/season 2/s02e02 - episode.ogv",
			"show 2/season 3/03x03 - episode.ogv",
			"show 2/season 5/05x01 episode.ogv",
			"show 2/season 7/S07E77 episode.ogv",
			"show 3/season 1000/s1000e947 - episode.ogv",
			"show 3/specials/00x01 making of episode.ogv",
			"show 3/specials/s00e03 - holiday special.ogv",
		}},
		{"test-matt-server-backups", []string{
			"newest.txt", "new2.txt", "new3.txt", "new4.txt",
		}},
	}

	err = saveInProgressFile("", data)
	is.Error(err, "Should error when saving to a blank path")

	err = saveInProgressFile(tempFileName, data)
	equal, err := cmp.CompareFile(expectedFileName, tempFileName)
	is.NoError(err, "Should not error when saving good data to good file path.")
	is.True(equal, "Saved file contents should match expected.")
}

func TestLoadInProgressFile(t *testing.T) {
	is := assert.New(t)
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}
	testFilePath := filepath.Join(workingDir, "testdata", "inProgressData.json")

	expected := []BucketAndFiles{
		{"test-matt-media", []string{
			"show 1/season 1/01x01 episode.ogv",
			"show 1/season 1/S01E22 episode.ogv",
			"show 1/season 2/s02e02 - episode.ogv",
			"show 2/season 3/03x03 - episode.ogv",
			"show 2/season 5/05x01 episode.ogv",
			"show 2/season 7/S07E77 episode.ogv",
			"show 3/season 1000/s1000e947 - episode.ogv",
			"show 3/specials/00x01 making of episode.ogv",
			"show 3/specials/s00e03 - holiday special.ogv",
		}},
		{"test-matt-server-backups", []string{
			"newest.txt", "new2.txt", "new3.txt", "new4.txt",
		}},
	}

	_, err = loadInProgressFile("")
	is.Error(err, "Should error when loading a file that doesn't exist")

	actual, err := loadInProgressFile(testFilePath)
	is.NoError(err, "Should not error when loading good data from good file path.")
	is.Equal(expected, actual, "Loaded file contents should match expected.")
}

func TestDownloadFilesFromBucketAndFiles(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	tempDir, err := ioutil.TempDir("", "TestDownloadFilesFromBucketAndFiles")
	if err != nil {
		t.Error("Could not create temporary directory")
	}
	defer os.RemoveAll(tempDir)

	config := Config{
		FileDownloadLocation: tempDir,
		MaxDownloadRetries:   2,
	}
	mapping := []BucketAndFiles{
		{"test-matt-photos",
			[]string{"2015-02/IMG_02.gif", "2016-10/IMG_10.gif"}},
	}

	goodBucketErr := downloadFilesFromBucketAndFiles(ctx, testClient, config, mapping)
	is.NoError(goodBucketErr, "Should not error when downloading good files from good bucket")

	config.FileDownloadLocation = "E:/does/not/exist/,"
	badLocationErr := downloadFilesFromBucketAndFiles(ctx, testClient, config, mapping)
	is.Error(badLocationErr, "Should error when downloading files to invalid location")
}

func TestValidateBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)

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
	err := uploadFreshServerBackupFile(ctx, backupBucket)
	if err != nil {
		t.Error("Could not prep test case for validating server backup bucket.")
	}

	for _, tb := range config.Buckets {
		bucket := testClient.Bucket(tb.Name)
		err := validateBucket(ctx, bucket, config)
		is.NoError(err, "Should not error when validating a bucket type that passes validations")
	}

	missingBucketName := "does-not-exist"
	missingBucket := testClient.Bucket(missingBucketName)
	missingBucketErr := validateBucket(ctx, missingBucket, config)
	is.Error(missingBucketErr, "Should error when validating a bucket that doesn't exist")

	missingValidationTypeBucketName := "test-matt-empty"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: missingValidationTypeBucketName, Type: "empty"})
	missingValidationTypeBucket := testClient.Bucket(missingValidationTypeBucketName)
	missingValidationTypeErr := validateBucket(ctx, missingValidationTypeBucket, config)
	is.Error(missingValidationTypeErr, "Should error when validation type doesn't have matching validation logic")

	failBucketName := "test-matt-server-backups"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: failBucketName, Type: "server-backup"})
	failBucket := testClient.Bucket(failBucketName)
	failBucketErr := validateBucket(ctx, failBucket, config)
	is.Error(failBucketErr, "Should error when validations fail")
}

func TestGetObjectsToDownloadFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)

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
		_, err := getObjectsToDownloadFromBucket(ctx, bucket, config)
		is.NoError(err, "Should not error when getting objects from valid buckets")
	}

	missingBucketName := "does-not-exist"
	missingBucket := testClient.Bucket(missingBucketName)
	_, missingBucketErr := getObjectsToDownloadFromBucket(ctx, missingBucket, config)
	is.Error(missingBucketErr, "Should error when trying to get objects from bucket that doesn't exist")

	missingValidationTypeBucketName := "test-matt-empty"
	config.Buckets = append(config.Buckets, BucketToProcess{Name: missingValidationTypeBucketName, Type: "empty"})
	missingValidationTypeBucket := testClient.Bucket(missingValidationTypeBucketName)
	_, missingValidationTypeErr := getObjectsToDownloadFromBucket(ctx, missingValidationTypeBucket, config)
	is.Error(missingValidationTypeErr, "Should error when validation type doesn't have matching get objects logic")

	tooFewFilesBucketName := "test-matt-empty"
	tooFewFilesBucket := testClient.Bucket(tooFewFilesBucketName)
	config.Buckets = []BucketToProcess{{Name: tooFewFilesBucketName, Type: "photo"}}
	_, tooFewFilesErr := getObjectsToDownloadFromBucket(ctx, tooFewFilesBucket, config)
	is.Error(tooFewFilesErr, "Should error when bucket doesn't have enough files to get")

	config.Buckets = []BucketToProcess{{Name: tooFewFilesBucketName, Type: "server-backup"}}
	_, tooFewFilesErr = getObjectsToDownloadFromBucket(ctx, tooFewFilesBucket, config)
	is.Error(tooFewFilesErr, "Should error when bucket doesn't have enough files to get")

	config.FilesToDownload.EpisodesFromEachShow = 7
	mediaBucketName := "test-matt-media"
	mediaBucket := testClient.Bucket(mediaBucketName)
	config.Buckets = []BucketToProcess{{Name: mediaBucketName, Type: "media"}}
	_, mediaBucketErr := getObjectsToDownloadFromBucket(ctx, mediaBucket, config)
	is.Error(mediaBucketErr, "Should error when bucket doesn't have enough files to get")
}

func TestDownloadFilesFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	tempDir, err := ioutil.TempDir("", "TestDownloadFilesFromBucket")
	if err != nil {
		t.Error("Could not create temporary directory")
	}
	defer os.RemoveAll(tempDir)

	config := Config{
		FileDownloadLocation: tempDir,
		MaxDownloadRetries:   2,
	}
	files := []string{
		"2015-02/IMG_02.gif", "2016-10/IMG_10.gif",
	}

	missingBucket := testClient.Bucket("does-not-exist")
	missingBucketErr := downloadFilesFromBucket(ctx, missingBucket, files, config)
	is.Error(missingBucketErr, "Should error when trying to get objects from bucket that doesn't exist")

	emptyBucket := testClient.Bucket("test-matt-empty")
	emptyBucketErr := downloadFilesFromBucket(ctx, emptyBucket, files, config)
	is.Error(emptyBucketErr, "Should error when unable to find files in bucket")

	goodBucket := testClient.Bucket("test-matt-photos")
	goodBucketErr := downloadFilesFromBucket(ctx, goodBucket, files, config)
	is.NoError(goodBucketErr, "Should not error when downloading good files from good bucket")

	existingFilesErr := downloadFilesFromBucket(ctx, goodBucket, files, config)
	is.NoError(existingFilesErr, "Should not error when retrying to download good files from good bucket")

	config.FileDownloadLocation = "E:/does/not/exist/,"
	badLocationErr := downloadFilesFromBucket(ctx, goodBucket, files, config)
	is.Error(badLocationErr, "Should error when downloading files to invalid location")
}

func TestValidateServerBackups(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	rules := ServerFileValidationRules{
		OldestFileMaxAgeInDays: 10,
		NewestFileMaxAgeInDays: 5,
	}
	happyPathBucket := testClient.Bucket("test-matt-server-backups-fresh")
	err := uploadFreshServerBackupFile(ctx, happyPathBucket)
	if err != nil {
		t.Error("Could not prep test case for validating server backups.")
	}
	happyPathErr := validateServerBackups(ctx, happyPathBucket, rules)
	is.NoError(happyPathErr, "Should not error when bucket has a freshly uploaded file")

	badBucket := testClient.Bucket("does-not-exist")
	badBucketErr := validateServerBackups(ctx, badBucket, rules)
	is.Error(badBucketErr, "Should error when validating a non existent bucket")

	//TODO: figure out why empty bucket is not failing validation as expected
	/*
		emptyBucket := testClient.Bucket("test-matt-empty")
		emptyErr := validateServerBackups(emptyBucket, rules)
		is.Error(emptyErr, "Should error when validating a bucket with no objects")
	*/
	veryOldFileBucket := testClient.Bucket("test-matt-server-backups-old")
	veryOldFileErr := validateServerBackups(ctx, veryOldFileBucket, rules)
	is.Error(veryOldFileErr, "Should error when bucket has oldest file past archive cutoff")

	rules.NewestFileMaxAgeInDays = 0
	newFileTooOldErr := validateServerBackups(ctx, happyPathBucket, rules)
	is.Error(newFileTooOldErr, "Should error when bucket has newest file past cutoff")

	//TODO: somehow make checking oldest file pass but fail on figuring out the newest file... how is this branch testable?
}

func TestGetMediaFilesToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-media")
	actual, err := getMediaFilesToDownload(ctx, happyPathBucket, rules)
	is.Equal(9, len(actual))
	is.NoError(err, "Should not error when getting files to download from valid media bucket")

	rules.EpisodesFromEachShow = 4
	_, notEnoughShowsErr := getMediaFilesToDownload(ctx, happyPathBucket, rules)
	is.Error(notEnoughShowsErr, "Should error when there are not enough episodes to get of each show")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getMediaFilesToDownload(ctx, badBucket, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")

}

func TestGetPhotosToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-photos")
	err := uploadThisMonthPhotos(ctx, happyPathBucket)
	if err != nil {
		t.Error("Could not prep test case for getting photos to download.")
	}
	years := time.Now().Year() - 2009 //
	expected := years*rules.PhotosFromEachYear + rules.PhotosFromThisMonth
	actual, err := getPhotosToDownload(ctx, happyPathBucket, rules)
	is.Equal(expected, len(actual))
	is.NoError(err, "Should not error when getting files to download from valid photos bucket")

	rules.PhotosFromThisMonth = 11
	_, notEnoughMonthPhotosErr := getPhotosToDownload(ctx, happyPathBucket, rules)
	is.Error(notEnoughMonthPhotosErr, "Should error when there are not enough photos to get of this month")

	rules.PhotosFromEachYear = 11
	_, notEnoughYearPhotosErr := getPhotosToDownload(ctx, happyPathBucket, rules)
	is.Error(notEnoughYearPhotosErr, "Should error when there are not enough photos to get of each year")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getPhotosToDownload(ctx, badBucket, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")
}

func TestGetServerBackupsToDownload(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	rules := FileDownloadRules{
		ServerBackups:        4,
		EpisodesFromEachShow: 3,
		PhotosFromThisMonth:  5,
		PhotosFromEachYear:   10,
	}

	happyPathBucket := testClient.Bucket("test-matt-server-backups")
	expected := []string{"newest.txt", "new2.txt", "new3.txt", "new4.txt"}
	actual, err := getServerBackupsToDownload(ctx, happyPathBucket, rules)
	is.Equal(expected, actual)
	is.NoError(err, "Should not error when getting files to download from valid server backup bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, badBucketErr := getServerBackupsToDownload(ctx, badBucket, rules)
	is.Error(badBucketErr, "Should error when getting files to download from a non existent bucket")

	emptyBucket := testClient.Bucket("test-matt-empty")
	_, emptyBucketErr := getServerBackupsToDownload(ctx, emptyBucket, rules)
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
	testClient := getTestClient(ctx, t)

	for _, tc := range testBucketTopLevelDirsCases {
		expected := tc.expected
		bucket := testClient.Bucket(tc.bucketName)
		actual, err := getBucketTopLevelDirs(ctx, bucket)
		is.NoError(err, "Should not error when reading from a populated test bucket")
		is.Equal(expected, actual)
	}

	emptyBucket := testClient.Bucket("test-matt-empty")
	actual, err := getBucketTopLevelDirs(ctx, emptyBucket)
	is.Empty(actual, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getBucketTopLevelDirs(ctx, badBucket)
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
	testClient := getTestClient(ctx, t)
	bucket := testClient.Bucket("test-matt-server-backups")
	actual, err := getNewestObjectFromBucket(ctx, bucket)
	is.NoError(err, "Should not error when getting latest object from bucket")
	is.Equal("newest.txt", actual.Name)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getNewestObjectFromBucket(ctx, emptyBucket)
	is.Nil(actualEmpty, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getNewestObjectFromBucket(ctx, badBucket)
	is.Error(err, "Should error when reading from a non existent bucket")
}

func TestGetOldestObjectFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	bucket := testClient.Bucket("test-matt-server-backups")
	actual, err := getOldestObjectFromBucket(ctx, bucket)
	is.NoError(err, "Should not error when getting latest object from bucket")
	is.Equal("oldest.txt", actual.Name)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getOldestObjectFromBucket(ctx, emptyBucket)
	is.Nil(actualEmpty, "Should not find any dirs in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getOldestObjectFromBucket(ctx, badBucket)
	is.Error(err, "Should error when reading from a non existent bucket")
}

func TestGetRandomFilesFromBucket(t *testing.T) {
	is := assert.New(t)
	ctx := context.Background()
	testClient := getTestClient(ctx, t)

	emptyBucket := testClient.Bucket("test-matt-empty")
	actualEmpty, err := getRandomFilesFromBucket(ctx, emptyBucket, 0, "")
	is.Nil(actualEmpty, "Should not find any files in an empty bucket")
	is.NoError(err, "Should not error when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getRandomFilesFromBucket(ctx, badBucket, 1, "")
	is.Error(err, "Should error when reading from a non existent bucket")

	goodBucketFewFiles := testClient.Bucket("test-matt-server-backups-old")
	_, err = getRandomFilesFromBucket(ctx, goodBucketFewFiles, -1, "")
	is.Error(err, "Should error when requesting a negative number of files")
	_, err = getRandomFilesFromBucket(ctx, goodBucketFewFiles, 10, "")
	is.Error(err, "Should error when requesting more files than are available")

	goodBucketManyFiles := testClient.Bucket("test-matt-media")
	manyFiles, err := getRandomFilesFromBucket(ctx, goodBucketManyFiles, 5, "")
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

func TestDownloadFile(t *testing.T) {
	is := assert.New(t)
	cmp := equalfile.New(nil, equalfile.Options{}) // compare using single mode
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}
	ctx := context.Background()
	testClient := getTestClient(ctx, t)
	tempDir, err := ioutil.TempDir("", "TestDownloadFile")
	if err != nil {
		t.Error("Could not create temporary directory")
	}
	tempF, err := ioutil.TempFile(tempDir, "")
	if err != nil {
		t.Error("Could not create temporary file")
	}
	tempFileName := tempF.Name()
	defer os.RemoveAll(tempDir)

	expectedFileName := filepath.Join(workingDir, "testdata", "Red_1x1.gif")
	goodBucket := testClient.Bucket("test-matt-photos")
	emptyBucket := testClient.Bucket("test-matt-empty")

	err = downloadFile(ctx, emptyBucket, "2014-11/IMG_09.gif", tempFileName)
	is.Error(err, "Should error when downloading a file that doesn't exist.")

	err = downloadFile(ctx, goodBucket, "2014-11/IMG_09.gif", "E:/lol/")
	is.Error(err, "Should error when downloading to a bad path.")

	err = downloadFile(ctx, goodBucket, "2014-11/IMG_09.gif", tempFileName)
	equal, err := cmp.CompareFile(expectedFileName, tempFileName)
	is.NoError(err, "Should not error when downloading a good file.")
	is.True(equal, "Saved file contents should match expected.")

	existingFileErr := downloadFile(ctx, goodBucket, "2014-11/IMG_09.gif", tempFileName)
	equal, err = cmp.CompareFile(expectedFileName, tempFileName)
	is.Error(existingFileErr, "Should error when file already exists and matches contents.")
	is.True(errors.IsAlreadyExists(existingFileErr), "Should send already exists error when file already exists and matches contents.")
	is.True(equal, "Saved file contents should match expected.")
}

func TestVerifyDownloadedFile(t *testing.T) {
	is := assert.New(t)
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}
	ctx := context.Background()
	testClient := getTestClient(ctx, t)

	sameContentsTestFile := filepath.Join(workingDir, "testdata", "Red_1x1.gif")
	diffSizeTestFile := filepath.Join(workingDir, "testdata", "newest.txt")
	sameSizeDiffContentsTestFile := filepath.Join(workingDir, "testdata", "Gray_1x1.gif")

	testObj, err := testClient.Bucket("test-matt-photos").Object("2012-12/IMG_02.gif").Attrs(ctx)
	if err != nil {
		t.Error("Could not load remote test file")
	}

	err = verifyDownloadedFile(nil, diffSizeTestFile)
	is.Error(err, "Should error but not panic when passed a bad objAttrs")
	is.True(errors.IsNotValid(err), "Should return NotValid error when passed a bad objAttrs")

	err = verifyDownloadedFile(testObj, "/does/not/exist")
	is.Error(err, "Should error but not panic when passed a bad file path")
	is.True(errors.IsNotFound(err), "Should return NotFound error when passed a bad file path")

	err = verifyDownloadedFile(testObj, sameContentsTestFile)
	is.NoError(err, "Should verify that same contents mean same file")

	err = verifyDownloadedFile(testObj, diffSizeTestFile)
	is.Error(err, "Should verify that different sizes mean different file")
	is.True(errors.IsNotValid(err), "Should return NotValid error when file has a different size")

	err = verifyDownloadedFile(testObj, sameSizeDiffContentsTestFile)
	is.Error(err, "Should verify that different contents mean different file")
	is.True(errors.IsNotValid(err), "Should return NotValid error when file has different contents")
}

func TestGetCrc32CFromFile(t *testing.T) {
	is := assert.New(t)
	workingDir, err := os.Getwd()
	if err != nil {
		t.Error("Could not determine current directory")
	}

	testFile := filepath.Join(workingDir, "testdata", "Red_1x1.gif")
	missingFile := filepath.Join(workingDir, "testdata", "does_not_exist.jpeg")
	expected := uint32(0x26512888)
	actual, err := getCrc32CFromFile(testFile)
	is.NoError(err, "Should not error when calculating CRC for a file")
	is.Equal(expected, actual, "Calculated CRC should match expected")

	_, err = getCrc32CFromFile(missingFile)
	is.Error(err, "Should error when calculating CRC for a file that doesn't exist")
}
