package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/option"
)

//
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

var testBucketTopLevelDirsCases = []struct {
	bucketName string
	expected   []string
}{
	{"test-matt-media", []string{"show 1/", "show 2/", "show 3/"}},
}

func TestGetBucketTopLevelDirs(t *testing.T) {

	is := assert.New(t)
	ctx := context.Background()
	// set up a test project with a readonly service account that can be committed
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
