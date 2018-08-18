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
	googleAuthFileLocation := "D:\\Matt\\Documents\\google cloud storage\\test-backup-validator-auth.json"
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
		is.NoError(err, "Should not error out when reading from a populated test bucket")
		is.Equal(expected, actual)
	}

	emptyBucket := testClient.Bucket("test-matt-empty")
	actual, err := getBucketTopLevelDirs(emptyBucket, ctx)
	is.Empty(actual, "SHould not find any dirs in an empy bucket")
	is.NoError(err, "Should not error out when reading from an empty bucket")

	badBucket := testClient.Bucket("does-not-exist")
	_, err = getBucketTopLevelDirs(badBucket, ctx)
	is.Error(err, "Should error out when reading from a non existant bucket")

}
