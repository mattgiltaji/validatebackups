package validatebackups

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

var testFileConfigCases = []struct {
	filename string
	expected Config
}{
	{"fullConfig.json", Config{
		GoogleAuthFileLocation: "over-there",
		FileDownloadLocation:   "where-should-the-files-go",
		Buckets: []BucketToProcess{
			{Name: "bucket-one"},
			{Name: "bucket-two"},
			{Name: "bucket-three"},
		}},
	}, {"partialConfig.json", Config{
		GoogleAuthFileLocation: "over-here",
		Buckets: []BucketToProcess{
			{Name: "bucket-a"},
		}},
	}, {"emptyConfig.json", Config{}},
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
		is.Equal(expected.Buckets, actual.Buckets)
	}

	_, err = loadConfigurationFromFile(filepath.Join(testDataDir, "doesNotExist.json"))
	is.Error(err, "Should error out when reading config from a file that doesn't exist.")
}
