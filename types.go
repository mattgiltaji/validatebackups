package validatebackups

import (
	"cloud.google.com/go/storage"
)

type Validator struct {
	Client storage.Client
}

type Config struct {
	GoogleAuthFileLocation string `json:"google_auth_file_location"`
	FileDownloadLocation string `json:"file_download_location"`
	Buckets []BucketToProcess `json:"buckets"`
}

type BucketToProcess  struct {
	Name string `json:"name"`
}