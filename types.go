package main

//Config represents the configuration options available.
// It is expected to be parsed from a json file passed in at runtime.
type Config struct {
	GoogleAuthFileLocation string                    `json:"google_auth_file_location"`
	FileDownloadLocation   string                    `json:"file_download_location"`
	ServerBackupRules      ServerFileValidationRules `json:"server_backup_rules"`
	FilesToDownload        FileDownloadRules         `json:"files_to_download"`
	Buckets                []BucketToProcess         `json:"buckets"`
}

//BucketToProcess is a mapping of bucket names toa type indicating how they should be validated.
type BucketToProcess struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

//ServerFileValidationRules contains parameters to adjust validations on server-backup type buckets.
type ServerFileValidationRules struct {
	OldestFileMaxAgeInDays int `json:"oldest_file_max_age_in_days"`
	NewestFileMaxAgeInDays int `json:"newest_file_max_age_in_days"`
}

//FileDownloadRules contains parameters to adjust how many files get downloaded for manual verifications across different bucket types.
type FileDownloadRules struct {
	ServerBackups        int `json:"server_backups"`
	EpisodesFromEachShow int `json:"episodes_from_each_show"`
	PhotosFromThisMonth  int `json:"photos_from_this_month"`
	PhotosFromEachYear   int `json:"photos_from_each_year"`
}

//BucketAndFiles represents a mapping between a bucket and all the files for it to be downloaded for manual verification.
//It is used in the DownloadsInProgress.json file which itself is used for resuming downloads if the program ends early.
type BucketAndFiles struct {
	BucketName string   `json:"bucket_name"`
	Files      []string `json:"files"`
}
