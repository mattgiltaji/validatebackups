package main

type Config struct {
	GoogleAuthFileLocation string                    `json:"google_auth_file_location"`
	FileDownloadLocation   string                    `json:"file_download_location"`
	ServerBackupRules      ServerFileValidationRules `json:"server_backup_rules"`
	FilesToDownload        FileDownloadRules         `json:"files_to_download"`
	Buckets                []BucketToProcess         `json:"buckets"`
}

type BucketToProcess struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type ServerFileValidationRules struct {
	OldestFileMaxAgeInDays int `json:"oldest_file_max_age_in_days"`
	NewestFileMaxAgeInDays int `json:"newest_file_max_age_in_days"`
}

type FileDownloadRules struct {
	ServerBackups        int `json:"server_backups"`
	EpisodesFromEachShow int `json:"episodes_from_each_show"`
	PhotosFromThisMonth  int `json:"photos_from_this_month"`
	PhotosFromEachYear   int `json:"photos_from_each_year"`
}
