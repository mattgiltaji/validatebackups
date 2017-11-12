package validatebackups

import (
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	//read path to config file from cli
	//load config from file
	//connect to gcs
	//loop over relevant buckets
}

func loadConfigurationFromFile(filePath string) Config {
	var config Config
	configFile, err := os.Open(filePath)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)
	return config
}
