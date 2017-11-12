package validatebackups

import (
	"encoding/json"
	"os"

	"github.com/juju/errors"
)

func main() {
	//read path to config file from cli
	//load config from file
	//connect to gcs
	//loop over relevant buckets
}

func loadConfigurationFromFile(filePath string) (config Config, err error) {
	configFile, err := os.Open(filePath)
	defer configFile.Close()
	if err != nil {
		err = errors.Annotatef(err, "Unable to open config file at %s", filePath)
		return
	}
	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	return
}
