package main

import (
	"io/ioutil"
	"os"

	"github.com/naoina/toml"
)

//Settings are the configurable options for the AgentController
type Settings struct {
	Main struct {
		Listen        string
		RedisHost     string
		RedisPassword string
	}

	TLS struct {
		Cert string
		Key  string
	}

	Influxdb struct {
		Host     string
		Db       string
		User     string
		Password string
	}

	Handlers struct {
		Binary string
		Cwd    string
		Env    map[string]string
	}
}

//LoadSettingsFromTomlFile does exactly what the name says, it loads a toml in a Settings struct
func LoadSettingsFromTomlFile(filename string) (settings Settings, err error) {
	f, err := os.Open(filename)
	if err != nil {
		return
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	err = toml.Unmarshal(buf, &settings)
	return

}

func (settings Settings) TLSEnabled() bool {
	return settings.TLS.Cert != "" && settings.TLS.Key != ""
}
