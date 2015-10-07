package main

import (
	"io/ioutil"
	"os"

	"github.com/naoina/toml"
)

//HTTPBinding defines the address that should be bound on and optional tls certificates
type HTTPBinding struct {
	Address string
	TLS     []struct {
		Cert string
		Key  string
	}
	ClientCA []struct {
		Cert string
	}
}

//Settings are the configurable options for the AgentController
type Settings struct {
	Main struct {
		RedisHost     string
		RedisPassword string
	}

	Listen []HTTPBinding

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

	Syncthing struct {
		Port int
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

//TLSEnabled returns true if TLS settings are present
func (httpBinding HTTPBinding) TLSEnabled() bool {
	return len(httpBinding.TLS) > 0
}

//ClientCertificateRequired returns true if ClientCA's are present
func (httpBinding HTTPBinding) ClientCertificateRequired() bool {
	return len(httpBinding.ClientCA) > 0
}
