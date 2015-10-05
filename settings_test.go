package main

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestLoadSettingsFromFile(t *testing.T) {

	settingsfile, _ := ioutil.TempFile("", "")

	settingsfile.WriteString("[Main]")
	settingsfile.WriteString("listen = \"1.2.3.4:1234\"\n")
	settingsfile.Close()
	defer os.Remove(settingsfile.Name())

	settings, err := LoadSettingsFromTomlFile(settingsfile.Name())
	if err != nil {
		t.Error("Error while loading toml file", err)
	}
	//Check if the settings are loaded from the configuation file
	if settings.Main.Listen != "1.2.3.4:1234" {
		t.Error("Bind property not loaded from the configuration file")
	}

}

func TestTLSEnabled(t *testing.T) {
	var settings Settings
	if settings.TLSEnabled() {
		t.Error("Empty settings should not have TLS enabled")
	}
	settings.TLS.Cert = "/path/to/Cert"

	if settings.TLSEnabled() {
		t.Error("Only a cert without a key should enable TLS")
	}
	settings.TLS.Key = "/path/to/key"
	if !settings.TLSEnabled() {
		t.Error("If both a cert and key are specified, TLS is enabled")
	}

}
