package settings_test

import (
	"io/ioutil"
	"os"
	"testing"
	"github.com/amrhassan/agentcontroller2/settings"
)

func TestLoadSettingsFromFile(t *testing.T) {

	settingsfile, _ := ioutil.TempFile("", "")

	settingsfile.WriteString("[Main]\n")
	settingsfile.WriteString("\n")
	settingsfile.WriteString("[[listen]]\n")
	settingsfile.WriteString("Address = \"1.2.3.4:1234\"\n")
	settingsfile.Close()
	defer os.Remove(settingsfile.Name())

	settings, err := settings.LoadSettingsFromTomlFile(settingsfile.Name())
	if err != nil {
		t.Error("Error while loading toml file", err)
	}

	//Check if the settings are loaded from the configuation file
	if settings.Listen[0].Address != "1.2.3.4:1234" {
		t.Error("Bind property not loaded from the configuration file")
	}

}
