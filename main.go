package main

import (
	. "github.com/platypus-platform/pp-logging"
	"github.com/platypus-platform/pp-store"
	"gopkg.in/yaml.v1"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
)

type SetliveConfig struct {
	Sbpath           string
	RunitStagingPath string
	RunitPath        string
}

func main() {
	hostname, err := os.Hostname()
	if err != nil {
		Fatal(err.Error())
		os.Exit(1)
	}
	config := SetliveConfig{
		Sbpath:           "/etc/servicebuilder.d",
		RunitStagingPath: "/var/service-stage",
		RunitPath:        "/var/service",
	}

	err = pp.PollIntent(hostname, func(intent pp.IntentNode) {
		for _, app := range intent.Apps {
			setLive(config, app)
		}
	})

	if err != nil {
		Fatal(err.Error())
		os.Exit(1)
	}
}

func setLive(config SetliveConfig, app pp.IntentApp) {
	version := app.ActiveVersion()

	if version == "" {
		// TODO: Ensure stopped
		Info("%s: No active version, skipping", app.Name)
		return
	}

	install := path.Join(app.Basedir, "installs", app.Name+"_"+version)
	current := path.Join(app.Basedir, "current")

	if _, err := os.Stat(install); os.IsNotExist(err) {
		Info("%s: not prepared, skipping", app)
		return
	}

	// TODO: obtain lease before stopping, only if running
	Info("%s: Stopping", app.Name)
	cmd := exec.Command("sv", "stop", path.Join(config.RunitPath, app.Name))
	if err := cmd.Run(); err != nil {
		Fatal("%s: Could not stop: %s", app.Name, err)
		return
	}

	// Ensure servicebuilder files up-to-date
	// TODO: How to provide all our custom options here? Template?
	Info("%s: Configuring service builder", app.Name)
	if err := configureServiceBuilder(config, app); err != nil {
		Fatal("%s: Could not configure servicebuilder: %s", app.Name, err)
		return
	}

	Info("%s: Symlinking", app.Name)

	if err := os.Remove(current); err != nil {
		Fatal("%s: Could not remove current symlink: %s", app.Name, err)
		return
	}

	if err := os.Symlink(install, current); err != nil {
		Fatal("%s: Could not symlink current: %s", app.Name, err)
		return
	}

	Info("%s: Starting", app.Name)

	cmd = exec.Command("sv", "start", path.Join(config.RunitPath, app.Name))
	if err := cmd.Run(); err != nil {
		Fatal("%s: Could not start: %s", app.Name, err)
		return
	}
}

func configureServiceBuilder(config SetliveConfig, app pp.IntentApp) error {
	sb := map[string]interface{}{}
	sb[app.Name] = map[string][]string{
		"run": []string{
			path.Join(app.Basedir, "current/bin/launch"), // TODO Support directory
		},
	}

	data, err := yaml.Marshal(&sb)
	if err != nil {
		return err
	}

	sbFile := path.Join(config.Sbpath, app.Name+".yaml")
	err = ioutil.WriteFile(sbFile, data, 0644)
	if err != nil {
		return err
	}

	sbCmd := exec.Command("servicebuilder",
		"-c", config.Sbpath,
		"-d", config.RunitPath,
		"-s", config.RunitStagingPath,
	)
	if err := sbCmd.Run(); err != nil {
		return err
	}
	return nil
}
