package main

import (
	"errors"
	"github.com/platypus-platform/pp-kv-consul"
	. "github.com/platypus-platform/pp-logging"
	"gopkg.in/yaml.v1"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sync"
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

	c := make(chan IntentNode)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for intent := range c {
			for _, app := range intent.Apps {
				setLive(config, app)
			}
		}
	}()

	err = pollOnce(hostname, c)
	close(c)
	wg.Wait()
	if err != nil {
		Fatal(err.Error())
		os.Exit(1)
	}
}

func setLive(config SetliveConfig, app IntentApp) {
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

func configureServiceBuilder(config SetliveConfig, app IntentApp) error {
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

type IntentApp struct {
	Name     string
	Versions map[string]string
	Basedir  string
}

type IntentNode struct {
	Apps map[string]IntentApp
}

func (app *IntentApp) ActiveVersion() string {
	for id, status := range app.Versions {
		if status == "active" {
			return id
		}
	}
	return ""
}

// TODO: This is duplicated from pp-preparer. DRY it up.
func pollOnce(hostname string, c chan IntentNode) error {
	Info("Polling intent store")
	kv, _ := ppkv.NewClient()
	apps, err := kv.List(path.Join("nodes", hostname))
	if err != nil {
		return err
	}

	intent := IntentNode{
		Apps: map[string]IntentApp{},
	}

	for appName, data := range apps {
		Info("Checking spec for %s", appName)

		appData, worked := stringMap(data)
		if !worked {
			Fatal("Invalid node data for %s", appName)
			continue
		}

		cluster := appData["cluster"]
		if cluster == "" {
			Fatal("No cluster key in node data for %s", appName)
			continue
		}

		clusterKey := path.Join("clusters", appName, cluster, "versions")
		configKey := path.Join("clusters", appName, cluster, "deploy_config")

		versions, err := getMap(kv, clusterKey)
		if err != nil {
			Fatal("No or invalid data for %s: %s", clusterKey, err)
			continue
		}

		deployConfig, err := getMap(kv, configKey)
		if err != nil {
			Fatal("No or invalid data for %s: %s", configKey, err)
			continue
		}

		basedir := deployConfig["basedir"]
		if !path.IsAbs(basedir) {
			Fatal("Not allowing relative basedir in %s", configKey)
			continue
		}

		intent.Apps[appName] = IntentApp{
			Name:     appName,
			Basedir:  basedir,
			Versions: versions,
		}
	}

	c <- intent

	return nil
}

func getMap(kv *ppkv.Client, query string) (map[string]string, error) {
	raw, err := kv.Get(query)

	if err != nil {
		return nil, err
	}

	mapped, worked := stringMap(raw)
	if !worked {
		return nil, errors.New("Not a string map")
	}

	return mapped, nil
}

func stringMap(raw interface{}) (map[string]string, bool) {
	mapped, worked := raw.(map[string]interface{})
	if !worked {
		return nil, false
	}
	ret := map[string]string{}
	for k, v := range mapped {
		str, worked := v.(string)
		if !worked {
			return nil, false
		}
		ret[k] = str
	}
	return ret, true
}
