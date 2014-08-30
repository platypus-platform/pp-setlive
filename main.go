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

	c := make(chan WorkSpec)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for w := range c {
			if w.Version == "" {
				// TODO: Ensure stopped
				Info("No active version for %s, skipping", w.App)
				continue
			}

			install := path.Join(w.Basedir, "installs", w.App+"_"+w.Version)
			current := path.Join(w.Basedir, "current")

			if _, err := os.Stat(install); os.IsNotExist(err) {
				Info("%s/%s has not been prepared, skipping", w.App, w.Version)
				continue
			}

			// TODO: obtain lease before stopping, only if running
			Info("Stopping %s", w.App)
			cmd := exec.Command("sv", "stop", path.Join(config.RunitPath, w.App))
			if err := cmd.Run(); err != nil {
				Fatal("Could not stop %s: %s", w.App, err)
				continue
			}

			// Ensure servicebuilder files up-to-date
			// TODO: How to provide all our custom options here? Template?
			Info("Writing servicebuilder config")
			sb := map[string]interface{}{}
			sb[w.App] = map[string][]string{
				"run": []string{
					path.Join(w.Basedir, "current/bin/launch"), // TODO Support directory
				},
			}
			data, err := yaml.Marshal(&sb)
			if err != nil {
				Fatal("Could not generate servicebuilder config: %s", err)
				continue
			}
			sbFile := path.Join(config.Sbpath, w.App+".yaml")
			err = ioutil.WriteFile(sbFile, data, 0644)
			if err != nil {
				Fatal("Could not write servicebuilder config: %s", err)
				continue
			}

			sbCmd := exec.Command("servicebuilder",
				"-c", config.Sbpath,
				"-d", config.RunitPath,
				"-s", config.RunitStagingPath,
			)
			if err := sbCmd.Run(); err != nil {
				Fatal("Could not run servicebuilder: %s", w.App, err)
				continue
			}

			Info("Symlinking")

			if err := os.Remove(current); err != nil {
				Fatal("Could not remove current symlink: %s", err)
				continue
			}

			if err := os.Symlink(install, current); err != nil {
				Fatal("Could not symlink current: %s", err)
				continue
			}

			Info("Starting %s", w.App)

			cmd = exec.Command("sv", "start", path.Join(config.RunitPath, w.App))
			if err := cmd.Run(); err != nil {
				Fatal("Could not start %s: %s", w.App, err)
				continue
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

type WorkSpec struct {
	App     string
	Version string
	Basedir string
}

// TODO: This is duplicated from pp-preparer. DRY it up.
func pollOnce(hostname string, c chan WorkSpec) error {
	Info("Polling intent store")
	kv, _ := ppkv.NewClient()
	apps, err := kv.List(path.Join("nodes", hostname))
	if err != nil {
		return err
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

		found := false
		for version, status := range versions {
			if status == "active" {
				found = true
				Info("Found active version: %s/%s", appName, version)
				c <- WorkSpec{
					App:     appName,
					Version: version,
					Basedir: basedir,
				}
				continue
			}
		}
		if !found {
			Info("No active version")
			c <- WorkSpec{
				App:     appName,
				Version: "",
				Basedir: basedir,
			}
		}
	}
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
