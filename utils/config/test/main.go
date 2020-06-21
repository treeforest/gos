package main

import (
	"github.com/treeforest/gos/utils/config/source/file"
			"github.com/treeforest/gos/utils/config"
		"fmt"
	"github.com/treeforest/gos/utils/config/encoder/json"
	"github.com/treeforest/gos/utils/config/source"
	)

type Host struct {
	Name    string `json:"name"`
	Address string `json:"address"`
	Port    int    `json:"port"`
}

func TestDefaultConfig() {
	// Load file source
	//e := json.NewEncoder()
	if err := config.Load(file.NewSource(
		file.WithPath("./config/config.json"),
		//source.WithEncoder(e),
	)); err != nil {
		panic(err)
	}

	var host Host
	if err := config.Get("hosts", "database").Scan(&host); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(host.Name, host.Address, host.Port)
}

func TestConfig() {
	conf, _ := config.NewConfig()

	e := json.NewEncoder()
	if err := conf.Load(file.NewSource(
		file.WithPath("./config/config.json"),
		source.WithEncoder(e),
	)); err != nil {
		panic(err)
	}

	var host Host

	if err := conf.Get("hosts", "database").Scan(&host); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(host.Name, host.Address, host.Port)
}

func TestWatch() {
	var host Host

	config.Load(file.NewSource(
		file.WithPath("./config/config.json"),
	))

	for {
		w, err := config.Watch("hosts", "database")
		if err != nil {
			fmt.Println("Watch error: ", err)
			return
		}

		v, err := w.Next()
		if err != nil {
			fmt.Println("Next error: ", err)
			return
		}

		v.Scan(&host)

		fmt.Println(host)
	}
}

func main() {

	//TestDefaultConfig()
	//TestConfig()
	TestWatch()

}
