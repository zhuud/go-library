package app

import (
	"log"

	"github.com/spf13/cobra"
)

var (
	ConfPath string

	rootCmd = &cobra.Command{
		Use:     "run",
		Short:   "go run main.go [cmd | consumer cmd] [-f config file]",
		Long:    "go run main.go [cmd | consumer cmd] [-f config file]",
		Example: "go run main.go -f etc/config.test.yaml | go run main.go cmdxxx -f etc/config.test.yaml | go run main.go consumer consumerxxx -f etc/config.test.yaml",
	}
)

func init() {
	ConfPath = *rootCmd.PersistentFlags().StringP("config", "f", "", "the config file")
}

func Run() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("cmd.Run error: %v", err)
	}
}

func AddCommand(cmds ...*cobra.Command) {
	for _, cmd := range cmds {
		if cmd.Name() == "" || cmd.Name() == "serve" {
			rootCmd.Run = cmd.Run
		}
	}
	rootCmd.AddCommand(cmds...)
}
