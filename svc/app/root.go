package app

import (
	"log"

	"github.com/spf13/cobra"
	"github.com/zeromicro/go-zero/core/proc"
)

var (
	rootCmd = &cobra.Command{
		Use:     "run",
		Short:   "go run main.go [-f config file] [cmd | consumer]",
		Long:    "go run main.go [-f config file] [cmd | consumer]",
		Example: "go run main.go -f etc/config.test.yaml | go run main.go -f etc/config.test.yaml cmdxxx | go run main.go -f etc/config.test.yaml consumerxxx",
	}
)

func init() {
	rootCmd.PersistentFlags().StringP("config", "f", "", "the config file")
}

func Run() {
	defer proc.Shutdown()
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
