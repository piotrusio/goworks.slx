package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
	"golang.org/x/sys/windows/svc"

	"github.com/salesworks/s-works/slx/cmd/slx-windows/service"
	"github.com/salesworks/s-works/slx/internal/app"
)

func main() {
	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Could not get executable path: %v\n", err)
	} else {
		exeDir := filepath.Dir(exePath)
		envPath := filepath.Join(exeDir, "slx.env")

		if err := godotenv.Load(envPath); err != nil {
			fmt.Printf("Error: Failed to load env file: %v\n", err)
		}
	}

	isInteractive, err := svc.IsAnInteractiveSession()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to determine interactive session: %v\n", err)
		os.Exit(1)
	}

	if !isInteractive {
		if err := service.Run(func(ctx context.Context) error {
			return runAsService()
		}); err != nil {
			os.Exit(1)
		}
		return
	}
	handleInteractiveCommands()
}

func runAsService() error {
	return app.Run()
}

func handleInteractiveCommands() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: slx-windows [install|uninstall|start|stop|debug]")
		return
	}

	cmd := os.Args[1]

	if cmd == "debug" {
		if err := app.Run(); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "debug run failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

	var err error
	switch cmd {
	case "install":
		err = service.Install()
	case "uninstall", "remove":
		err = service.Remove()
	case "start":
		err = service.Start()
	case "stop":
		err = service.Stop()
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Command '%s' failed: %v\n", cmd, err)
		os.Exit(1)
	}
	fmt.Printf("Command '%s' executed successfully.\n", cmd)
}
