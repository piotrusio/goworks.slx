package main

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/salesworks/s-works/slx/internal/app"
)

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Warning: no .env file found in current directory\n")
	}

	if err := app.Run(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}
