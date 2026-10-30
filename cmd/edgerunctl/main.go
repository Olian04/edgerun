// Command edgerunctl manages an edgerun orchestrator from the command line.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/Olian04/edgerun/internal/ctl"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	os.Exit(ctl.Run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}
