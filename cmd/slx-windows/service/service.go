package service

import (
	"context"
	"errors"
	"sync"

	"golang.org/x/sys/windows/svc"
)

const (
	// A unique exit code for application-specific errors.
	appSpecificExitCode = 1
)

// AppRunner defines the signature for the function that runs the core application logic.
// This matches the signature of the `run` function in your main.go.
type AppRunner func(ctx context.Context) error

// windowsService implements the svc.Handler interface.
type windowsService struct {
	runApp    AppRunner
	cancel    context.CancelFunc
	appExited chan struct{}
}

// Execute is the entry point for the service, called by the Windows SCM.
func (s *windowsService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	changes <- svc.Status{State: svc.StartPending}

	// Create a cancellable context for the entire application.
	var appCtx context.Context
	appCtx, s.cancel = context.WithCancel(context.Background())
	defer s.cancel()

	s.appExited = make(chan struct{})

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	// Run the main application logic in a goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errChan <- s.runApp(appCtx)
	}()

	changes <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}

	for {
		select {
		case err := <-errChan:
			// The application finished. If it was due to an error (and not just context cancellation),
			// signal a service-specific error.
			if err != nil && !errors.Is(err, context.Canceled) {
				return true, appSpecificExitCode
			}
			// Normal exit.
			return false, 0

		case req := <-r:
			switch req.Cmd {
			case svc.Interrogate:
				changes <- req.CurrentStatus
			case svc.Stop, svc.Shutdown:
				changes <- svc.Status{State: svc.StopPending}
				// A stop was requested. Cancel the context to signal the app to shut down.
				s.cancel()
				wg.Wait() // Wait for the app goroutine to finish.
				return false, 0
			default:
				// Do nothing for unhandled commands.
			}
		}
	}
}

// Run starts the service. This is called from main.go.
func Run(runApp AppRunner) error {
	return svc.Run(serviceName, &windowsService{runApp: runApp})
}
