package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/sys/windows/svc"
)

const (
	// A unique exit code for application-specific errors.
	appSpecificExitCode = 1
	// Maximum time to wait for graceful shutdown
	maxShutdownTime = 30 * time.Second
)

// AppRunner defines the signature for the function that runs the core application logic.
// This matches the signature of the `run` function in your main.go.
type AppRunner func(ctx context.Context) error

// windowsService implements the svc.Handler interface.
type windowsService struct {
	runApp    AppRunner
	cancel    context.CancelFunc
	appCtx    context.Context
	appDone   chan error
	mu        sync.Mutex
}

// Execute is the entry point for the service, called by the Windows SCM.
func (s *windowsService) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (bool, uint32) {
	changes <- svc.Status{State: svc.StartPending}

	// Create a cancellable context for the entire application.
	s.mu.Lock()
	s.appCtx, s.cancel = context.WithCancel(context.Background())
	s.appDone = make(chan error, 1)
	s.mu.Unlock()

	// Run the main application logic in a goroutine.
	go func() {
		defer close(s.appDone)
		s.appDone <- s.runApp(s.appCtx)
	}()

	changes <- svc.Status{State: svc.Running, Accepts: svc.AcceptStop | svc.AcceptShutdown}

	for {
		select {
		case err := <-s.appDone:
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
				
				// Signal the app to shut down
				s.mu.Lock()
				if s.cancel != nil {
					s.cancel()
				}
				s.mu.Unlock()

				// Wait for graceful shutdown with timeout
				select {
				case err := <-s.appDone:
					if err != nil && !errors.Is(err, context.Canceled) {
						return true, appSpecificExitCode
					}
					return false, 0
				case <-time.After(maxShutdownTime):
					// Force exit if graceful shutdown takes too long
					return true, appSpecificExitCode
				}
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