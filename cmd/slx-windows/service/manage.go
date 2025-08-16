package service

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/eventlog"
	"golang.org/x/sys/windows/svc/mgr"
)

const (
	serviceName        = "SLX Service"
	serviceDisplayName = "SLX Integration Service"
	serviceDescription = "A background service for SalesWorks to XL Integrations."
)

// Install creates the Windows service.
func Install() error {
	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %w", err)
	}

	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	s, err := m.OpenService(serviceName)
	if err == nil {
		s.Close()
		return fmt.Errorf("service '%s' already exists", serviceName)
	}

	cfg := mgr.Config{
		DisplayName: serviceDisplayName,
		Description: serviceDescription,
		StartType:   mgr.StartAutomatic, // Start automatically on system boot.
	}

	s, err = m.CreateService(serviceName, exePath, cfg)
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}
	defer s.Close()

	// Install the event log source. This allows the service to write to the Windows Event Log.
	if err = eventlog.InstallAsEventCreate(serviceName, eventlog.Error|eventlog.Warning|eventlog.Info); err != nil {
		// If this fails, the service can still run, but won't log to the Event Viewer.
		// We can return a warning, but not a fatal error.
		fmt.Printf("Warning: Failed to install event log source (run as administrator?): %v\n", err)
	}

	return nil
}

// Remove deletes the Windows service.
func Remove() error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("service '%s' is not installed", serviceName)
	}
	defer s.Close()

	if err = s.Delete(); err != nil {
		return fmt.Errorf("failed to delete service: %w", err)
	}

	if err = eventlog.Remove(serviceName); err != nil {
		fmt.Printf("Warning: Failed to remove event log source: %v\n", err)
	}

	return nil
}

// Start sends the start command to the service.
func Start() error {
	return controlService(func(s *mgr.Service) error {
		return s.Start()
	})
}

// Stop sends the stop command to the service.
func Stop() error {
	return controlService(func(s *mgr.Service) error {
		status, err := s.Control(svc.Stop)
		if err != nil {
			return err
		}
		// Wait for the service to actually stop.
		timeout := time.After(15 * time.Second)
		for status.State != svc.Stopped {
			select {
			case <-time.After(300 * time.Millisecond):
				status, err = s.Query()
				if err != nil {
					return fmt.Errorf("failed to query service status: %w", err)
				}
			case <-timeout:
				return fmt.Errorf("timeout waiting for service to stop")
			}
		}
		return nil
	})
}

// controlService is a helper to connect to the SCM and perform an action.
func controlService(action func(s *mgr.Service) error) error {
	m, err := mgr.Connect()
	if err != nil {
		return fmt.Errorf("failed to connect to service manager: %w", err)
	}
	defer m.Disconnect()

	s, err := m.OpenService(serviceName)
	if err != nil {
		return fmt.Errorf("could not access service '%s': %w", serviceName, err)
	}
	defer s.Close()

	return action(s)
}
