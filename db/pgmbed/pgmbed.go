package pgmbed

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/acronis/perfkit/logger"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
)

var (
	embeddedPostgresInitialized sync.Once
	embeddedPostgresRefCount    int
	embeddedPostgresMutex       sync.Mutex

	// Only one instance of embedded Postgres is allowed
	// TODO: make it possible to run multiple instances of embedded Postgres
	embeddedPostgresInstance *embeddedpostgres.EmbeddedPostgres
)

// Opts is a structure to store all the embedded postgresql options
type Opts struct {
	Enabled        bool
	Port           int
	DataDir        string
	MaxConnections int
}

// ParseOptions parses the CS string to extract the embedded Postgres options and returns a cleaned CS.
func ParseOptions(cs string) (string, *Opts, error) {
	// Parse the CS to extract query parameters.
	parsedURL, err := url.Parse(cs)
	if err != nil {
		return "", nil, fmt.Errorf("pgmbed: invalid connection string: %v", err)
	}

	queryParams := parsedURL.Query()

	// Create a new embeddedPostgresOpts with defaults
	opts := &Opts{
		Enabled:        false, // default value
		Port:           5433,  // default value
		DataDir:        "",    // default value
		MaxConnections: 512,   // default value
	}

	// Extract and parse the query parameters.
	if enabled, exists := queryParams["embedded-postgres"]; exists {
		if opts.Enabled, err = strconv.ParseBool(enabled[0]); err != nil {
			return "", nil, fmt.Errorf("invalid value for embedded-postgres: %v", err)
		}
		delete(queryParams, "embedded-postgres") // Remove from query params
	}

	if port, exists := queryParams["ep-port"]; exists {
		if opts.Port, err = strconv.Atoi(port[0]); err != nil {
			return "", nil, fmt.Errorf("invalid value for ep-port: %v", err)
		}
		delete(queryParams, "ep-port") // Remove from query params
	}

	if dataDir, exists := queryParams["ep-data-dir"]; exists {
		opts.DataDir = dataDir[0]
		delete(queryParams, "ep-data-dir") // Remove from query params
	}

	if maxConns, exists := queryParams["ep-max-connections"]; exists {
		if opts.MaxConnections, err = strconv.Atoi(maxConns[0]); err != nil {
			return "", nil, fmt.Errorf("invalid value for ep-max-connections: %v", err)
		}
		delete(queryParams, "ep-max-connections") // Remove from query params
	}

	// Rebuild connection string without the embedded Postgres parameters.
	parsedURL.RawQuery = queryParams.Encode()
	cleanedConnectionString := parsedURL.String()

	return cleanedConnectionString, opts, nil
}

func packConnectionString(cs string, opts *Opts) string {
	if cs == "" || opts == nil {
		return cs
	}

	var u, err = url.Parse(cs)
	if err != nil {
		return cs
	}

	u.Host = fmt.Sprintf("localhost:%d", opts.Port)
	u.User = url.UserPassword("postgres", "postgres")
	u.Path = "/postgres"

	cs = u.String()
	return cs
}

type embeddedPostgresLogger struct {
	logger logger.Logger
}

func (l embeddedPostgresLogger) Write(p []byte) (n int, err error) {
	if l.logger == nil {
		return len(p), nil
	}

	message := string(p)

	var lines = strings.Split(message, "\n")
	for _, line := range lines {
		l.logger.Info("-- embedded postgres: %s\n", line)
	}

	return len(p), nil
}

func getEmbeddedPostgresDataDir(dir string, logger logger.Logger) (string, error) {
	if dir == "" {
		dir = ".embedded-postgres-go"
		if userHome, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(userHome, dir)
		}
		dir = filepath.Join(dir, "data")
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if logger != nil {
			logger.Info("creating Embedded Postgres data dir: " + dir)
		}
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return "", fmt.Errorf("failed to create data directory: %v", err)
		}
	}

	if logger != nil {
		logger.Info("using Embedded Postgres data dir: " + dir)
	}

	return dir, nil
}

// Launch starts the embedded Postgres instance if it's enabled and not already running.
func Launch(cs string, opts *Opts, logger logger.Logger) (string, error) {
	if opts == nil || !opts.Enabled {
		// If embedded Postgres is not enabled, return the original connection string.
		return cs, nil
	}

	// Ensure that the embedded Postgres instance is initialized only once.
	embeddedPostgresMutex.Lock()
	defer embeddedPostgresMutex.Unlock()

	// Increase the reference counter for a new connection.
	embeddedPostgresRefCount++

	// Ensure that the embedded Postgres instance is initialized only once.
	var initErr error
	embeddedPostgresInitialized.Do(func() {
		var port = uint32(opts.Port)
		var maxConnections = strconv.Itoa(opts.MaxConnections)
		var dataDir string
		dataDir, initErr = getEmbeddedPostgresDataDir(opts.DataDir, logger)

		// Initialize embedded Postgres with the specified configuration.
		embeddedPostgresInstance = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Port(port).
			DataPath(dataDir).
			Logger(embeddedPostgresLogger{logger: logger}).
			StartParameters(map[string]string{
				"max_connections":  maxConnections,
				"jit":              "off",
				"random_page_cost": "1.1"}))

		// Start the embedded Postgres instance.
		if initErr = embeddedPostgresInstance.Start(); initErr != nil {
			if initErr.Error() == fmt.Sprintf("process already listening on port %d", port) {
				initErr = nil
			} else {
				embeddedPostgresInstance = nil
				initErr = fmt.Errorf("embedded Postgres DB start error: %v", initErr)
			}
		}
	})

	if initErr != nil {
		return "", initErr
	}

	// Return the modified DSN without the embedded Postgres options.
	return packConnectionString(cs, opts), nil
}

// Terminate finishes the embedded Postgres instance if it's running.
func Terminate() error {
	embeddedPostgresMutex.Lock()
	defer embeddedPostgresMutex.Unlock()

	// Decrease the reference counter when a connection is closed.
	embeddedPostgresRefCount--

	// If no more connections are active, terminate the embedded Postgres instance.
	if embeddedPostgresRefCount == 0 {
		return terminateEmbeddedPostgres()
	}

	return nil
}

// terminateEmbeddedPostgres terminates the embedded Postgres instance if it's running.
func terminateEmbeddedPostgres() error {
	if embeddedPostgresInstance == nil {
		return nil
	}

	var err = embeddedPostgresInstance.Stop()
	embeddedPostgresInstance = nil

	if err != nil {
		return fmt.Errorf("embedded Postgres DB stop error: %v", err)
	}

	return nil
}
