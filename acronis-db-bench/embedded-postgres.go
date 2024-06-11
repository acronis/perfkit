package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres" // embedder postgres
)

// EmbeddedPostgresOpts is a structure to store all the embedded postgresql options
type EmbeddedPostgresOpts struct {
	Port           int    `long:"ep-port" description:"embedded postgres port (default 5433)" required:"false" default:"5433"`
	DataDir        string `long:"ep-data-dir" description:"embedded postgres data dir path (default: $USER_HOME/.embedded-postgres-go/data)" required:"false" default:""`
	MaxConnections int    `long:"ep-max-connections" description:"embedded postgres 'max_connections' (default 512)" required:"false" default:"512"`
}

type embeddedPostgresLogger struct {
	benchmark *benchmark.Benchmark
}

func (l embeddedPostgresLogger) Write(p []byte) (n int, err error) {
	message := string(p)

	if l.benchmark.Logger.LogLevel >= benchmark.LogInfo {
		lines := strings.Split(message, "\n")

		for _, line := range lines {
			fmt.Printf("-- embedded postgres: %s\n", line)
		}
	}

	return len(p), nil
}

func getEmbeddedPostgresDataDir(b *benchmark.Benchmark) string {
	dir := b.TestOpts.(*TestOpts).EmbeddedPostgresOpts.DataDir
	if dir == "" {
		dir = ".embedded-postgres-go"
		if userHome, err := os.UserHomeDir(); err == nil {
			dir = filepath.Join(userHome, dir)
		}
		dir = filepath.Join(dir, "data")
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		b.Log(benchmark.LogInfo, 0, "Creating Embedded Postgres data dir: "+dir)
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			b.Exit("Failed to create data directory: %v", err)
		}
	}

	b.Log(benchmark.LogInfo, 0, "Using Embedded Postgres data dir: "+dir)

	return dir
}

func initEmbeddedPostgres(b *benchmark.Benchmark) {
	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("can't cast testOpts")
	}

	if testOpts.DBOpts.EmbeddedPostgres {
		testOpts.DBOpts.Driver = "postgres"
		port := uint32(b.TestOpts.(*TestOpts).EmbeddedPostgresOpts.Port)
		maxConnections := strconv.Itoa(b.TestOpts.(*TestOpts).EmbeddedPostgresOpts.MaxConnections)
		testOpts.DBOpts.Dsn = fmt.Sprintf("host=localhost port=%d user=postgres password=postgres dbname=postgres sslmode=disable", port)
		dataDir := getEmbeddedPostgresDataDir(b)

		database := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
			Port(port).
			DataPath(dataDir).
			Logger(embeddedPostgresLogger{benchmark: b}).
			StartParameters(map[string]string{"max_connections": maxConnections, "jit": "off", "random_page_cost": "1.1"}))

		b.Vault.(*DBTestData).EmbeddedPostgres = database
		if err := database.Start(); err != nil {
			b.Vault.(*DBTestData).EmbeddedPostgres = nil
			b.Exit("Embedded Postgres DB start error: %v", err)
		}
	}
}

func finiEmbeddedPostgres(b *benchmark.Benchmark) {
	if b.Vault != nil && b.TestOpts != nil {
		testOpts, ok := b.TestOpts.(*TestOpts)
		if !ok {
			b.Exit("can't cast testOpts")
		}
		if testOpts.DBOpts.EmbeddedPostgres {
			database := b.Vault.(*DBTestData).EmbeddedPostgres
			if database != nil {
				if err := database.Stop(); err != nil {
					b.Vault.(*DBTestData).EmbeddedPostgres = nil
					b.Exit("Embedded Postgres DB stop error: %v", err)
				}
			}
		}
	}
}
