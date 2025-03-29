// Package benchmark is a library to create benchmarks.
//
// It includes a CLI wrapper for go-flags library, common options for every test,
// database options for the test, and a logger for logging messages.
//
// The CLI wrapper provides functionalities to initialize the CLI with the application name and common options,
// set the application name, add default group, add flag group, set usage, set description, and parse the CLI arguments.
//
// Example:
//
//	cli := &CLI{}
//	cli.Init("My Application", &CommonOpts{})
//	cli.SetApplicationName("New Application Name")
//	cli.AddFlagGroup("Group Name", "Group Description", &DatabaseOpts{})
//	cli.SetUsage("Usage Information")
//	cli.SetDescription("Description Information")
//	cli.Parse()
//
// The common options include verbose, workers, loops, duration, sleep, repeat, quiet, and randseed.
//
// Example:
//
//	commonOpts := &CommonOpts{
//	    Verbose:  []bool{true, false},
//	    Workers:  5,
//	    Loops:    10,
//	    Duration: 15,
//	    Sleep:    20,
//	    Repeat:   2,
//	    Quiet:    false,
//	    RandSeed: 123456789,
//	}
//
// The database options include driver, dsn, dontCleanup, useTruncate, maxOpenConns, mySQLEngine, reconnect, and dryRun.
//
// Example:
//
//	dbOpts := &DatabaseOpts{
//	    Driver:       "postgres",
//	    Dsn:          "host=127.0.0.1 sslmode=disable user=test_user",
//	    DontCleanup:  false,
//	    UseTruncate:  true,
//	    MaxOpenConns: 10,
//	    MySQLEngine:  "innodb",
//	    Reconnect:    true,
//	    DryRun:       false,
//	}
//
// The logger provides functionalities to create a new logger, log a message, and log a message with a specific level.
//
// Example:
//
//	logger := logger.NewPlaneLogger(logger.LevelWarn, false)
//	logger.Log(logger.LevelWarn, "This is a log message")
package benchmark
