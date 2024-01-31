package benchmark

import (
	"errors"
	"fmt"
	"os"

	"github.com/jessevdk/go-flags"
)

// CommonOpts represents common flags for every test
type CommonOpts struct {
	Verbose  []bool `short:"v" long:"verbose" description:"Show verbose debug information (-v - info, -vv - debug)"`
	Workers  int    `short:"c" long:"concurrency" description:"sets number of goroutines that runs testing function" default:"0"`
	Loops    int    `short:"l" long:"loops" description:"sets TOTAL(not per worker) number of iterations of testing function(greater priority than DurationSec)" default:"0"`
	Duration int    `short:"d" long:"duration" description:"sets duration(in seconds) for work time for every loop" default:"5"`
	Sleep    int    `short:"S" long:"sleep" description:"sleep given amount of msec between requests" required:"false" default:"0"`
	Repeat   int    `short:"r" long:"repeat" description:"repeat the test given amount of times" required:"false" default:"1"`
	Quiet    bool   `short:"Q" long:"quiet" description:"be quiet and print as less information as possible"`
	RandSeed int64  `short:"s" long:"randseed" description:"Seed used for random number generation" required:"false" default:"1"`
}

// DatabaseOpts represents common flags for every test
type DatabaseOpts struct {
	Driver       string `long:"driver" description:"db driver (postgres|mysql|sqlite3)" default:"postgres" required:"false"`
	Dsn          string `long:"dsn" description:"dsn connection string" default:"host=127.0.0.1 sslmode=disable user=test_user" required:"false"`
	DontCleanup  bool   `long:"dont-cleanup" description:"do not cleanup DB content before/after the test in '-t all' mode" required:"false"`
	UseTruncate  bool   `long:"use-truncate" description:"use TRUNCATE instead of DROP TABLE in cleanup procedure" required:"false"`
	MaxOpenConns int    `long:"maxopencons" description:"Set sql/db MaxOpenConns per worker, default value is set to 2 because the benchmark uses it's own workers pool" default:"2" required:"false"`
	MySQLEngine  string `long:"mysql-engine" description:"mysql engine (innodb|myisam|xpand|...)" default:"innodb" required:"false"`
	Reconnect    bool   `long:"reconnect" description:"reconnect to DB before every test iteration" required:"false"`
	DryRun       bool   `long:"dry-run" description:"do not execute any INSERT/UPDATE/DELETE queries on DB-side" required:"false"`
}

// CLI is a wrapper for go-flags library
type CLI struct {
	parser     *flags.Parser
	commonOpts *CommonOpts
}

// Init initializes CLI with given application name and commonOptsPointer.
func (cli *CLI) Init(applicationName string, commonOptsPointer *CommonOpts) {
	cli.parser = flags.NewNamedParser(applicationName, flags.Default)
	cli.commonOpts = commonOptsPointer
}

// SetApplicationName sets application name.
func (cli *CLI) SetApplicationName(name string) {
	cli.parser.Name = name
}

// addDefaultGroup adds default group with common options.
func (cli *CLI) addDefaultGroup(commonOptsPointer *CommonOpts) {
	_, err := cli.parser.AddGroup("Common options", "CommonOptions represents common flags for every test", commonOptsPointer)
	handleAddGroupError(err)
}

// AddFlagGroup adds flags in struct flagsPtr(should be pointer!) to given group.
func (cli *CLI) AddFlagGroup(groupName, groupDescription string, flagsPtr interface{}) {
	_, err := cli.parser.AddGroup(groupName, groupDescription, flagsPtr)
	handleAddGroupError(err)
}

// handleAddGroupError handles error from AddGroup.
func handleAddGroupError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}
}

// checkCommonOpts checks common options.
func (cli *CLI) checkCommonOpts() error {
	if cli.commonOpts.Duration < 1 {
		return errors.New("duration should be > 0")
	}
	if cli.commonOpts.Loops < 0 {
		return errors.New("loops should be >= 0")
	}
	if cli.commonOpts.Workers < 1 {
		cli.commonOpts.Workers = 1
	}

	return nil
}

// SetUsage sets usage.
func (cli *CLI) SetUsage(usage string) {
	cli.parser.Usage = usage
}

// SetDescription sets description.
func (cli *CLI) SetDescription(description string) {
	cli.parser.Usage = cli.parser.Usage + "\n" + description
}

// Parse initializes CLI arguments.
func (cli *CLI) Parse() []string {
	cli.addDefaultGroup(cli.commonOpts)
	values, err := cli.parser.Parse()
	if err != nil {
		var flagsError *flags.Error
		errors.As(err, &flagsError)
		if errors.Is(flagsError.Type, flags.ErrHelp) {
			os.Exit(0)
		}
		fmt.Println(err)
		os.Exit(0)
	}
	err = cli.checkCommonOpts()
	if err != nil {
		fmt.Println(err)
		os.Exit(0)
	}

	return values
}
