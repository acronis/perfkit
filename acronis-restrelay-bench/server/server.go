// Package main implements an entrypoint logic of the benchmark server.
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/jessevdk/go-flags"

	"restrelay-bench/server/comm"
	"restrelay-bench/server/logic"
)

// CLI is a struct for command line arguments
type CLI struct {
	PostgresHost     string `long:"postgres-host" description:"sets host URL for postgres connection" default:"localhost"`
	MysqlHost        string `long:"mysql-host" description:"sets host URL for mysql connection" default:"localhost"`
	DatabaseUser     string `long:"db-user" description:"sets user for database connection" default:"test"`
	DatabasePassword string `long:"db-password" description:"sets password for database connection" default:"password"` // example value of a secret
	Host             string `short:"h" long:"host" description:"listen to this host" default:""`
	Port             int    `short:"p" long:"port" description:"listen to this port" default:"8080"`
	NumberInChain    int    `long:"number-in-chain" description:"number of server in topology" default:"0"`
	NextService      string `long:"next-service" description:"url of next server in topology"`

	HTTPServer  string `long:"server" description:"set http-server(standard|fast|gin)" default:"standard"`
	JSONPackage string `long:"json" description:"set json serializer(standard|jsoniter)" default:"standard"`
}

func parseCLI() CLI {
	cli := CLI{}
	parser := flags.NewNamedParser(os.Args[0], flags.Default)
	if _, err := parser.AddGroup("restrelay-bench flags", "This is flags for setting restrelay-bench up", &cli); err != nil {
		panic(err)
	}

	_, err := parser.Parse()
	if err != nil {
		flagsErr := err.(*flags.Error)
		if flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		panic(flagsErr)
	}

	return cli
}

func handleCLI(cli CLI) (string, string) {
	nextServiceEnvVar := os.Getenv("NEXT_SERVICE")

	if nextServiceEnvVar != "" {
		cli.NextService = nextServiceEnvVar
	}

	numberInChainEnvVar := os.Getenv("NUMBER_IN_CHAIN")

	if numberInChainEnvVar != "" {
		cli.NumberInChain, _ = strconv.Atoi(numberInChainEnvVar)
	}

	SetupEnvironment(cli.NumberInChain, cli.NextService)

	err := logic.SetupDatabaseEnvironment(
		cli.DatabaseUser,
		cli.DatabasePassword,
		cli.PostgresHost,
		cli.MysqlHost,
	)
	if err != nil {
		log.Fatalf("error during database setup with %s\n", err)
	}

	switch cli.JSONPackage {
	case "standard":
		logic.UseJsoniter = false
	case "jsoniter":
		logic.UseJsoniter = true
	default:
		log.Fatal("Unknown -- json flag value")
	}

	return cli.HTTPServer, fmt.Sprintf("%s:%d", cli.Host, cli.Port)
}

// baseURL is an URL address of next server
var baseURL string

// nodeName based on numberInChain and needed for getting parameters from input request's url
var nodeName string

// SetupEnvironment sets all required global variables for methods package
func SetupEnvironment(NumberInChain int, BaseURL string) {
	baseURL = BaseURL
	nodeName = fmt.Sprintf("n%d", NumberInChain)
}

func main() {
	logic.SetupReferenceIterations()
	var serverType, serverAddr = handleCLI(parseCLI())

	fmt.Println("started")

	if err := comm.RunServer(serverType, serverAddr, baseURL, nodeName); err != nil {
		log.Fatal(err)
	}
}
