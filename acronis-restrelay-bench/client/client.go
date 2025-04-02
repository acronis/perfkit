// Package main provides benchmarking client utilities for restrelay-bench.
// It includes various tests and options for restrelay-bench server performance analysis.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/acronis/perfkit/benchmark"
)

var expirationDuration time.Duration

// Options is a struct for command line arguments
type Options struct {
	Scenario               string `short:"s" long:"scenario" description:"valid url case for request" default:"sleep-sleep-sleep"`
	Host                   string `long:"host" description:"restrelay-bench server host name or IP" default:"localhost"`
	Port                   int    `short:"p" long:"port" description:"restrelay-bench server port" default:"0"`
	SSLEnabled             bool   `long:"ssl-enabled" description:"enables requesting via http"`
	Log                    string `long:"logger-type" description:"(none|json|text)" default:"json"`
	DisableKeepAliveServer bool   `long:"disable-keep-alive-server" description:"disables keep-alive connections between servers"`
	DisableKeepAliveClient bool   `long:"disable-keep-alive-client" description:"disables keep-alive connections between client and server"`
	// ExpirationConnecion    string `long:"keep-alive-expiration" description:"expiration time for keep-alive connections in golangs duration format" default:"0s"`
	ListScenarios  bool   `short:"L" long:"list-scenarios" description:"prints prebuilt scenarios"`
	CustomScenario string `short:"C" long:"custom-scenario" description:"set custom scenario"`
}

type logsStore struct {
	mutex sync.Mutex
	logs  []logRecord
}

func (store *logsStore) Append(log logRecord) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.logs = append(store.logs, log)
}

func (store *logsStore) GetLogs() []logRecord {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	return store.logs
}

type store struct {
	url  string
	logs logsStore
}

const (
	entrypointURLFormat = "%s://%s/api/restrelay_bench_server/?connection=%s%s"
)

func buildBaseURL(host string, port int, sslEnabled bool, disableKeepAlive bool, query string) string {
	var connection string
	if disableKeepAlive {
		connection = "close"
	} else {
		connection = "keep-alive"
	}

	var schema string
	if sslEnabled {
		schema = "https"
	} else {
		schema = "http"
	}

	if port != 0 {
		host = fmt.Sprintf("%s:%d", host, port)
	}

	return fmt.Sprintf(entrypointURLFormat, schema, host, connection, query)
}

var listAliases = map[string][]string{
	"echo":                       {},
	"forward":                    {"n0=forward"},
	"forward-forward":            {"n0=forward", "n1=forward"},
	"forward-forward_data":       {"n0=forwarddata(size=4KB)", "n1=forwarddata(size=4KB)"},
	"forward_close_keep_alive":   {"n0=forward(keepalive=true)"},
	"busyloop_1ms":               {"n0=busyloop(duration=1ms)"},
	"busyloop_50ms":              {"n0=busyloop(duration=50ms)"},
	"busyloop_10M_times":         {"n0=busyloop(iterations=10000000)"},
	"busyloop_1M_times":          {"n0=busyloop(iterations=1000000)"},
	"sleep_1ms":                  {"n0=sleep(duration=1ms)"},
	"sleep_50ms":                 {"n0=sleep(duration=50ms)"},
	"db_postgres_select_1":       {"n0=db(engine=postgres,query=1)"},
	"db_mysql_select_1":          {"n0=db(engine=mysql,query=1)"},
	"forward-forward-db":         {"n0=forward", "n1=forward", "n2=db(engine=postgres,query=1)"},
	"allocate_100":               {"n0=allocate(size=100B)"},
	"allocate_1000":              {"n0=allocate(size=1KB)"},
	"allocate_10000":             {"n0=allocate(size=10KB)"},
	"log_print_logf_json_1000":   {"n0=log(skip=false,logger=logf,type=json,length=1000)"},
	"log_skip_logf_json_1000":    {"n0=log(skip=true,logger=logf,type=json,length=1000)"},
	"log_print_logf_text_1000":   {"n0=log(skip=false,logger=logf,type=text,length=1000)"},
	"log_skip_logf_text_1000":    {"n0=log(skip=true,logger=logf,type=text,length=1000)"},
	"log_print_logrus_json_1000": {"n0=log(skip=false,logger=logrus,type=json,length=1000)"},
	"log_skip_logrus_json_1000":  {"n0=log(skip=true,logger=logrus,type=json,length=1000)"},
	"log_print_logrus_text_1000": {"n0=log(skip=false,logger=logrus,type=text,length=1000)"},
	"log_skip_logrus_text_1000":  {"n0=log(skip=true,logger=logrus,type=text,length=1000)"},
	"parse_2_10":                 {"n0=parse(depth=2,width=10)"},
	"database-parse-allocate":    {"n0=db(engine=postgres,1)", "n0=forward", "n1=parse(depth=2,width=10)", "n1=forward", "n2=allocate(size=10000B)"},
}

type logJSON struct {
	Name       string    `json:"name"`
	Timestamps []int64   `json:"timestamps"`
	Marshalled []logJSON `json:"marshalled"`
}

type logRecord struct {
	Nodes       logJSON
	clientStart int64
	clientEnd   int64
}

func (log logRecord) String() string {
	return fmt.Sprintf("Start: %d;\n%v\nEnd:%d\n", log.clientStart, log.Nodes, log.clientEnd)
}

type logsStorage struct {
	Logs    []logRecord
	counted []timestampDelta
	uri     string
}

func (logs logsStorage) ClientDelta() timestampDelta {
	res := make(timestampDelta, len(logs.Logs))
	for i, log := range logs.Logs {
		res[i] = log.clientEnd - log.clientStart
	}

	return res
}

func (logs logsStorage) ServerDelta() timestampDelta {
	res := make(timestampDelta, len(logs.Logs))
	for i, log := range logs.Logs {
		front := log.Nodes.Timestamps

		if len(front) == 0 {
			return res
		}

		res[i] = front[len(front)-1] - front[0]
	}

	return res
}

// logsDFS is a function for depth-first search of logs
func logsDFS(logs logsStorage) []timestampDelta {
	res := make([]timestampDelta, 0)
	res = append(res, make(timestampDelta, 0))

	for _, log := range logs.Logs {
		res = dfs(res, 0, log.Nodes)
	}

	return res
}

func dfs(res []timestampDelta, level int, curLog logJSON) []timestampDelta {
	for i := 1; i < len(curLog.Timestamps); i += 2 {
		res[level] = append(res[level], curLog.Timestamps[i+1]-curLog.Timestamps[i])
	}
	for i := 0; i < len(curLog.Marshalled); i++ {
		if len(res) == level+1 {
			res = append(res, make(timestampDelta, 0))
		}
		res = dfs(res, level+1, curLog.Marshalled[i])
	}

	return res
}

func (logs *logsStorage) ActionDelta(num int) (res timestampDelta) {
	if logs.counted == nil {
		logs.counted = logsDFS(*logs)
	}

	return logs.counted[num]
}

func (logs *logsStorage) GetTimestampDeltaLength() int {
	if logs.counted == nil {
		logs.counted = logsDFS(*logs)
	}

	return len(logs.counted)
}

type timestampDelta []int64

func (l timestampDelta) Sort() {
	sort.Slice(l, func(i int, j int) bool { return l[i] < l[j] })
}

func (l timestampDelta) Max() int64 {
	if len(l) == 0 {
		log.Println("WARNING: got no timestamps, returned Max = 0 ")

		return 0
	}

	return l[len(l)-1]
}

func (l timestampDelta) Min() int64 {
	if len(l) == 0 {
		log.Println("WARNING: got no timestamps, returned Min = 0 ")

		return 0
	}

	return l[0]
}

func (l timestampDelta) Median() int64 {
	if len(l) == 0 {
		log.Println("WARNING: got no timestamps, returned Median = 0 ")

		return 0
	}

	if len(l)%2 == 0 {
		return (l[len(l)/2] + l[len(l)/2+1]) / 2
	}

	return l[len(l)/2]
}

func (l timestampDelta) Average() float64 {
	if len(l) == 0 {
		log.Println("WARNING: got no timestamps, returned Average = 0 ")

		return 0
	}

	var sum int64
	for _, val := range l {
		sum += val
	}

	return float64(sum) / float64(len(l))
}

func (l timestampDelta) AllMetricsString(name string, actionNumber, totalActions int) string {
	var delta timestampDelta
	if actionNumber < 0 || totalActions < 1 {
		delta = l
	} else {
		for i := actionNumber; i < len(l); i += totalActions {
			delta = append(delta, l[i])
		}
	}

	delta.Sort()

	return fmt.Sprintf("%s:%d. Max: %.9f. Min: %.9f. Median: %.9f. Average: %.9f",
		name,
		actionNumber,
		float64(delta.Max())/(float64(time.Second.Nanoseconds())),
		float64(delta.Min())/(float64(time.Second.Nanoseconds())),
		float64(delta.Median())/(float64(time.Second.Nanoseconds())),
		delta.Average()/(float64(time.Second.Nanoseconds())),
	)
}

func getNumberOfActionsForNode(uri string, node int) (res int) {
	url, err := url.ParseRequestURI(uri)
	if err != nil {
		panic(err)
	}

	nodeName := fmt.Sprintf("n%d", node)
	query := url.Query()[nodeName]

	return len(query)
}

func aggregate(logs logsStorage) {
	ts := logs.ClientDelta()
	fmt.Println(ts.AllMetricsString("Client delta", 0, 0))
	ts = logs.ServerDelta()
	fmt.Println(ts.AllMetricsString("Server delta", 0, 0))

	for i := 0; i < logs.GetTimestampDeltaLength(); i++ {
		ts = logs.ActionDelta(i)
		totalActions := getNumberOfActionsForNode(logs.uri, i)
		for j := 0; j < totalActions; j++ {
			fmt.Println(ts.AllMetricsString(fmt.Sprintf("Node %d", i+1), j, totalActions))
		}
	}
}

func printListAliases() {
	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 0, 8, 2, ' ', tabwriter.Debug)

	fmt.Println("restrelay-bench client prebuilt urls")
	for key, value := range listAliases {
		fmt.Fprintf(w, "%s:\t%s\n", key, value)
	}
	if err := w.Flush(); err != nil {
		fmt.Printf("failed to print list of scenarios: %v\n", err)
	}
}

func validateArgs(args Options) {
	if args.ListScenarios {
		printListAliases()
		os.Exit(0)
	}

	if args.Host == "" {
		fmt.Println("Host should be not empty")
		os.Exit(-1)
	}
	if args.Port < 0 {
		fmt.Println("Port should be positive number")
		os.Exit(-1)
	}
}

func readAndUnmarshalBody(resp *http.Response) (res logRecord) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Unable read response body,", err.Error())
		os.Exit(-1)
	}

	if resp.StatusCode != 200 {
		fmt.Printf("Error on server with %s.\n%s\n", resp.Status, string(body))
		os.Exit(-1)
	}

	var r logJSON

	err = json.Unmarshal(body, &r)
	if err != nil {
		return
	}

	res.Nodes = r

	return
}

func readLogChannel(l []logRecord, uri string) {
	logs := logsStorage{
		Logs: l,
		uri:  uri,
	}

	aggregate(logs)
}

type benchClient struct {
	client *http.Client
}

func newBenchClient() *benchClient {
	client := &benchClient{}
	client.client = &http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: 10000000,
		},
	}

	return client
}

func main() {
	var b = benchmark.NewBenchmark()

	b.AddOpts = func() benchmark.TestOpts {
		var opts = Options{}
		b.Cli.AddFlagGroup(
			"restrelay-bench",
			"This is a client for restrelay-bench server application. User should pick prebuilt urls. Result is timestamps averages in stdout.",
			&opts,
		)

		return &opts
	}

	b.Init = func() {
		opts := b.TestOpts.(*Options)
		validateArgs(*opts)

		var err error
		expirationDuration, err = time.ParseDuration("0s") //time.ParseDuration(opts.ExpirationConnection)
		if err != nil {
			fmt.Printf("Failed to parse --keep-alive-expiration: %s\n", err.Error())
			os.Exit(-1)
		}
		if expirationDuration < 0 {
			fmt.Println("Expiration time should be positive")
			os.Exit(-1)
		}

		var url string
		if opts.CustomScenario != "" {
			url = opts.CustomScenario
		} else {
			url = strings.Join(listAliases[opts.Scenario], "&")
			if url != "" {
				url = "&" + url
			}
		}

		url = buildBaseURL(opts.Host, opts.Port, opts.SSLEnabled, opts.DisableKeepAliveServer, url)

		vault := store{
			url:  url,
			logs: logsStore{},
		}
		b.Vault = &vault
		b.Workers = []*benchmark.BenchmarkWorker{}
		for i := 0; i < b.CommonOpts.Workers; i++ {
			b.Workers = append(b.Workers, benchmark.NewBenchmarkWorker(b, i))
		}
	}

	b.WorkerInitFunc = func(worker *benchmark.BenchmarkWorker) {
		worker.Data = newBenchClient()
	}

	b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
		dataStore := b.Vault.(*store)
		client := worker.Data.(*benchClient)
		disableKeepAlive := b.TestOpts.(*Options).DisableKeepAliveClient

		req, err := http.NewRequest("GET", dataStore.url, nil)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}

		if disableKeepAlive {
			req.Header.Add("Connection", "close")
		} else {
			if expirationDuration != 0 {
				/*
					How can we achieve conn recreation after fixed time?
					Attach metadata about connection creation to net.Conn interface wrapper?
					if time.Now().Before(client.CreatedAt.Add(expirationDuration)) {
						req.Header.Add("Connection", "keep-alive")
					} else {
						req.Header.Add("Connection", "close")
					}
				*/
				req.Header.Add("Connection", "keep-alive")
			} else {
				req.Header.Add("Connection", "keep-alive")
			}
		}

		startTS := time.Now().UnixNano()

		resp, err := client.client.Do(req)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}

		log := readAndUnmarshalBody(resp)

		err = resp.Body.Close()
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}

		endTS := time.Now().UnixNano()
		log.clientStart = startTS
		log.clientEnd = endTS
		dataStore.logs.Append(log)

		return 1
	}

	b.Finish = func() {
		dataStore := b.Vault.(*store)

		readLogChannel(dataStore.logs.GetLogs(), dataStore.url)
	}

	b.Run()
}
