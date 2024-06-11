# Acronis REST-Relay Benchmark

## Project Introduction
The Acronis REST-Relay Benchmark is a comprehensive performance testing tool designed to evaluate the performance of infrastructure with different workloads running on it. The benchmark consists of two main executable components: the REST HTTP client and the REST API server. The server can be deployed in multiple instances, with each instance assigned a unique sequence number.

Benchmark is intended for performance researchers to deploy and assess server configurations in their test environments. It allows for detailed analysis of server behavior under load, helping identify potential bottlenecks and performance issues in various deployment scenarios.

Typical use-cases:
1. Compare benchmark results measured on different environments (e.g. server deployment on bare metal host vs depoyment in VMs vs k8s docker pods) and tune appropriate environment configuration.
2. Compare results on different hardware nodes or VMs to select optimal configuration for your production environment.
3. Analyse how different environment configuration settings impact different benchmark actions: chatty REST API communication, memory allocation, CPU intensive calculation.
4. Tune VMs or k8s pods CPU and memory limits and see how they impact benchmark results.

## Benchmark Algorithm

1. **Client Request Generation**: The client generates HTTP requests to server instance #0 based on user-defined parameters provided via the command line.
2. **Parameter Customization**: Parameters include server URL/IP, test duration, number of parallel workers, number of requests, and a critical "server actions" list.
3. **Server Actions Execution**: Each HTTP request contains parameters that dictate the actions the server should perform, such as:
   1) Sleeping for N milliseconds 
   2) Allocating N KBs of memory 
   3) Running CPU-intensive workloads (busyloop)
   4) Generating or parsing JSON payloads 
   5) Forwarding requests with some additional generated data to another server with an incremented sequence number
4. **Action Processing**: The server parses the incoming request, determines the actions, executes them, and either returns a 200 OK status to the caller or forwards the request to the next server in the sequence.
5. **Client Reporting**: The client continues generating HTTP requests according to the defined parameters and compiles performance metrics, reporting the overall HTTP API rate.

## Key Features

* **Flexible Deployment**: Supports deployment as processes on a single machine, across multiple machines, or as Docker pods pinned to specific hosts in a Kubernetes cluster.
* **Configurable Client Parameters**: Allows customization of server URL/IP, test duration, number of parallel workers, number of requests, and detailed "server actions."
* **Sequential Server Actions**: Each server instance can perform a series of predefined actions or forward requests to subsequent servers in a sequence.
* **Comprehensive Performance Metrics**: The client reports HTTP API rates and other performance metrics, providing insights into the system's behavior under load.

## Project structure

1. client          -- benchmark test client generating load on server
2. server          -- server-side HTTP-server processing requests and simulating load on infrastructure 
3. configs         -- docker+kubernetes configuration files
4. kube-configurer -- CLI utility for generating kubernetes configuration files

## Usage Examples

### Prerequisites
* **Golang 1.21** compiler (for compiling the executables)
* **Docker** (for containerized deployments)
* **Kubernetes** (for cluster-based deployments)

Clone the repository:

```sh
git clone https://github.com/acronis/perfkit.git
cd acronis-restrelay-bench
```

### Local Run

1.  Build the client and server executables:
    ```sh
    make build-restrelay-bench
    ```
2. Start Server Instances: Deploy the desired number of server instances according to your test environment configuration.
    ```sh
    ./restrelay-bench-server --port 8080 --number-in-chain 0 --next-service http://localhost:8081 &
    ./restrelay-bench-server --port 8081 --number-in-chain 1 &
    # Repeat for additional server instances
    ```
3. Run the Client: Execute the client with the desired parameters.
    ```sh
    ./restrelay-bench-client --host=localhost:8080 --duration=5 --custom-scenario="&n0=busyloop(duration=1s)&n0=forward&n1=busyloop(duration=1s)" --concurrency=1
    ```
   
    Expected output:
    ```
    Client delta:0. Max: 1.909856000. Min: 1.849110000. Median: 1.850932000. Average: 1.869966000
    Server delta:0. Max: 1.890732000. Min: 1.848368000. Median: 1.849951000. Average: 1.863017000
    Node 1:0. Max: 0.923217000. Min: 0.903773000. Median: 0.903962000. Average: 0.910317333
    Node 1:1. Max: 0.967321000. Min: 0.944481000. Median: 0.945905000. Average: 0.952569000
    Node 2:0. Max: 0.944730000. Min: 0.943233000. Median: 0.943984000. Average: 0.943982333
    time: 5.610003 sec; threads: 1; loops: 3; rate: 0.53 loops/sec;
    ```

### Run in Kubernetes

Configure the Kubernetes cluster and deploy the server instances using the provided configuration files.
Use MiniKube or a remote Kubernetes cluster.

#### MiniKube Configuration
1. Install minkube and kubectl 
2. Start MiniKube 
    ```sh
    minikube start
    ```
3. Install ingress controller addon:
    ```sh
    minikube addons enable ingress
    ```
4. Connect minikube to docker to use local images:
    ```sh
    eval $(minikube docker-env)             # Unix shells
    minikube docker-env | Invoke-Expression # PowerShell
    ```

#### Kubernetes Cluster Configuration
1. Setup Kubernetes cluster
2. Install ingress controller on k8s cluster https://github.com/kubernetes/ingress-nginx
3. Setup docker registry
4. Set registry address for k8s cluster:
    ```sh
    export RESTRELAY_REGISTRY=<your registry>
    ```
5. Configure deployment topology by specifying the registry:
    ```sh
    sed -i.bak "s|image: restrelay-bench-server:1.0|image: $RESTRELAY_REGISTRY/restrelay-bench-server:1.0|g" "configs/topology.yml"
    ```
6. Configure ingress class name in the topology file:
    ```sh
   # ingressClassName can be nginx-fw or some other ingress class depending on your installation
    sed -i.bak "s|ingressClassName: nginx|ingressClassName: <your ingress class>|g" "configs/topology.yml"
    ```
7. Connect to kube cluster to use kubectl:
    ```sh
    export KUBECONFIG=~/.kube/<cluster config>
    ```

#### Deployment in Kubernetes

1. Setup k8s cluster, configure kubectl and set docker registry for k8s cluster
2. Build the server executable for target architecture:
    ```sh
    cd server
    env CGO_ENABLED=0 GOARCH=amd64 GOOS=linux go build -o ./../restrelay-bench-server
    cd ..
    ```
3. Build docker image for target architecture:
    ```sh
    docker build --platform="linux/amd64" -f ./configs/server.dockerfile -t restrelay-bench-server:1.0 .
    ```
4. Optional (Skip for Minikube installation). If docker registry is remote, push the image to the registry:
    ```sh
    docker tag restrelay-bench-server:1.0 ${RESTRELAY_REGISTRY}/restrelay-bench-server:1.0
    docker push ${RESTRELAY_REGISTRY}/restrelay-bench-server:1.0
    ```
5. Deploy the server instances in the Kubernetes cluster:
    ```sh
    kubectl apply -f configs/topology.yml
    ```
6. Optional (For Minikube installation). Start the tunnel to expose the service:
    ```sh
    minikube tunnel
    ```
7. Build the client executable:
    ```sh
    make build-restrelay-bench
    # or
    cd client
    go build -o ./../restrelay-bench-client
    cd ..
    ```
8. Run the client with the desired parameters:
    ```sh
    # For Minikube
    ./restrelay-bench-client --host=127.0.0.1 --port=80 --duration=5 --custom-scenario="&n0=busyloop(duration=1s)" --concurrency=1
   
    # For remote Kubernetes cluster
    ./restrelay-bench-client --host=<your DC fqdn> --ssl-enabled  --duration=5 --custom-scenario="&n0=busyloop(duration=1s)" --concurrency=1
    ```
   Expected output:
    ```
    Client delta:0. Max: 0.727529000. Min: 0.494093000. Median: 0.519060500. Average: 0.544034100
    Server delta:0. Max: 0.580143144. Min: 0.433845462. Median: 0.482523251. Average: 0.474031042
    Node 1:0. Max: 0.580069734. Min: 0.433786211. Median: 0.482424803. Average: 0.473954292
    time: 5.440901 sec; threads: 1; loops: 10; rate: 1.84 loops/sec;
    ```


### Detailed Configuration

#### Server Request Parameters

Parameters should be in this format: n`number_in_chain`=`action`(`parametes_separated_by_comma`)

Examples:
```  
    /?connection=keep-alive&
        n0=sleep(duration=1s)&
        n0=forward&  
        n1=allocate(size=1KB) 
        
    /?connection=keep-alive&
        n0=forward&
        n1=sleep(duration=111ms)&
        n2=allocate(size=1KB)&
        n3=parse(depth=4,width=10)&
        n4=db(engine=postgres,query=1)
```

Full list of Server-side Actions

* sleep -- call os.Sleep(). Parameters:
    * duration in Go time.Duration, "1m", "25s", etc
* busyloop -- run empty loop. Parameters:
    * duration in Go time.Duration, "1m", "25s", etc
* allocate -- allocates array of structs. Parameters:
    * size in bytes in following format: "[int]B",  "[int]KB", "[int]MB", "[int]GB"
* db -- performs request to database. Parameters:
    * engine - mysql or postgres,
    * query - number of query 1(SELECT 1)
* serial -- serializes logJSON(recursive tree). Generates go's objects(if not cached) and serializes it. Parameters:
    * depth - tree-height, have to be > 0, [int];
    * width - tree-width, have to be > 0, [int];
* parse -- same as serial, but one more step: deserializes serialized string(generates new if not cached). Parameters:
    * depth - tree-height, have to be > 0, [int];
    * width - tree-width, have to be > 0, [int];
* forward -- forward to next node in chain. No parameters.
* forwardjson -- forward to next node in chain with json. Generates go's objects and serializes it (if not cached) and forward to the next node. Parameters:
    * depth - tree-height, have to be > 0, [int];
    * width - tree-width, have to be > 0, [int];
* forwarddata -- forward to next node in chain with data using cached jsons. Parameters:
    * size in bytes in following format: "[int]B",  "[int]KB", "[int]MB", "[int]GB"
* log -- log message. Parameters:
    * skip - debug level enabled, not mandatory, [bool];
    * logger - logger name, one of 'logf' or 'logrus' [string];
    * type - log type, one of 'text' or 'json' [string];
    * length - log message length, [int];

    
#### Server

      -l, --local               run server as binary in local machine
          --host=               sets host URL for database connection (default: localhost)
      -u, --user=               sets user for database connection (default: test)
      -p, --password=           sets password for database connection (default: test)
          --go-services-logger  enable go-services logger
          --server=             set http-server(standard|fast|go-service|gin) (default: standard)
          --metrics             turn on prometheus metrics

#### Client

      -s, --scenario=     valid url case for request (default: sleep-sleep-sleep)
          --host=         host of restrelay-bench entrypoint (default: localhost)
      -p, --port=         port of restrelay-bench entrypoint (default: 8080)
          --log=          1 -- no logs, 2 -- logJSON logs, 3 -- plain text logs (default: 2)
          --keep-alive    enables keep-alive header
          --list-aliases  prints all valid cases in program

      -v, --verbose       Show verbose debug information
      -c, --concurrency=  sets number of goroutines that runs testing function (default: 0)
      -l, --loops=        sets TOTAL(not per worker) number of iterations of testing function(greater priority than DurationSec) (default: 0)
      -d, --duration=     sets duration(in seconds) for worktime for every loop (default: 5)

#### Kube-Configurer

    -n, --nodes=         sets number of nodes in topology (default: 3)
    -o, --output=        sets configuration output file in yml format (default: result)
    -d, --delete-output= sets output for deleting topology in bash(or fish). Writes to stdout if empty
    -f, --fish           generate delete command output for fish shell


## Contributing

Contributions are welcome! Please fork the repository and submit pull requests for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.