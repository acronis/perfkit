# Acronis perfkit

The **perfkit** is a comprehensive Go library and a suite of ready-to-use performance testing tools, designed to streamline development, benchmarking and analysis of various systems, including databases and cloud-based infrastructure.

It includes:
* [benchmark](benchmark) - a library for writing benchmarks
* [db](db) - a library for interacting with databases, offering an abstraction layer to facilitate database operations
* [acronis-db-bench](acronis-db-bench) - A tool for benchmarking SQL and NoSQL database performance
* [acronis-restrelay-bench](acronis-restrelay-bench) - A tool for benchmarking cloud deployments and REST API workloads

# Ready-to-use benchmarks

## Acronis Database Benchmark (acronis-db-bench)

The Acronis Database Benchmark is a comprehensive framework for testing and analyzing the performance of various database systems. It supports a wide range of both SQL and NoSQL databases, such as MySQL, PostgreSQL, Cassandra, ClickHouse, ElasticSearch, OpenSearch and more, making it a powerful tool for cross-database performance comparisons. The tool provides extensive support for different data access patterns, including inserts, updates, facet searches, indexed searches, vector searches, and others, allowing for a detailed exploration of how each database handles different query types and workloads.

With built-in features for simulating multi-tenant environments, the benchmark can model real-world SaaS-like scenarios where multiple tenants access shared resources. Additionally, it offers advanced logging and diagnostic features that help developers and database administrators quickly identify and troubleshoot performance issues. The tool is ideal for assessing the impact of various database configurations, comparing the overhead of database clusters versus single instances, and analyzing performance bottlenecks under concurrent workloads. It also enables users to evaluate how databases perform under high-cardinality data, different indexing strategies, and various levels of hardware and virtualization environments. This makes it highly suitable for optimizing database systems and ensuring efficient performance in production-like environments.

## Acronis REST-relay Benchmark (acronis-restrelay-bench)

The Acronis REST-Relay Benchmark is a powerful tool designed for evaluating server infrastructure performance under various workloads. It consists of two main components: a REST HTTP client and a REST API server, which can be deployed in multiple instances to simulate different environments. The tool is ideal for performance researchers who want to analyze server behavior under load, compare hardware or virtualized environments, and fine-tune configurations. It allows detailed customization of client parameters and server actions such as memory allocation, CPU-intensive tasks, and request forwarding, offering comprehensive insights into system performance and bottlenecks in complex deployment scenarios.

The benchmark is particularly useful for comparing performance across bare-metal, VM, and Kubernetes environments, enabling users to identify the optimal configuration for production environments. Additionally, it supports tuning CPU and memory limits in VMs or Kubernetes pods to assess their impact on performance. With extensive logging and detailed metrics reporting, it provides valuable feedback for troubleshooting and optimizing infrastructure settings for REST API-based applications. Whether you're testing different hardware setups, simulating complex REST API workloads, or investigating concurrency issues, the Acronis REST-Relay Benchmark offers the flexibility needed for rigorous performance testing and analysis.

# Developing your own benchmarks

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have met the following requirements:
* You have installed the latest version of [Go](https://golang.org/dl/).

### Getting perfkit

To get perfkit, follow these steps:

1. Clone the repository:
```bash
git clone https://github.com/acronis/perfkit.git
```

2. Navigate to the project directory:

`cd perfkit`

### Using benchmark library

`go get github.com/acronis/perfkit/benchmark`

```go
package main

import (
    "github.com/acronis/perfkit/benchmark"
)

func main() {
	b := benchmark.New()
	// do something
}
```

### Installing

```bash
make install
acronis-db-bench --help
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE.txt](LICENSE.txt) file for details.