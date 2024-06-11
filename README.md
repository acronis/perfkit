# perfkit

perfkit is a set of performance tools.

It includes:
* [benchmark](benchmark) - a library for writing benchmarks
* [acronis-db-bench](acronis-db-bench) - a tool to run database benchmarks
* [acronis-restrelay-bench](acronis-restrelay-bench) - a tool to run end-to-end cloud deployment benchmarks

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have met the following requirements:
* You have installed the latest version of [Go](https://golang.org/dl/).
* You have a `<Mac/Linux/Windows>` machine. State which OS is supported/required.

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