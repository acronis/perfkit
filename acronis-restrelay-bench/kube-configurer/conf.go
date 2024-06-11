// Package kube-configurer implements a configuration generator for Kubernetes.
package main

import (
	"fmt"
	"os"
	"text/template"

	"github.com/jessevdk/go-flags"
)

type kubernetesServiceArgs struct {
	ServiceName      string
	ServiceApp       string
	Replicas         int
	OptionalNodePort string
	OutputPort       string
	DeploymentName   string
	ContainerName    string
	ImageName        string
	NextService      string
	NumberInChain    int
	Port             int
}

type topology []kubernetesServiceArgs

// Opts is a struct for command line arguments
type Opts struct {
	Registry            string `short:"r" long:"registry" description:"sets docker registry url" default:"localhost:5000"`
	Nodes               int    `short:"n" long:"nodes" description:"sets number of nodes in topology" default:"3"`
	Output              string `short:"o" long:"output" description:"sets configuration output file in yml format" default:"result"`
	DeleteCommandOutput string `short:"d" long:"delete-output" description:"sets output for deleting topology in bash(or fish). Writes to stdout if empty"`
	DBConfig            string `long:"db-config" description:"sets path to k8s config for database" default:"db.yml"`
	NodeConfig          string `long:"node-config" description:"sets path to k8s config for each node" default:"node.yml"`
	// Replicas            int    `short:"rep" long:"rep" description:"sets number of replicas in each tier" default:"2"`
}

func finishWithError(message string) {
	fmt.Println(message)
	os.Exit(-1)
}

func generateService(currentNumber, nextNumber int, registry string, replicas int) kubernetesServiceArgs {
	var nextServiceURL string
	var optNodePort string
	var outputPort string

	if currentNumber == 0 {
		optNodePort = "type: NodePort"
		outputPort = "nodePort: 31432"
	} else {
		optNodePort = ""
		outputPort = "targetPort: 8080"
	}

	nextServiceURL = fmt.Sprintf("http://restrelay-bench-%d:8080/", nextNumber)

	return kubernetesServiceArgs{
		ServiceName:      fmt.Sprintf("restrelay-bench-%d", currentNumber),
		ServiceApp:       fmt.Sprintf("restrelay-bench-%d-app", currentNumber),
		Replicas:         replicas,
		OptionalNodePort: optNodePort,
		OutputPort:       outputPort,
		DeploymentName:   fmt.Sprintf("restrelay-bench-%d-deployment", currentNumber),
		ContainerName:    fmt.Sprintf("restrelay-bench-%d", currentNumber),
		ImageName:        fmt.Sprintf("%s/restrelay-bench:1.0", registry), //nolint:perfsprint
		NextService:      nextServiceURL,
		NumberInChain:    currentNumber,
		Port:             8080,
	}
}

func generate(number int, registry string, replicas int) (res topology) {
	res = make(topology, number)

	for i := 0; i < number-1; i++ {
		res[i] = generateService(i, i+1, registry, replicas)
	}
	res[number-1] = generateService(number-1, -1, registry, replicas)

	return
}

func parseCLI() (res Opts) {
	cliParser := flags.NewNamedParser(os.Args[0], flags.Default)
	cliParser.AddGroup("", "", &res) //nolint:errcheck
	_, err := cliParser.Parse()
	if err != nil {
		e := err.(*flags.Error)
		if e.Type == flags.ErrHelp {
			os.Exit(0)
		}
		fmt.Println(e.Message)
		os.Exit(0)
	}

	return res
}

func writeAdditionalNodes(output *os.File, pathDB string) {
	var input, err = os.ReadFile(pathDB)
	if err != nil {
		finishWithError(err.Error())
	}

	if _, err = output.WriteString("\n---\n"); err != nil {
		finishWithError(err.Error())
	}

	if _, err = output.Write(input); err != nil {
		finishWithError(err.Error())
	}
}

func writeTopologyToFile(file *os.File, template *template.Template, topology []kubernetesServiceArgs, pathDB string) {
	for i := 0; i < len(topology)-1; i++ {
		writeTemplateToFile(file, template, topology[i])
		_, err := file.WriteString("\n---\n")
		if err != nil {
			finishWithError(err.Error())
		}
	}
	writeTemplateToFile(file, template, topology[len(topology)-1])
	writeAdditionalNodes(file, pathDB)
}

func createTemplate(path string) *template.Template {
	templ := template.New("node.yml")
	templ, err := templ.ParseFiles(path)
	if err != nil {
		finishWithError(err.Error())
	}

	return templ
}

func openResultFile(name string) *os.File {
	if name == "" {
		finishWithError("Wrong filename")
	}

	file, err := os.Create(name + ".yml")
	if err != nil {
		finishWithError(err.Error())
	}

	return file
}

func writeTemplateToFile(file *os.File, template *template.Template, data interface{}) {
	err := template.Execute(file, data)
	if err != nil {
		finishWithError(err.Error())
	}
}

func writeDeleteCommand(filename, command string) {
	if filename == "" {
		fmt.Println(command)

		return
	}

	var file, err = os.Create(filename)
	if err != nil {
		finishWithError(err.Error())
	}

	if _, err = file.WriteString(command); err != nil {
		finishWithError(err.Error())
	}

	if err = file.Close(); err != nil {
		finishWithError(err.Error())
	}
}

func (topology topology) DeleteCommand(useFish bool) (res string) {
	for _, node := range topology {
		if useFish {
			res += fmt.Sprintf("kubectl delete deployment %s; and kubectl delete service %s; and ", node.DeploymentName, node.ServiceName)
		} else {
			res += fmt.Sprintf("kubectl delete deployment %s; kubectl delete service %s;", node.DeploymentName, node.ServiceName)
		}
	}

	if useFish {
		res += "kubectl delete deployment postgresql-deployment; and kubectl delete service postgresql-service"
	} else {
		res += "kubectl delete deployment postgresql-deployment; kubectl delete service postgresql-service"
	}

	return
}

func createTopologyFromCLIArgs(args Opts, template *template.Template, file *os.File) {
	var top = generate(args.Nodes, args.Registry, 2)
	writeDeleteCommand(args.DeleteCommandOutput, top.DeleteCommand(false))
	writeTopologyToFile(file, template, top, args.DBConfig)
}

func main() {
	var args = parseCLI()
	var templ = createTemplate(args.NodeConfig)

	var res = openResultFile(args.Output)
	createTopologyFromCLIArgs(args, templ, res)

	if err := res.Close(); err != nil {
		finishWithError(err.Error())
	}
}
