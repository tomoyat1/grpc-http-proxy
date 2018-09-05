package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/mercari/grpc-http-proxy/config"
	"github.com/mercari/grpc-http-proxy/http"
	"github.com/mercari/grpc-http-proxy/log"
	"github.com/mercari/grpc-http-proxy/source"
)

var mappingFile = flag.String("mapping-file", "", "mapping file for grpc service names "+
	"to server host names. Kubernetes API will be used for service discovery if this is unspecified")

func main() {
	flag.Parse()
	env, err := config.ReadFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to read environment variables: %s\n", err.Error())
		os.Exit(1)
	}
	logger, err := log.NewLogger(env.LogLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to create logger: %s\n", err)
		os.Exit(1)
	}

	var s *http.Server
	if *mappingFile == "" {
		k8sConfig, err := rest.InClusterConfig()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to create k8s config: %s\n", err)
			os.Exit(1)
		}
		k8sClient, err := kubernetes.NewForConfig(k8sConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to create k8s client: %s\n", err)
			os.Exit(1)
		}
		d := source.NewService(k8sClient, "", logger)
		stopCh := make(chan struct{})
		d.Run(stopCh)
		s = http.New(env.Token, d, logger)
	} else {
		d := source.NewStatic(logger, *mappingFile)
		s = http.New(env.Token, d, logger)
	}
	logger.Info("starting grpc-http-proxy",
		zap.String("log_level", env.LogLevel),
		zap.Int16("port", env.Port),
	)

	addr := fmt.Sprintf(":%d", env.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to listen HTTP port %s\n", err)
		os.Exit(1)
	}
	s.Serve(ln)
}
