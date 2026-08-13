package rpc

import (
	"fmt"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func ServerTransportCredentials(certFile, keyFile string) (credentials.TransportCredentials, error) {
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("gRPC TLS certificate and key must be configured together")
	}
	transport, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load gRPC server TLS: %w", err)
	}
	return transport, nil
}

func ClientTransportCredentials(caFile, serverName string) (credentials.TransportCredentials, error) {
	if caFile == "" {
		if serverName != "" {
			return nil, fmt.Errorf("gRPC TLS server name requires a CA file")
		}
		return insecure.NewCredentials(), nil
	}
	transport, err := credentials.NewClientTLSFromFile(caFile, serverName)
	if err != nil {
		return nil, fmt.Errorf("load gRPC client TLS: %w", err)
	}
	return transport, nil
}
