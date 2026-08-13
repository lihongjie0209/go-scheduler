package rpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerTransportCredentialsRequiresPair(t *testing.T) {
	t.Parallel()
	for _, pair := range [][2]string{{"cert.pem", ""}, {"", "key.pem"}} {
		if _, err := ServerTransportCredentials(pair[0], pair[1]); err == nil {
			t.Fatalf("accepted incomplete TLS pair: %q %q", pair[0], pair[1])
		}
	}
	if transport, err := ServerTransportCredentials("", ""); err != nil || transport != nil {
		t.Fatalf("plaintext server credentials = %v, %v", transport, err)
	}
}

func TestClientTransportCredentialsValidatesConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := ClientTransportCredentials("", "executor.internal"); err == nil {
		t.Fatal("accepted server name without CA")
	}
	if transport, err := ClientTransportCredentials("", ""); err != nil || transport == nil {
		t.Fatalf("plaintext client credentials = %v, %v", transport, err)
	}
}

func TestTransportCredentialsSecureGRPCConnection(t *testing.T) {
	t.Parallel()
	certFile, keyFile := writeTestCertificate(t)
	serverTransport, err := ServerTransportCredentials(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(serverTransport))
	healthv1.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	clientTransport, err := ClientTransportCredentials(certFile, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientTransport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if _, err = healthv1.NewHealthClient(connection).Check(ctx, &healthv1.HealthCheckRequest{}); err != nil {
		t.Fatalf("TLS health check: %v", err)
	}
}

func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certFile, keyFile := filepath.Join(directory, "tls.crt"), filepath.Join(directory, "tls.key")
	if err = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
