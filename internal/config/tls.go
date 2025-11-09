package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"net"
)

// Postgres TLS
func (c *Config) CreatePostgresTLSConfig() *tls.Config {
	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM([]byte(c.DBCACert)); !ok {
		log.Fatal("failed to parse Postgres CA certificate")
	}
	return &tls.Config{
		RootCAs:    rootCertPool,
		ServerName: c.DBHost,
	}
}

// Kafka TLS
func (c *Config) CreateKafkaTLSConfig() *tls.Config {
	// Load CA certificate
	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM([]byte(c.KafkaCACert)); !ok {
		log.Fatal("failed to parse Kafka CA certificate")
	}

	// Extract host without port for TLS ServerName
	var serverName string
	if len(c.KafkaBrokers) > 0 {
		host, _, err := net.SplitHostPort(c.KafkaBrokers[0])
		if err != nil {
			// If SplitHostPort fails, it might be because there's no port
			// Try to use the entire string as host
			serverName = c.KafkaBrokers[0]
		} else {
			serverName = host
		}
	}

	fmt.Printf("Kafka ServerName: '%s'\n", serverName)
	return &tls.Config{
		RootCAs:    rootCertPool,
		ServerName: serverName, // must match SAN in certificate
		MinVersion: tls.VersionTLS12,
	}
}
