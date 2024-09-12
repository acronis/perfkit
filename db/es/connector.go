package es

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"

	es8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/acronis/perfkit/db"
)

const (
	magicEsEnvVar = "ELASTICSEARCH_URL"
)

// nolint: gochecknoinits // remove init() when we will have a better way to register connectors
func init() {
	for _, esNameStyle := range []string{"es", "elastic", "elasticsearch"} {
		if err := db.Register(esNameStyle, &esConnector{}); err != nil {
			panic(err)
		}
	}
}

// nolint:gocritic //TODO refactor unnamed returns
func elasticCredentialsAndConnString(cs string, tlsEnabled bool) (string, string, string, error) {
	var u, err = url.Parse(cs)
	if err != nil {
		return "", "", "", fmt.Errorf("cannot parse connection url %v, err: %v", cs, err)
	}

	var username = u.User.Username()
	var password, _ = u.User.Password()

	var scheme string
	if tlsEnabled {
		scheme = "https"
	} else {
		scheme = "http"
	}

	var finalURL = url.URL{
		Scheme: scheme,
		Host:   u.Host,
	}
	cs = finalURL.String()

	return username, password, cs, nil
}

type esConnector struct{}

func (c *esConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
	var adds []string
	var username, password, cs string
	var err error

	if s := os.Getenv(magicEsEnvVar); s == "" {
		username, password, cs, err = elasticCredentialsAndConnString(cfg.ConnString, cfg.TLSEnabled)
		if err != nil {
			return nil, fmt.Errorf("db: elastic: %v", err)
		}

		adds = append(adds, cs)
	}

	var tlsConfig tls.Config
	if len(cfg.TLSCACert) == 0 {
		tlsConfig = tls.Config{
			InsecureSkipVerify: true, // nolint:gosec // TODO: InsecureSkipVerify is true
		}
	} else {
		var caCertPool = x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cfg.TLSCACert)

		// nolint:gosec // TODO: TLS MinVersion too low
		tlsConfig = tls.Config{
			RootCAs: caCertPool,
		}
	}

	var conf = es8.Config{
		Addresses: adds,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   cfg.MaxOpenConns,
			ResponseHeaderTimeout: cfg.MaxConnLifetime,
			DialContext:           (&net.Dialer{Timeout: cfg.MaxConnLifetime}).DialContext,
			TLSClientConfig:       &tlsConfig,
		},
		Username: username,
		Password: password,
	}

	var es *es8.Client
	if es, err = es8.NewClient(conf); err != nil {
		return nil, fmt.Errorf("db: cannot connect to es db at %v, err: %v", cs, err)
	}

	var ping *esapi.Response
	if ping, err = es.Ping(); err != nil {
		if err.Error() == "EOF" {
			return nil, fmt.Errorf("db: failed ping es db at %s, TLS CA required", cs)
		}
		return nil, fmt.Errorf("db: failed ping es db at %v, err: %v", cs, err)
	} else if ping != nil && ping.IsError() {
		return nil, fmt.Errorf("db: failed ping es db at %v, elastic err: %v", cs, ping.String())
	}

	var rw = &esQuerier{es: es}
	return &esDatabase{
		rw:          rw,
		raw:         es,
		queryLogger: cfg.QueryLogger,
	}, nil
}

func (c *esConnector) DialectName(scheme string) (db.DialectName, error) {
	return db.ELASTICSEARCH, nil
}
