package resque

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	"github.com/gomodule/redigo/redis"
)

var (
	errInvalidScheme = errors.New("invalid Redis database URI scheme")
)

// createPool creates a Redis connection pool
func createPool(options Options) (*redis.Pool, error) {
	return &redis.Pool{
		MaxActive:   options.MaxConnections,
		MaxIdle:     options.MaxIdle,
		IdleTimeout: options.IdleTimeout,
		Dial: func() (redis.Conn, error) {
			return dialRedis(options)
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}, nil
}

// dialRedis establishes a Redis connection
func dialRedis(options Options) (redis.Conn, error) {
	uri, err := url.Parse(options.URI)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %w", err)
	}

	var network string
	var host string
	var password string
	var db string
	var dialOptions []redis.DialOption

	// Configure timeouts
	dialOptions = append(dialOptions,
		redis.DialConnectTimeout(options.ConnectTimeout),
		redis.DialReadTimeout(options.ReadTimeout),
		redis.DialWriteTimeout(options.WriteTimeout),
	)

	switch uri.Scheme {
	case "redis", "rediss":
		network = "tcp"
		host = uri.Host
		if uri.User != nil {
			password, _ = uri.User.Password()
		}
		if len(uri.Path) > 1 {
			db = uri.Path[1:]
		}

		// Configure TLS for rediss or if explicitly enabled
		if uri.Scheme == "rediss" || options.UseTLS {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: options.TLSSkipVerify,
			}

			if options.TLSCertPath != "" {
				pool, err := loadCertPool(options.TLSCertPath)
				if err != nil {
					return nil, err
				}
				tlsConfig.RootCAs = pool
			}

			dialOptions = append(dialOptions,
				redis.DialUseTLS(true),
				redis.DialTLSConfig(tlsConfig),
			)
		}
	case "unix":
		network = "unix"
		host = uri.Path
	default:
		return nil, errInvalidScheme
	}

	// Establish connection
	conn, err := redis.Dial(network, host, dialOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// Authenticate if password provided
	if password != "" {
		if _, err := conn.Do("AUTH", password); err != nil {
			conn.Close()
			return nil, fmt.Errorf("authentication failed: %w", err)
		}
	}

	// Select database if specified
	if db != "" {
		if _, err := conn.Do("SELECT", db); err != nil {
			conn.Close()
			return nil, fmt.Errorf("failed to select database: %w", err)
		}
	}

	return conn, nil
}

// loadCertPool loads a certificate pool from a file
func loadCertPool(certPath string) (*x509.CertPool, error) {
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	certs, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read cert file %q: %w", certPath, err)
	}

	if ok := rootCAs.AppendCertsFromPEM(certs); !ok {
		return nil, fmt.Errorf("failed to append certs from %q", certPath)
	}

	return rootCAs, nil
}
