package redis

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"time"

	goworkerErrors "github.com/benmanns/goworker/errors"
	"github.com/gomodule/redigo/redis"
)

var (
	// ErrInvalidScheme is returned when the Redis URI scheme is invalid
	ErrInvalidScheme = errors.New("invalid Redis database URI scheme")
)

// ConnectionOptions defines the interface for Redis connection options
type ConnectionOptions interface {
	GetURI() string
	GetMaxConnections() int
	GetMaxIdle() int
	GetIdleTimeout() time.Duration
	GetConnectTimeout() time.Duration
	GetReadTimeout() time.Duration
	GetWriteTimeout() time.Duration
	GetUseTLS() bool
	GetTLSSkipVerify() bool
	GetTLSCertPath() string
}

// CreatePool creates a Redis connection pool using the provided options
func CreatePool(options ConnectionOptions) (*redis.Pool, error) {
	return &redis.Pool{
		MaxActive:   options.GetMaxConnections(),
		MaxIdle:     options.GetMaxIdle(),
		IdleTimeout: options.GetIdleTimeout(),
		Dial: func() (redis.Conn, error) {
			return DialRedis(options)
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

// DialRedis establishes a Redis connection using the provided options
func DialRedis(options ConnectionOptions) (redis.Conn, error) {
	uri, err := url.Parse(options.GetURI())
	if err != nil {
		return nil, goworkerErrors.NewConnectionError(options.GetURI(),
			fmt.Errorf("invalid URI: %w", err))
	}

	var network string
	var host string
	var password string
	var db string
	var dialOptions []redis.DialOption

	// Configure timeouts
	dialOptions = append(dialOptions,
		redis.DialConnectTimeout(options.GetConnectTimeout()),
		redis.DialReadTimeout(options.GetReadTimeout()),
		redis.DialWriteTimeout(options.GetWriteTimeout()),
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
		if uri.Scheme == "rediss" || options.GetUseTLS() {
			tlsConfig := &tls.Config{
				InsecureSkipVerify: options.GetTLSSkipVerify(),
			}

			if options.GetTLSCertPath() != "" {
				pool, err := LoadCertPool(options.GetTLSCertPath())
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
		return nil, goworkerErrors.NewConnectionError(options.GetURI(), ErrInvalidScheme)
	}

	// Establish connection
	conn, err := redis.Dial(network, host, dialOptions...)
	if err != nil {
		return nil, goworkerErrors.NewConnectionError(options.GetURI(),
			fmt.Errorf("failed to connect: %w", err))
	}

	// Authenticate if password provided
	if password != "" {
		if _, err := conn.Do("AUTH", password); err != nil {
			conn.Close()
			return nil, goworkerErrors.NewConnectionError(options.GetURI(),
				fmt.Errorf("authentication failed: %w", err))
		}
	}

	// Select database if specified
	if db != "" {
		if _, err := conn.Do("SELECT", db); err != nil {
			conn.Close()
			return nil, goworkerErrors.NewConnectionError(options.GetURI(),
				fmt.Errorf("failed to select database: %w", err))
		}
	}

	return conn, nil
}

// LoadCertPool loads a certificate pool from a file
func LoadCertPool(certPath string) (*x509.CertPool, error) {
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
