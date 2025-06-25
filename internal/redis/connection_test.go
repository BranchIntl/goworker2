package redis

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	goworkerErrors "github.com/BranchIntl/goworker2/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simplified mock using struct literal - reduces 25 lines to 10
type mockOpts struct {
	uri, certPath                                       string
	maxConn, maxIdle                                    int
	idleTimeout, connTimeout, readTimeout, writeTimeout time.Duration
	useTLS, tlsSkipVerify                               bool
}

func (m mockOpts) GetURI() string                   { return m.uri }
func (m mockOpts) GetMaxConnections() int           { return m.maxConn }
func (m mockOpts) GetMaxIdle() int                  { return m.maxIdle }
func (m mockOpts) GetIdleTimeout() time.Duration    { return m.idleTimeout }
func (m mockOpts) GetConnectTimeout() time.Duration { return m.connTimeout }
func (m mockOpts) GetReadTimeout() time.Duration    { return m.readTimeout }
func (m mockOpts) GetWriteTimeout() time.Duration   { return m.writeTimeout }
func (m mockOpts) GetUseTLS() bool                  { return m.useTLS }
func (m mockOpts) GetTLSSkipVerify() bool           { return m.tlsSkipVerify }
func (m mockOpts) GetTLSCertPath() string           { return m.certPath }

// Helper functions to reduce repetition
func defaultOpts() mockOpts {
	return mockOpts{
		uri: "redis://localhost:6379", maxConn: 10, maxIdle: 5,
		idleTimeout: 5 * time.Minute, connTimeout: 10 * time.Second,
		readTimeout: 10 * time.Second, writeTimeout: 10 * time.Second,
	}
}

func unreachableOpts(uri string) mockOpts {
	opts := defaultOpts()
	opts.uri = uri
	opts.connTimeout = 100 * time.Millisecond
	return opts
}

func assertConnError(t *testing.T, err error) {
	require.Error(t, err)
	var connErr *goworkerErrors.ConnectionError
	assert.ErrorAs(t, err, &connErr)
}

func TestCreatePool(t *testing.T) {
	tests := []struct {
		name string
		opts mockOpts
	}{
		{"default options", defaultOpts()},
		{"custom settings", mockOpts{
			uri: "redis://localhost:6379", maxConn: 20, maxIdle: 10,
			idleTimeout: 10 * time.Minute, connTimeout: 5 * time.Second,
			readTimeout: 15 * time.Second, writeTimeout: 15 * time.Second,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := CreatePool(tt.opts)

			require.NoError(t, err)
			require.NotNil(t, pool)
			assert.Equal(t, tt.opts.maxConn, pool.MaxActive)
			assert.Equal(t, tt.opts.maxIdle, pool.MaxIdle)
			assert.Equal(t, tt.opts.idleTimeout, pool.IdleTimeout)
			assert.NotNil(t, pool.Dial, "Dial function should be set")
			assert.NotNil(t, pool.TestOnBorrow, "TestOnBorrow function should be set")

			// Test TestOnBorrow function behavior
			err = pool.TestOnBorrow(nil, time.Now())
			assert.NoError(t, err, "TestOnBorrow should return nil for recent connections")
		})
	}
}

// Combine all DialRedis tests into one comprehensive test
func TestDialRedis(t *testing.T) {
	tests := []struct {
		name      string
		opts      mockOpts
		expectErr string
	}{
		{"invalid URI format", mockOpts{uri: ":/invalid-uri"}, ""},
		{"malformed URI", mockOpts{uri: "redis://[invalid-host"}, ""},
		{"unsupported scheme", mockOpts{uri: "http://localhost:6379"}, "invalid"},
		{"redis basic", unreachableOpts("redis://unreachable-host:6379"), ""},
		{"redis with password", unreachableOpts("redis://:password@unreachable-host:6379"), ""},
		{"redis with user:pass", unreachableOpts("redis://user:password@unreachable-host:6379"), ""},
		{"redis with database", unreachableOpts("redis://unreachable-host:6379/2"), ""},
		{"redis with pass+db", unreachableOpts("redis://:password@unreachable-host:6379/1"), ""},
		{"rediss TLS", unreachableOpts("rediss://unreachable-host:6380"), ""},
		{"unix socket", mockOpts{uri: "unix:///tmp/redis.sock"}, ""},
		{"TLS explicit", func() mockOpts {
			o := unreachableOpts("redis://unreachable-host:6379")
			o.useTLS = true
			return o
		}(), ""},
		{"TLS skip verify", func() mockOpts {
			o := unreachableOpts("redis://unreachable-host:6379")
			o.useTLS = true
			o.tlsSkipVerify = true
			return o
		}(), ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DialRedis(tt.opts)

			if tt.expectErr == "invalid" {
				require.Error(t, err)
				var connErr *goworkerErrors.ConnectionError
				require.ErrorAs(t, err, &connErr)
				assert.ErrorIs(t, connErr.Unwrap(), ErrInvalidScheme)
			} else {
				assertConnError(t, err)
			}
		})
	}
}

// Combine all cert tests
func TestLoadCertPool(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cert_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name    string
		path    string
		content string
		expect  string
	}{
		{"file not found", "/nonexistent/path/cert.pem", "", "failed to read cert file"},
		{"empty cert", filepath.Join(tmpDir, "empty.crt"), "", "failed to append certs"},
		{"invalid cert", filepath.Join(tmpDir, "invalid.crt"), "invalid content", "failed to append certs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the file for all test cases except the "file not found" case
			if tt.name != "file not found" {
				err := os.WriteFile(tt.path, []byte(tt.content), 0644)
				require.NoError(t, err)
			}

			_, err := LoadCertPool(tt.path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expect)
		})
	}
}

func TestDialRedis_WithCerts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cert_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name    string
		path    string
		content string
		expect  string
	}{
		{"invalid cert content", filepath.Join(tmpDir, "test.crt"), "invalid certificate content", "failed to append certs"},
		{"missing cert file", "/nonexistent/cert.pem", "", "failed to read cert file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.content != "" {
				err := os.WriteFile(tt.path, []byte(tt.content), 0644)
				require.NoError(t, err)
			}

			opts := unreachableOpts("rediss://unreachable-host:6380")
			opts.certPath = tt.path

			_, err := DialRedis(opts)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expect)
		})
	}
}
