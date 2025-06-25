package resque

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	assert.Equal(t, "redis://localhost:6379/", opts.URI)
	assert.Equal(t, "resque:", opts.Namespace)
	assert.Equal(t, 10, opts.MaxConnections)
	assert.Equal(t, 2, opts.MaxIdle)
	assert.Equal(t, 240*time.Second, opts.IdleTimeout)
	assert.Equal(t, 10*time.Second, opts.ConnectTimeout)
	assert.Equal(t, 10*time.Second, opts.ReadTimeout)
	assert.Equal(t, 10*time.Second, opts.WriteTimeout)
	assert.False(t, opts.UseTLS)
	assert.False(t, opts.TLSSkipVerify)
	assert.Empty(t, opts.TLSCertPath)
}
