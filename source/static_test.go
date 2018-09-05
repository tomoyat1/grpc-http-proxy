package source

import (
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"testing"
	
	"github.com/mercari/grpc-http-proxy/errors"
	"github.com/mercari/grpc-http-proxy/log"
)

func TestNewStatic(t *testing.T) {
	cases := []struct {
		name     string
		yamlFile string
		expected map[string]versions
	}{
		{
			name:     "valid yaml",
			yamlFile: "test-fixtures/valid.yaml",
			expected: map[string]versions{
				"a": {
					"v1": []*url.URL{parseURL(t, "a.v1")},
					"v2": []*url.URL{parseURL(t, "a.v2")},
				},
				"b": {
					"v1": []*url.URL{parseURL(t, "b.v1")},
				},
			},
		},
		{
			name:     "invalid yaml",
			yamlFile: "test-fixtures/invalid.yaml",
			expected: map[string]versions{},
		},
		{
			name:     "missing yaml",
			yamlFile: "test-fixtures/does-not-exist.yaml",
			expected: map[string]versions{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.NewDiscard()
			static := NewStatic(logger, tc.yamlFile)
			if got, want := static.records.m, tc.expected; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}

}

func TestStatic_Resolve(t *testing.T) {
	cases := []struct {
		name    string
		service string
		version string
		url     *url.URL
		err     error
	}{
		{
			name:    "resolved",
			service: "a",
			version: "v1",
			url:     parseURL(t, "a.v1"),
			err:     nil,
		},
		{
			name:    "service unresolvable",
			service: "b",
			version: "",
			url:     nil,
			err: &errors.ProxyError{
				Code:    errors.ServiceUnresolvable,
				Message: fmt.Sprintf("The gRPC service %s is unresolvable", "b"),
			},
		},
	}
	r := Records{
		m: map[string]versions{
			"a": {
				"v1": []*url.URL{parseURL(t, "a.v1")},
			},
		},
		recordsMu: sync.RWMutex{},
	}
	logger := log.NewDiscard()
	local := &Static{
		records: &r,
		logger:  logger,
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := local.Resolve(tc.service, tc.version)
			if got, want := u, tc.url; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
			if got, want := err, tc.err; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}
