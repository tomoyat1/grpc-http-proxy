package source

import (
	"fmt"
	"sync"

	"github.com/mercari/grpc-http-proxy"
	"github.com/mercari/grpc-http-proxy/errors"
)

type versions map[string][]proxy.ServiceURL

// Records contains mappings from a gRPC service to upstream hosts
// It holds one upstream for each service version
type Records struct {
	m     map[string]versions
	mutex sync.RWMutex
}

func serviceUnresolvable(svc string) *errors.Error {
	return &errors.Error{
		Code:    errors.ServiceUnresolvable,
		Message: fmt.Sprintf("The gRPC service %s is unresolvable", svc),
	}
}

func versionNotFound(svc, version string) *errors.Error {
	return &errors.Error{
		Code:    errors.ServiceUnresolvable,
		Message: fmt.Sprintf("Version %s of the gRPC service %s is unresolvable", version, svc),
	}
}

func versionNotSpecified(svc string) *errors.Error {
	return &errors.Error{
		Code: errors.VersionNotSpecified,
		Message: fmt.Sprintf("There are multiple version of the gRPC service %s available. "+
			"You must specify one", svc),
	}
}

func versionUndecidable(svc string) *errors.Error {
	return &errors.Error{
		Code: errors.VersionUndecidable,
		Message: fmt.Sprintf("Multiple possible backends found for the gRPC service %s. "+
			"Add annotations to distinguish versions", svc),
	}
}

// NewRecords creates an empty mapping
func NewRecords() *Records {
	m := make(map[string]versions)
	return &Records{
		m:     m,
		mutex: sync.RWMutex{},
	}
}

// ClearRecords clears all mappings
func (r *Records) ClearRecords() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.m = make(map[string]versions)
}

// GetRecord gets a records of the specified (service, version) pair
func (r *Records) GetRecord(svc, version string) (proxy.ServiceURL, error) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	vs, ok := r.m[svc]
	if !ok {
		return nil, serviceUnresolvable(svc)
	}
	if version == "" {
		if len(vs) != 1 {
			return nil, versionNotSpecified(svc)
		}
		for _, entries := range vs {
			if len(entries) != 1 {
				return nil, versionUndecidable(svc)
			}
			return entries[0], nil // this returns the first (and only) ServiceURL
		}
	}
	entries, ok := vs[version]
	if !ok {
		return nil, versionNotFound(svc, version)
	}
	if len(entries) != 1 {
		return nil, versionUndecidable(svc)
	}
	return entries[0], nil
}

// SetRecord sets the backend service URL for the specifiec (service, version) pair.
// When successful, true will be returned.
// This fails if the URL for the blank version ("") is to be overwritten, and invalidates that entry.
func (r *Records) SetRecord(svc, version string, url proxy.ServiceURL) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.m[svc]; !ok {
		r.m[svc] = make(map[string][]proxy.ServiceURL)
	}
	if r.m[svc][version] == nil {
		r.m[svc][version] = make([]proxy.ServiceURL, 0)
	}
	r.m[svc][version] = append(r.m[svc][version], url)
	return true
}

// RemoveRecord removes a record of the specified (service, version) pair
func (r *Records) RemoveRecord(svc, version string, url proxy.ServiceURL) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	vs, ok := r.m[svc]
	if !ok {
		return
	}
	entries, ok := vs[version]
	if !ok {
		return
	}
	newEntries := make([]proxy.ServiceURL, 0)
	for _, e := range entries {
		if e.String() != url.String() {
			newEntries = append(newEntries, e)
		}
	}
	vs[version] = newEntries
	if len(newEntries) == 0 {
		delete(vs, version)
	}
	if len(vs) < 1 {
		delete(r.m, svc)
	}
}

// IsServiceUnique checks if there is only one version of a service
func (r *Records) IsServiceUnique(svc string) bool {
	r.mutex.RLock()
	b := len(r.m[svc]) == 1
	r.mutex.RUnlock()
	return b
}

// RecordExists checks if a record exists
func (r *Records) RecordExists(svc, version string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	vs, ok := r.m[svc]
	if !ok {
		return false
	}
	entries, ok := vs[version]
	if !ok {
		return false
	}
	return len(entries) > 0
}