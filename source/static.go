package source

import (
	"net/url"

	"go.uber.org/zap"
)

// Static provides service discovery with a static mapping of services and their backend FQDNs
type Static struct {
	records *Records
	logger  *zap.Logger
}

// NewStatic creates a new Static
func NewStatic(l *zap.Logger, mappingFile string) *Static {
	local := &Static{
		logger: l,
	}
	r, err := NewRecordsFromYAML(mappingFile)
	if err != nil {
		local.logger.Error("failed to initialize records from yaml",
			zap.String("err", err.Error()))
		local.records = NewRecords()
		return local
	}
	local.records = r
	return local
}

// Resolve resolves the FQDN for a backend providing the gRPC service specified
func (l *Static) Resolve(svc, version string) (*url.URL, error) {
	r, err := l.records.GetRecord(svc, version)
	if err != nil {
		l.logger.Error("failed to resolve service",
			zap.String("service", svc),
			zap.String("version", version),
			zap.String("err", err.Error()))
		return nil, err
	}
	return r, nil
}
