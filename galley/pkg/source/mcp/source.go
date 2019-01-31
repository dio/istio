package mcp

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/hashicorp/go-multierror"
	"google.golang.org/grpc"

	mcpapi "istio.io/api/mcp/v1alpha1"
	"istio.io/istio/galley/pkg/metadata"
	"istio.io/istio/galley/pkg/runtime"
	"istio.io/istio/galley/pkg/runtime/resource"
	"istio.io/istio/pilot/pkg/bootstrap"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/mcp/client"
	"istio.io/istio/pkg/mcp/sink"
)

var scope = log.RegisterScope("mcp", "MCP runtime source", 0)

type source struct {
	nodeID  string
	address string

	ctx  context.Context
	dial func(ctx context.Context, address string) (mcpapi.AggregatedMeshConfigServiceClient, error)

	stop    func()
	handler resource.EventHandler

	localCache map[string]cacheEntry
}

type cacheEntry map[string]*cache

type cache struct {
	version         string
	previousVersion string
	eventKind       resource.EventKind
	entry           *resource.Entry
}

var _ runtime.Source = &source{}
var _ sink.Updater = &source{}

// New ...
func New(ctx context.Context, mcpAddress, nodeID string) (runtime.Source, error) {
	return &source{
		nodeID:  nodeID,
		address: mcpAddress,
		ctx:     ctx,
		dial: func(ctx context.Context, address string) (mcpapi.AggregatedMeshConfigServiceClient, error) {
			// TODO: configure connection creds
			securityOption := grpc.WithInsecure()

			// Copied from pilot/pkg/bootstrap/server.go
			msgSizeOption := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(bootstrap.DefaultMCPMaxMsgSize))
			conn, err := grpc.DialContext(ctx, address, securityOption, msgSizeOption)
			if err != nil {
				scope.Errorf("Unable to dial MCP Server %q: %v", address, err)
				return nil, err
			}
			return mcpapi.NewAggregatedMeshConfigServiceClient(conn), nil
		},
	}, nil
}

func (s *source) Apply(c *sink.Change) error {
	i, found := metadata.Types.Lookup(c.Collection)
	if !found {
		return fmt.Errorf("invalid collection: %v", c.Collection)
	}
	scope.Debugf("received object %+v and length %d", c, len(c.Objects))

	changeTypeURL := i.TypeURL.String()

	if _, ok := s.localCache[changeTypeURL]; !ok {
		s.localCache[changeTypeURL] = cacheEntry{}
	} else {
		for _, c := range s.localCache[changeTypeURL] {
			c.eventKind = resource.Deleted
		}
	}

	var errs error
	for _, o := range c.Objects {
		if o.TypeURL != changeTypeURL {
			errs = multierror.Append(errs,
				fmt.Errorf("type %v mismatch in received object: %v", changeTypeURL, o.TypeURL))
			continue
		}

		entry, err := toEntry(c.Collection, o)
		if err != nil {
			errs = multierror.Append(errs, err)
			continue
		}

		if err != nil {

		}

		if errs == nil {
			lc, ok := s.localCache[o.TypeURL][o.Metadata.Name]
			if ok {
				if lc.version == o.Metadata.Version {
					lc.eventKind = resource.None
				} else {
					lc.previousVersion = lc.version
					lc.version = o.Metadata.Version
					lc.eventKind = resource.Updated
				}
				lc.entry = &entry
			} else {
				s.localCache[o.TypeURL][o.Metadata.Name] = &cache{
					version:   o.Metadata.Version,
					eventKind: resource.Added,
					entry:     &entry,
				}
			}
		}
	}

	if errs != nil {
		for key, lc := range s.localCache[changeTypeURL] {
			if lc.eventKind == resource.Added {
				// this is safe because anything with state `Added` was new in this batch.
				delete(s.localCache[changeTypeURL], key)
			} else if lc.eventKind == resource.Updated {
				// We are not worried about the entry as it will be overwritten.
				lc.version = lc.previousVersion
			}
		}
		return errs
	}

	var forceFullSync bool
	for key, lc := range s.localCache[changeTypeURL] {
		if lc.eventKind == resource.None {
			continue
		}
		forceFullSync = true
		scope.Debugf("pushed an event %+v", lc.eventKind)
		s.handler(resource.Event{
			Kind:  lc.eventKind,
			Entry: *lc.entry,
		})

		// If its a resource.Deleted event, remove it from the local cache.
		if lc.eventKind == resource.Deleted {
			delete(s.localCache[changeTypeURL], key)
		}
	}
	if forceFullSync {
		// Do a FullSync after all events for a publish.
		s.handler(resource.Event{Kind: resource.FullSync})
	}

	return nil
}

func (s *source) Start(handler resource.EventHandler) error {
	ctx, cancel := context.WithCancel(s.ctx)
	s.stop = cancel

	s.handler = handler

	cl, err := s.dial(ctx, s.address)
	if err != nil {
		return err
	}

	options := &sink.Options{}
	mcpClient := client.New(cl, options)

	go mcpClient.Run(ctx)

	return nil
}

func (s *source) Stop() {
	s.stop()
}

// toEntry converts the object into a resource.Entry. It returns an error if it fails to marshal the object's time,
// but the resulting Entry is still usable.
func toEntry(collection string, o *sink.Object) (resource.Entry, error) {
	t, err := types.TimestampFromProto(o.Metadata.CreateTime)
	if err != nil {
		t = time.Now()
	}

	i, found := metadata.Types.Lookup(collection)
	if !found {
		return resource.Entry{}, fmt.Errorf("invalid type: %v", o)
	}

	return resource.Entry{
		ID: resource.VersionedKey{
			Key: resource.Key{
				Collection: i.Collection,
				FullName:   resource.FullNameFromNamespaceAndName("", o.Metadata.Name),
			},
			Version: resource.Version(o.Metadata.Version),
		},
		Item: o.Body,
		Metadata: resource.Metadata{
			CreateTime:  t,
			Labels:      o.Metadata.Labels,
			Annotations: o.Metadata.Annotations,
		},
	}, err
}
