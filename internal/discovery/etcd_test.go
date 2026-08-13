package discovery

import (
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type recordingClientConn struct{ states []resolver.State }

func (c *recordingClientConn) UpdateState(state resolver.State) error {
	c.states = append(c.states, state)
	return nil
}
func (*recordingClientConn) ReportError(error)                                    {}
func (*recordingClientConn) NewAddress([]resolver.Address)                        {}
func (*recordingClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult { return nil }

func TestResolverPublishesRemovalAndSkipsDrainingInstances(t *testing.T) {
	t.Parallel()
	cc := &recordingClientConn{}
	r := &etcdResolver{cc: cc}
	active, _ := json.Marshal(Metadata{InstanceID: "core-1", GRPCAddress: "127.0.0.1:9001"})
	draining, _ := json.Marshal(Metadata{InstanceID: "core-2", GRPCAddress: "127.0.0.1:9002", Draining: true})
	r.publish([]*mvccpb.KeyValue{{Value: active}, {Value: draining}})
	r.publish(nil)
	if len(cc.states) != 2 || len(cc.states[0].Addresses) != 1 || cc.states[0].Addresses[0].Addr != "127.0.0.1:9001" || len(cc.states[1].Addresses) != 0 {
		t.Fatalf("states = %+v", cc.states)
	}
}

func TestLoadTLSRejectsIncompleteClientPair(t *testing.T) {
	t.Parallel()
	if _, err := loadTLS("", "cert.pem", ""); err == nil {
		t.Fatal("expected incomplete key pair error")
	}
}
func TestLoadTLSDisabled(t *testing.T) {
	t.Parallel()
	config, err := loadTLS("", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if config != nil {
		t.Fatal("expected nil TLS config")
	}
}
