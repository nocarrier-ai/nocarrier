package devnats

import (
	"errors"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

// Start launches an embedded server on host:port with JetStream state stored
// under storeDir, and waits until it accepts connections.
func Start(host string, port int, storeDir string) (*server.Server, error) {
	ns, err := server.NewServer(&server.Options{
		ServerName: "nocarrier-dev",
		Host:       host,
		Port:       port,
		JetStream:  true,
		StoreDir:   storeDir,
	})
	if err != nil {
		return nil, err
	}
	go ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		return nil, errors.New("embedded NATS server did not become ready")
	}
	return ns, nil
}
