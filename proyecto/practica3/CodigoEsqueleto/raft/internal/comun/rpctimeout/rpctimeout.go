package rpctimeout

import (
	"fmt"
	"net/rpc"
	"strings"
	"time"
)

type HostPort string // Con la forma, "host:puerto", con host mediante DNS o IP

const maxRetries = 3

func MakeHostPort(host, port string) HostPort {
	return HostPort(host + port)
}

func (hp HostPort) Host() string {
	return string(hp[:strings.Index(string(hp), ":")])
}

func (hp HostPort) Port() string {
	return string(hp[strings.Index(string(hp), ":")+1:])
}

func (hp HostPort) CallTimeout(serviceMethod string, args interface{},
	reply interface{}, timeout time.Duration) error {
	client, err := rpc.Dial("tcp", string(hp))
	if err != nil {
		fmt.Printf("Error dialing endpoint: %v\n", err)
		return err
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("Error cerrando la conexion TCP: %v\n", err)
		}
	}(client)
	for attempt := 1; attempt <= maxRetries; attempt++ {
		done := client.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1)).Done
		timer := time.NewTimer(timeout) // Crear un nuevo temporizador para esta iteración
		select {
		case call := <-done:
			if !timer.Stop() {
				// Drena el canal si el temporizador expiró.
				<-timer.C
			}
			return call.Error
		case <-timer.C:
			// El timeout expiró
			timeout *= 2
		}
		timer.Stop()
	}

	fmt.Printf("Total timeout in CallTimeout after %d attempts\n", maxRetries)
	return fmt.Errorf("failed to call %s after %d attempts", serviceMethod, maxRetries)
}

func StringArrayToHostPortArray(stringArray []string) (result []HostPort) {

	for _, s := range stringArray {
		result = append(result, HostPort(s))
	}

	return
}

// Array de HostPort end points a un solo string CON ESPACIO DE SEPARACION
func HostPortArrayToString(hostPortArray []HostPort) (result string) {
	for _, hostPort := range hostPortArray {
		result = result + " " + string(hostPort)
	}

	return
}
