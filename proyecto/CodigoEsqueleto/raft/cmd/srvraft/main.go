package main

import (
	//"errors"
	//"fmt"
	//"log"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
	//"time"
)

func main() {
	// Convierte el primer argumento de la línea de comandos en un entero
	// Este entero representa el índice de este nodo en el array de nodos Raft
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	// Crea una lista de nodos de tipo HostPort usando los argumentos pasados
	// Cada uno de los elementos en `os.Args[2:]` representa la dirección de una réplica
	var nodos []rpctimeout.HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	// Crea una nueva instancia del nodo Raft y la registra en el servidor RPC
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	errorRegister := rpc.Register(nr) // Registra el nodo en el servidor RPC
	check.CheckError(errorRegister, "main ERROR: rpc.Register()")

	// Lanza la goroutine que se encarga de gestionar la lógica de liderazgo de Raft
	go nr.GestionarLiderazgo()

	// Configura el servidor para que escuche conexiones en la dirección del nodo actual
	// `os.Args[2:][me]` es la dirección de este nodo en `nodos`
	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "main ERROR: net.Listen()")

	rpc.Accept(l)
}
