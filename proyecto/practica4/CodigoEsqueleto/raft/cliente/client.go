package main

import (
	"fmt"
	"math/rand"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"time"
)

// Genera una operación aleatoria (leer o escribir).
func generarOperacion() raft.TipoOperacion {
	operaciones := []string{"leer", "escribir"}
	operacion := operaciones[rand.Intn(len(operaciones))] // Seleccionar aleatoriamente entre leer y escribir

	clave := fmt.Sprintf("clave_%d", rand.Intn(100)) // Generar una clave aleatoria

	if operacion == "escribir" {
		valor := fmt.Sprintf("valor_%d", rand.Intn(1000)) // Generar un valor aleatorio
		return raft.TipoOperacion{
			Operacion: "escribir",
			Clave:     clave,
			Valor:     valor,
		}
	}
	return raft.TipoOperacion{
		Operacion: "leer",
		Clave:     clave,
		Valor:     "", // Valor vacío en lectura
	}
}

// Envia una operación al nodo Raft usando RPC con timeout.
func enviarOperacion(node rpctimeout.HostPort, nodos []rpctimeout.HostPort, operacion raft.TipoOperacion) error {
	var resultado raft.ResultadoRemoto
	err := node.CallTimeout("NodoRaft.SometerOperacionRaft", operacion, &resultado, 2*time.Second)
	if err != nil {
		return fmt.Errorf("error al enviar operación: %v", err)
	}

	if resultado.EsLider {
		fmt.Printf("Operación confirmada en líder %s: %+v\n", string(node), resultado)
	} else {
		// Validar que el IdLider es válido
		if resultado.IdLider >= 0 && resultado.IdLider < len(nodos) {
			fmt.Printf("Redirigiendo al líder: %s\n", nodos[resultado.IdLider])
			return enviarOperacion(nodos[resultado.IdLider], nodos, operacion) // Redirigir al líder
		} else {
			fmt.Println("IdLider inválido, no se puede redirigir.")
		}
	}
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano()) // Semilla para la generación aleatoria

	// Lista de nodos Raft con sus direcciones
	nodos := rpctimeout.StringArrayToHostPortArray([]string{
		"172.20.100.2:8000",
		"172.20.100.3:8001",
		"172.20.100.4:8002",
		"172.20.100.5:8003",
		"172.20.100.6:8004",
	})

	for {
		// Seleccionar un nodo aleatorio para probar
		nodo := nodos[rand.Intn(len(nodos))]

		// Generar operación aleatoria
		operacion := generarOperacion()

		fmt.Printf("Enviando operación: %+v al nodo %s\n", operacion, string(nodo))

		// Enviar la operación al nodo seleccionado
		err := enviarOperacion(nodo, nodos, operacion)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}

		// Esperar un tiempo aleatorio antes de la siguiente solicitud
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
}
