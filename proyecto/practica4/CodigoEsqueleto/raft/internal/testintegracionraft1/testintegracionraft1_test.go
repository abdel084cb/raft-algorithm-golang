package testintegracionraft1

import (
	"fmt"
	"os"
	"path/filepath"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
	"strconv"
	"testing"
	"time"
)

const TimeoutRpc = 2000

const (
	MAQUINA1       = "192.168.3.1"
	MAQUINA2       = "192.168.3.1"
	MAQUINA3       = "192.168.3.1"
	PUERTOREPLICA1 = "31050"
	PUERTOREPLICA2 = "31051"
	PUERTOREPLICA3 = "31052"
	REPLICA1       = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2       = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3       = MAQUINA3 + ":" + PUERTOREPLICA3
	EXECREPLICA    = "cmd/srvraft/main.go"
	PRIVKEYFILE    = "id_rsa"
)

var PATH string = filepath.Join(os.Getenv("HOME"), "practica-4-SDIST", "proyecto", "practica4", "CodigoEsqueleto", "raft")
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// Primer conjunto de tests básicos para validar el comportamiento inicial del sistema
func TestPrimerasPruebas(t *testing.T) {
	// Configuración inicial del entorno de despliegue con 3 réplicas
	cfg := makeCfgDespliegue(t,
		3,                                      // Número de réplicas
		[]string{REPLICA1, REPLICA2, REPLICA3}, // Dirección de las réplicas
		[]bool{true, true, true})               // Estado inicial de conexión de las réplicas
	// Garantiza la limpieza de procesos al final del test
	defer cfg.stop()

	// Test1: Verifica que al inicio no se elige ningún líder en el clúster
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	// Test2: Comprueba que el clúster puede elegir correctamente un líder al inicio
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	// Test3: Valida que tras el fallo de un líder, el clúster elige un nuevo líder correctamente
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	// Test4: Verifica que tres operaciones se comprometen exitosamente en un entorno estable
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })
}

// Conjunto de tests para validar acuerdos en situaciones con fallos
func TestAcuerdosConFallos(t *testing.T) {
	// Configuración inicial del entorno de despliegue con 3 réplicas
	cfg := makeCfgDespliegue(t,
		3,                                      // Número de réplicas
		[]string{REPLICA1, REPLICA2, REPLICA3}, // Dirección de las réplicas
		[]bool{true, true, true})               // Estado inicial de conexión de las réplicas
	// Garantiza la limpieza de procesos al final del test
	defer cfg.stop()

	// Test5.1: Valida que el clúster puede alcanzar un acuerdo incluso si un seguidor se desconecta
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	// Test5.2: Comprueba que el clúster no puede alcanzar acuerdos si la mayoría de los nodos fallan
	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	// Test5.3: Evalúa la capacidad del clúster para manejar operaciones concurrentes de forma consistente
	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })
}

// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)
	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// Operativa en configuracion de despliegue y pruebas asociadas
type configDespliegue struct {
	t           *testing.T
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
	cr          canalResultados
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(t *testing.T, n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.t = t
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)
	cfg.cr = make(canalResultados, 2000)
	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()
	time.Sleep(50 * time.Millisecond)
	cfg.cr.stop()
}

// --------------------------------------------------------------------------//
// FUNCIONES DE SUBTESTS													 //
// --------------------------------------------------------------------------//

// AcuerdoApesarDeSeguidor valida que el clúster puede alcanzar y mantener acuerdos
// incluso si un nodo seguidor se desconecta temporalmente.
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar los procesos distribuidos en las réplicas
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second) // Esperar a que los nodos estén completamente inicializados

	// Obtener el ID del líder inicial tras iniciar el clúster
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Desconectar uno de los nodos seguidores
	nodoDesconectado := (idLider + 1) % 3 // Selecciona un seguidor
	cfg.stopNode(nodoDesconectado)
	fmt.Printf("Nodo %d desconectado\n", nodoDesconectado)

	// Realizar dos operaciones mientras el seguidor está desconectado
	cfg.commitOperation("escribir", "clave1", "valor1", 0, idLider)
	cfg.commitOperation("escribir", "clave2", "valor2", 1, idLider)

	// Reconectar el nodo desconectado
	fmt.Printf("Reconectando nodo %d\n", nodoDesconectado)
	cfg.startNode(nodoDesconectado)
	time.Sleep(10 * time.Second) // Dar tiempo al nodo para sincronizarse

	// Verificar que los índices y mandatos en los logs son consistentes en todas las réplicas
	if res := cfg.verifyIndexTermForEachNode(1, 1); res != 0 {
		t.Fatalf("Los logs no coinciden después de la reconexión")
	}

	// Detener los procesos distribuidos al finalizar la prueba
	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// SinAcuerdoPorFallos valida que no se puede alcanzar un acuerdo en el clúster
// cuando se desconecta la mayoría de los nodos seguidores (quorum perdido).
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar los procesos distribuidos en las réplicas
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second) // Esperar a que los nodos estén completamente inicializados

	// Identificar el líder inicial del clúster
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Desconectar la mayoría de los nodos seguidores, dejando solo al líder
	for i := 0; i < 3; i++ {
		if i != idLider {
			cfg.stopNode(i) // Desconecta nodos que no son líderes
		}
	}

	// Intentar realizar una operación (debe fallar debido a la falta de quorum)
	fmt.Println("Intentando someter operación sin mayoría...")
	indice, _, _, _, valueReply := cfg.submitOperation("escribir", "clave3", "valor3", idLider)

	// Validar que la operación no fue comprometida
	if indice != -1 || valueReply == "Se ha escrito en RAM" {
		t.Fatalf("Error: operación no debería haberse comprometido sin mayoría, pero se recibió índice %d y valor %s", indice, valueReply)
	}

	fmt.Println("Operación correctamente rechazada sin mayoría.")

	// Finalizar los procesos distribuidos tras la prueba
	fmt.Println(".............", t.Name(), "Superado")
	cfg.stopDistributedProcesses()
}

// SometerConcurrentementeOperaciones verifica que el clúster Raft puede manejar
// múltiples operaciones sometidas de forma concurrente y mantener la consistencia.
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar los procesos distribuidos en las réplicas
	cfg.startDistributedProcesses()
	time.Sleep(10 * time.Second) // Esperar a que los nodos se inicialicen completamente

	// Identificar el líder del clúster
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Crear una lista de operaciones para someter al clúster
	operaciones := []raft.TipoOperacion{
		{Operacion: "escribir", Clave: "clave1", Valor: "valor1"},
		{Operacion: "escribir", Clave: "clave2", Valor: "valor2"},
		{Operacion: "escribir", Clave: "clave3", Valor: "valor3"},
		{Operacion: "escribir", Clave: "clave4", Valor: "valor4"},
		{Operacion: "escribir", Clave: "clave5", Valor: "valor5"},
	}

	// Ejecutar cada operación concurrentemente en el líder
	for i, op := range operaciones {
		go func(op raft.TipoOperacion, indice int) {
			cfg.concurrentOperation(idLider, op, 1, "Se ha escrito en RAM")
		}(op, i)
	}

	// Esperar a que todas las operaciones concurrentes se estabilicen en el clúster
	time.Sleep(20 * time.Second)

	// Verificar que todos los nodos tienen logs consistentes
	if res := cfg.verifyIndexTermForEachNode(4, 1); res != 0 {
		t.Fatalf("Error: logs inconsistentes después de operaciones concurrentes")
	}

	// Detener los procesos distribuidos tras la prueba
	cfg.stopDistributedProcesses()
	fmt.Println(".............", t.Name(), "Superado")
}

// Test1: soloArranqueYparadaTest1 verifica que no hay ningún líder en el clúster Raft
// cuando los nodos han sido inicializados pero aún no han intercambiado mensajes de latido.
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Asociar el test actual a la configuración para registrar errores
	cfg.t = t

	// Iniciar los procesos distribuidos en las réplicas
	cfg.startDistributedProcesses()

	// Verificar el estado de la réplica 0: no debe haber un líder inicial
	cfg.verifyState(0, 0, false, -1)

	// Verificar el estado de la réplica 1: no debe haber un líder inicial
	cfg.verifyState(1, 0, false, -1)

	// Verificar el estado de la réplica 2: no debe haber un líder inicial
	cfg.verifyState(2, 0, false, -1)

	// Detener los procesos distribuidos tras la prueba
	cfg.stopDistributedProcesses()

	// Indicar que el test se ha completado con éxito
	fmt.Println(".............", t.Name(), "Superado")
}

// Test2: elegirPrimerLiderTest2 verifica que, tras iniciar las réplicas, se elige
// un líder correctamente en el primer intento dentro del clúster Raft.
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Inicia los procesos distribuidos en las réplicas.
	cfg.startDistributedProcesses()

	// Mensaje para indicar que se está probando la elección de un líder.
	fmt.Printf("Probando líder en curso\n")

	// Esperar un tiempo para permitir la elección del líder.
	time.Sleep(10 * time.Second)

	// Comprobar que se ha elegido un único líder entre las réplicas.
	cfg.pruebaUnLider(3)

	// Detener los procesos distribuidos después de completar la prueba.
	cfg.stopDistributedProcesses()

	// Mensaje final indicando que la prueba se completó con éxito.
	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer líder y reelección de uno nuevo - 3 NODOS RAFT
// Este test verifica que, tras desconectar al líder actual, el sistema es capaz
// de realizar una nueva elección de líder correctamente.
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	fmt.Println(t.Name(), ".....................") // Nombre del test para seguimiento

	// Inicia los procesos distribuidos en las réplicas.
	cfg.startDistributedProcesses()

	// Mensaje indicando que se va a identificar al líder inicial.
	fmt.Printf("Líder inicial\n")

	// Espera para permitir que las réplicas realicen la elección inicial.
	time.Sleep(10 * time.Second)

	// Identifica el líder actual entre las réplicas.
	liderActual := cfg.pruebaUnLider(3)

	// Desconecta al líder actual para simular un fallo.
	cfg.desconectarLider(liderActual)

	// Espera para permitir que las réplicas realicen una nueva elección de líder.
	time.Sleep(15 * time.Second)

	// Mensaje indicando que se comprobará la elección del nuevo líder.
	fmt.Printf("Comprobar nuevo líder\n")

	// Comprueba que se ha elegido correctamente un nuevo líder entre las réplicas.
	cfg.pruebaUnLider(3)

	// Detiene los procesos distribuidos tras completar el test.
	cfg.stopDistributedProcesses()

	// Mensaje final indicando que la prueba se completó con éxito.
	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situación estable y sin fallos - 3 NODOS RAFT
// Este test verifica que en una configuración estable, sin fallos, el sistema
// puede comprometer tres operaciones correctamente en el clúster.
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	fmt.Println(t.Name(), ".....................") // Indica el inicio del test

	// Inicia los procesos distribuidos en las réplicas.
	cfg.startDistributedProcesses()

	// Mensaje indicando que se está comprobando el líder.
	fmt.Printf("Comprobando líder\n")

	// Espera para permitir que el clúster elija un líder.
	time.Sleep(10 * time.Second)

	// Identifica el líder actual del clúster.
	idLider := cfg.pruebaUnLider(3)

	// Somete y compromete una operación de lectura.
	cfg.commitOperation("leer", "clave0", "valor0", 0, idLider)

	// Somete y compromete dos operaciones de escritura.
	cfg.commitOperation("escribir", "clave1", "valor1", 1, idLider)
	cfg.commitOperation("escribir", "clave2", "valor2", 2, idLider)

	// Detiene los procesos distribuidos tras completar las operaciones.
	cfg.stopDistributedProcesses()

	// Mensaje final indicando que la prueba se completó con éxito.
	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------//
//                          FUNCIONES DE APOYO								 //
// --------------------------------------------------------------------------//

// pruebaUnLider verifica que hay un solo líder en el clúster Raft.
// Este método intenta múltiples veces comprobar si existe un único líder válido.
// En caso de éxito, devuelve el ID del líder. Si no se encuentra un líder único, se genera un error.
func (cfg *configDespliegue) pruebaUnLider(numNodos int) int {
	// Repetir el proceso de verificación hasta 10 veces para manejar posibles reelecciones.
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond) // Pausa entre intentos.

		// Mapa para registrar los líderes detectados por término.
		lideres := make(map[int][]int)

		// Iterar sobre los nodos conectados y verificar su estado.
		for i := 0; i < numNodos; i++ {
			if cfg.conectados[i] {
				// Obtener el estado remoto del nodo y verificar si es líder.
				if _, termino, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					lideres[termino] = append(lideres[termino], i)
				}
			}
		}

		// Variable para almacenar el término más reciente con un líder.
		ultimoMandatoConLider := -1

		// Verificar la consistencia de los líderes detectados.
		for termino, lideres := range lideres {
			// Si hay más de un líder en el mismo término, se considera un error.
			if len(lideres) > 1 {
				cfg.t.Fatalf("termino %d tiene %d mas de un lider",
					termino, len(lideres))
			}
			// Actualizar el término más reciente.
			if termino > ultimoMandatoConLider {
				ultimoMandatoConLider = termino
			}
		}

		// Si se detectaron líderes válidos, devolver el ID del líder más reciente.
		if len(lideres) != 0 {
			return lideres[ultimoMandatoConLider][0] // Retorna el ID del líder.
		}
	}

	// Si no se encuentra un líder único tras varios intentos, se genera un error.
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Código inalcanzable; se incluye para cumplir con el tipo de retorno.
}

// desconectarLider desconecta el nodo líder especificado.
// Este método utiliza una llamada RPC para detener el proceso que está actuando como líder.
func (cfg *configDespliegue) desconectarLider(idLider int) {
	// Crear una variable para almacenar la respuesta de la llamada RPC.
	var reply raft.EstadoRemoto

	// Enviar una llamada RPC para detener el nodo líder identificado por idLider.
	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)

	// Verificar si ocurrió algún error durante la llamada RPC.
	check.CheckError(err, "Error en llamada RPC Para nodo")

	// Actualizar el estado del nodo desconectado en la configuración.
	cfg.conectados[idLider] = false
}

// obtenerEstadoRemoto obtiene el estado actual de un nodo Raft remoto.
// Este método utiliza una llamada RPC para consultar información sobre el nodo especificado.
// Devuelve el ID del nodo, el mandato actual, si el nodo es líder y el ID del líder actual.
func (cfg *configDespliegue) obtenerEstadoRemoto(
	index int) (int, int, bool, int) {
	// Crear una estructura para almacenar la respuesta de la llamada RPC.
	var reply raft.EstadoRemoto

	// Realizar la llamada RPC al método remoto "NodoRaft.ObtenerEstadoNodo".
	// Se pasa una estructura vacía como argumento y se captura la respuesta en `reply`.
	err := cfg.nodosRaft[index].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)

	// Verificar si ocurrió algún error durante la llamada RPC.
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	// Retornar los valores obtenidos: ID del nodo, mandato actual, si es líder y el ID del líder.
	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// startDistributedProcesses inicia los procesos distribuidos en todos los nodos Raft.
// Este método ejecuta el comando que arranca el servicio Raft en cada nodo de forma remota
// y actualiza el estado de conexión de cada nodo.
func (cfg *configDespliegue) startDistributedProcesses() {
	for i, endPoint := range cfg.nodosRaft {
		// Ejecuta el comando para iniciar el proceso Raft en el nodo remoto.
		// Se utiliza `ExecMutipleHosts` para ejecutar el comando en la máquina correspondiente.
		// El comando incluye:
		// - `EXECREPLICACMD`: Comando base para ejecutar el proceso Raft.
		// - El índice del nodo (`i`) como argumento.
		// - La lista de nodos en formato cadena.
		despliegue.ExecMutipleHosts(
			EXECREPLICACMD+" "+strconv.Itoa(i)+" "+
				rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

		// Marca el nodo como conectado.
		cfg.conectados[i] = true
	}

	// Introduce un retraso para asegurarse de que los procesos se inicialicen correctamente.
	time.Sleep(5000 * time.Millisecond)
}

// startNode inicia un proceso distribuido en un nodo Raft específico.
// Este método ejecuta el comando correspondiente en el nodo indicado para inicializar su servicio Raft
// y actualiza su estado como conectado.
func (cfg *configDespliegue) startNode(index int) {
	// Verificar que el índice del nodo esté dentro del rango válido.
	// Si no lo está, se detiene la ejecución del test y se muestra un mensaje de error.
	if index < 0 || index >= len(cfg.nodosRaft) {
		cfg.t.Fatalf("Nodo ID %d fuera de rango, no se puede iniciar.", index)
		return
	}

	// Obtener la información del nodo (host y puerto) desde la configuración.
	endPoint := cfg.nodosRaft[index]

	// Ejecutar el comando remoto para iniciar el proceso Raft en el nodo.
	// El comando incluye:
	// - `EXECREPLICACMD`: Comando base para ejecutar el servicio.
	// - El índice del nodo como argumento.
	// - La lista de nodos en formato cadena.
	despliegue.ExecMutipleHosts(
		EXECREPLICACMD+" "+strconv.Itoa(index)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
		[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)

	// Actualizar el estado del nodo como conectado.
	cfg.conectados[index] = true

	// Introducir un pequeño retraso para dar tiempo al nodo a inicializarse completamente.
	time.Sleep(2000 * time.Millisecond)
}

// stopDistributedProcesses detiene todos los procesos distribuidos en los nodos Raft.
// Para cada nodo conectado, envía una llamada RPC para detener su ejecución.
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio // Estructura vacía utilizada como parámetro de entrada y salida para la RPC.

	// Iterar sobre todos los nodos en la configuración.
	for i, endPoint := range cfg.nodosRaft {
		// Verificar si el nodo está marcado como conectado.
		if cfg.conectados[i] {
			// Enviar una llamada RPC al nodo para detener su ejecución.
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
			// Verificar si hubo un error en la llamada RPC y manejarlo si es necesario.
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}

	// Introducir un retraso para asegurar que los procesos hayan sido detenidos completamente.
	time.Sleep(5 * time.Second)
}

// verifyState verifica el estado remoto de un nodo específico comparándolo con los valores esperados.
// Si el estado real no coincide con el esperado, se registra un error en el test.
func (cfg *configDespliegue) verifyState(node int, predictedTerm int, predictedLeadership bool, predictedLeader int) {
	// Obtener el estado remoto real del nodo especificado.
	realNode, realTerm, realLeadership, realLeader := cfg.obtenerEstadoRemoto(node)

	// Registrar el estado actual del nodo en los logs del test para referencia.
	cfg.t.Log("Estado replica 0: ", realNode, realTerm, realLeadership, realLeader, "\n")

	// Comparar los valores reales del nodo con los valores esperados.
	if realNode != node || realTerm != predictedTerm ||
		realLeadership != predictedLeadership || realLeader != predictedLeader {
		// Si alguno de los valores no coincide, marcar el test como fallido.
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			node, cfg.t.Name())
	}
}

// submitOperation envía una operación al nodo líder especificado y espera una respuesta.
// Devuelve los detalles de la operación: índice de registro, mandato, si es líder, ID del líder y el valor devuelto.
func (cfg *configDespliegue) submitOperation(operation string, clave string, value string, leader int) (int, int, bool, int, string) {
	// Imprimir información sobre la operación que se está sometiendo.
	fmt.Printf("Sometiendo operacion %v, con clave %v y valor %v\n", operation, clave, value)

	var reply raft.ResultadoRemoto

	// Llamar al método remoto para someter la operación en el nodo líder.
	err := cfg.nodosRaft[leader].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{Operacion: operation, Clave: clave, Valor: value},
		&reply, TimeoutRpc*time.Millisecond)

	// Si ocurre un error en la llamada, devolver valores por defecto indicando fallo.
	if err != nil {
		return -1, -1, false, -1, ""
	}

	// Retornar los resultados obtenidos de la operación: índice, mandato, estado de liderazgo, ID del líder y valor devuelto.
	return reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver
}

// commitOperation somete una operación y verifica que se haya comprometido correctamente.
// Si la operación no se compromete, el test fallará.
func (cfg *configDespliegue) commitOperation(operation string, key string, value string, index int, leader int) {
	// Realiza la operación llamando a submitOperation y obtiene los resultados.
	indexReply, _, _, _, valueReply := cfg.submitOperation(operation, key, value, leader)

	// Caso para operaciones de escritura ("escribir").
	if operation == "escribir" {
		// Verifica que el índice y el valor devuelto coincidan con lo esperado.
		if indexReply != index || valueReply != "Se ha escrito en RAM" {
			cfg.t.Fatalf("Operacion %v, con key %v y value %v, no lograda. Log esperado: %v, Devuelto: %v\n",
				operation, key, value, "Se ha escrito en RAM", valueReply)
		}
		// Si la operación es exitosa, imprime un mensaje de confirmación.
		fmt.Printf("La operation %v, con key %v y value %v ha sido comprometida correctamente. Resultado: %v\n",
			operation, key, value, valueReply)

		// Caso para operaciones de lectura ("leer").
	} else if operation == "leer" {
		// Si el valor no se encuentra en la RAM, imprime un mensaje indicando la ausencia.
		if valueReply == "Clave no encontrada en RAM" {
			fmt.Printf("La operation %v, con key %v no encontró value en RAM. Resultado: %v\n",
				operation, key, valueReply)
			// Si el valor obtenido no coincide con lo esperado, falla el test.
		} else if valueReply != value {
			cfg.t.Fatalf("Operacion %v, con key %v esperaba value %v, pero se recibió: %v\n",
				operation, key, value, valueReply)
			// Si la operación es exitosa, imprime un mensaje de confirmación.
		} else {
			fmt.Printf("La operation %v, con key %v devolvió correctamente el value %v.\n",
				operation, key, valueReply)
		}
		// Caso para operaciones desconocidas.
	} else {
		// Si la operación no es ni "escribir" ni "leer", falla el test indicando operación no reconocida.
		cfg.t.Fatalf("Operacion desconocida: %v\n", operation)
	}
}

// stopNode detiene el proceso de un nodo Raft específico.
// Realiza una llamada RPC al nodo para detener su ejecución y lo marca como desconectado.
func (cfg *configDespliegue) stopNode(node int) {
	// Estructura para almacenar la respuesta del nodo.
	var reply raft.EstadoRemoto
	// Llama al método remoto "ParaNodo" para detener el nodo.
	err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	// Verifica si hubo errores en la llamada RPC y, de ser así, los reporta.
	check.CheckError(err, "Error en llamada RPC Para nodo")
	// Marca el nodo como desconectado en la configuración local.
	cfg.conectados[node] = false
	// Agrega un pequeño retraso para asegurar que el nodo se detenga completamente.
	time.Sleep(1 * time.Second)
}

// obtainLog recupera información del log replicado y el mandato de un nodo específico.
// Devuelve el índice del último log replicado y el mandato actual del nodo.
func (cfg *configDespliegue) obtainLog(node int) (int, int) {
	// Estructura para almacenar la respuesta del nodo.
	var reply raft.EstadoEntradas
	// Llama al método remoto "ObtenerEstadoEntradas" para obtener el estado del log del nodo.
	err := cfg.nodosRaft[node].CallTimeout("NodoRaft.ObtenerEstadoEntradas",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	// Verifica si hubo errores en la llamada RPC y, de ser así, los reporta.
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemotoLog")
	// Retorna el índice del log replicado y el mandato del nodo.
	return reply.IndiceReplicado, reply.Mandato
}

// concurrentOperation somete una operación de manera concurrente al líder especificado.
// Valida que la operación se someta correctamente y que los resultados sean consistentes.
func (cfg *configDespliegue) concurrentOperation(leader int, operation raft.TipoOperacion, term int, predictedValue string) int {
	// Estructura para almacenar la respuesta del nodo.
	var reply raft.ResultadoRemoto

	// Realizar la llamada RPC al líder para someter la operación con un tiempo límite.
	err := cfg.nodosRaft[leader].CallTimeout("NodoRaft.SometerOperacionRaft",
		operation, &reply, 10*time.Second)
	// Manejar errores en la llamada RPC, en caso de fallar, reportar y retornar error.
	if err != nil {
		fmt.Printf("Error RPC: SometerOperacionRaft: %v\n", err)
		return 1 // Indica error en la llamada RPC.
	}

	// Validar que el nodo identificado como líder y el mandato coincidan con lo esperado.
	if (reply.IdLider != leader) || (reply.Mandato != term) {
		fmt.Printf("Error: IndiceRegistro: %v, IdLider: %v (esperado: %v), Mandato: %v (esperado: %v)\n",
			reply.IndiceRegistro, reply.IdLider, leader, reply.Mandato, term)
		return 1 // Indica error en el líder o en el mandato.
	}

	// Si la operación es de lectura, validar que el valor retornado sea el esperado.
	if operation.Operacion == "leer" {
		if reply.ValorADevolver != predictedValue {
			fmt.Printf("Error: (valor esperado: %v, valor recibido: %v)\n",
				predictedValue, reply.ValorADevolver)
			return 1 // Indica error en el valor esperado.
		}
	}

	// Confirmar que la operación se sometió exitosamente y reportar los detalles.
	fmt.Printf("Operación sometida correctamente (lider: %v, termino: %v, índice: %v)\n",
		reply.IdLider, reply.Mandato, reply.IndiceRegistro)
	return 0 // Indica éxito.
}

// verifyIndexTermForEachNode verifica que todos los nodos del clúster tienen índices y mandatos consistentes.
func (cfg *configDespliegue) verifyIndexTermForEachNode(index int, term int) int {
	// Crear arreglos para almacenar los índices y mandatos de cada nodo.
	indexs := make([]int, cfg.numReplicas)
	terms := make([]int, cfg.numReplicas)

	// Obtener el índice y mandato actual de cada nodo llamando a su método remoto.
	for i := range cfg.nodosRaft {
		indexs[i], terms[i] = cfg.obtainLog(i)
	}

	// Verificar si el nodo 0 tiene el índice y mandato esperado.
	if indexs[0] != index || terms[0] != term {
		// Si no coinciden, imprimir un mensaje de error y devolver 1.
		fmt.Printf("Error: Estado del nodo 0 inválido (índice=%d, mandato=%d)\n", index, term)
		return 1
	}

	// Comparar los índices y mandatos de los demás nodos con el nodo 0.
	for i := 1; i < cfg.numReplicas; i++ {
		if indexs[0] != indexs[i] || terms[0] != terms[i] {
			// Si algún nodo no coincide, imprimir un mensaje de error y devolver 1.
			fmt.Printf("Error: Estado del nodo %d inválido (índice=%d, mandato=%d)\n", i, indexs[i], terms[i])
			return 1
		}
	}

	// Si todos los nodos tienen índices y mandatos consistentes, imprimir un mensaje de éxito.
	fmt.Println("Todos los logs están sincronizados y son consistentes")
	return 0 // Retornar 0 para indicar éxito.
}
