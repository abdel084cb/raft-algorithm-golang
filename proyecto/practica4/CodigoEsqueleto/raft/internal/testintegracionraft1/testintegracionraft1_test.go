package testintegracionraft1

import (
	"fmt"
	"raft/internal/comun/check"

	//"log"
	//"crypto/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/despliegue"
	"raft/internal/raft"
)

const TimeoutRpc = 200

const (
	//hosts
	MAQUINA1 = "192.168.3.3"
	MAQUINA2 = "192.168.3.3"
	MAQUINA3 = "192.168.3.3"

	//puertos
	PUERTOREPLICA1 = "31050"
	PUERTOREPLICA2 = "31051"
	PUERTOREPLICA3 = "31052"

	//nodos replicas
	REPLICA1 = MAQUINA1 + ":" + PUERTOREPLICA1
	REPLICA2 = MAQUINA2 + ":" + PUERTOREPLICA2
	REPLICA3 = MAQUINA3 + ":" + PUERTOREPLICA3

	// paquete main de ejecutables relativos a PATH previo
	EXECREPLICA = "cmd/srvraft/main.go"

	// comandos completo a ejecutar en máquinas remota con ssh. Ejemplo :
	// 				cd $HOME/raft; go run cmd/srvraft/main.go 127.0.0.1:29001

	// Ubicar, en esta constante, nombre de fichero de vuestra clave privada local
	// emparejada con la clave pública en authorized_keys de máquinas remotas

	PRIVKEYFILE = "id_rsa"
)

// PATH de los ejecutables de modulo golang de servicio Raft
var PATH string = filepath.Join(os.Getenv("HOME"), "practica-4-SDIST", "proyecto", "practica4", "CodigoEsqueleto", "raft")

// go run cmd/srvraft/main.go 0 127.0.0.1:29001 127.0.0.1:29002 127.0.0.1:29003
var EXECREPLICACMD string = "cd " + PATH + "; go run " + EXECREPLICA

// TEST primer rango
func TestPrimerasPruebas(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Run test sequence

	// Test1: soloArranqueYparadaTest1 verifica que no hay ningún líder.
	t.Run("T1:soloArranqueYparada",
		func(t *testing.T) { cfg.soloArranqueYparadaTest1(t) })

	time.Sleep(1 * time.Second)

	// Test2: elegirPrimerLiderTest2 verifica que se elige un líder correctamente en el primer intento.
	t.Run("T2:ElegirPrimerLider",
		func(t *testing.T) { cfg.elegirPrimerLiderTest2(t) })

	time.Sleep(1 * time.Second)

	// Test3: falloAnteriorElegirNuevoLiderTest3 verifica que se elige un nuevo líder correctamente después de un fallo del líder anterior.
	t.Run("T3:FalloAnteriorElegirNuevoLider",
		func(t *testing.T) { cfg.falloAnteriorElegirNuevoLiderTest3(t) })

	time.Sleep(1 * time.Second)

	// Test4: tresOperacionesComprometidasEstable verifica que tres operaciones se comprometen correctamente en una configuración estable.
	t.Run("T4:tresOperacionesComprometidasEstable",
		func(t *testing.T) { cfg.tresOperacionesComprometidasEstable(t) })

	time.Sleep(1 * time.Second)

}

// TEST primer rango
func TestAcuerdosConFallos(t *testing.T) { // (m *testing.M) {
	// <setup code>
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(t,
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	// tear down code
	// eliminar procesos en máquinas remotas
	defer cfg.stop()

	// Test5: Se consigue acuerdo a pesar de desconexiones de seguidor
	t.Run("T5:AcuerdoAPesarDeDesconexionesDeSeguidor ",
		func(t *testing.T) { cfg.AcuerdoApesarDeSeguidor(t) })

	t.Run("T5:SinAcuerdoPorFallos ",
		func(t *testing.T) { cfg.SinAcuerdoPorFallos(t) })

	t.Run("T5:SometerConcurrentementeOperaciones ",
		func(t *testing.T) { cfg.SometerConcurrentementeOperaciones(t) })

}

// ---------------------------------------------------------------------
//
// Canal de resultados de ejecución de comandos ssh remotos
type canalResultados chan string

func (cr canalResultados) stop() {
	close(cr)

	// Leer las salidas obtenidos de los comandos ssh ejecutados
	for s := range cr {
		fmt.Println(s)
	}
}

// ---------------------------------------------------------------------
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

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar réplicas
	cfg.startDistributedProcesses()

	// Obtener líder inicial
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Desconectar un nodo seguidor
	nodoDesconectado := (idLider + 1) % 3
	cfg.desconectarNodo(nodoDesconectado)
	fmt.Printf("Nodo %d desconectado\n", nodoDesconectado)

	// Comprometer operaciones con un nodo desconectado
	cfg.comprometerOperacion("escribir", "clave1", "valor1", 0, idLider)
	time.Sleep(5 * time.Second)
	cfg.comprometerOperacion("escribir", "clave2", "valor2", 1, idLider)

	// Reconectar nodo desconectado
	fmt.Printf("Reconectando nodo %d\n", nodoDesconectado)
	cfg.startDistributedProcesses()

	// Verificar el log en todos los nodos
	if res := cfg.verificarLogsYLider(2, 2); res != 0 {
		t.Fatalf("Los logs no coinciden después de la reconexión")
	}

	fmt.Println(".............", t.Name(), "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT
func (cfg *configDespliegue) SinAcuerdoPorFallos(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar réplicas
	cfg.startDistributedProcesses()

	// Obtener líder inicial
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Desconectar dos seguidores (mayoría desconectada)
	for i := 0; i < 3; i++ {
		if i != idLider {
			cfg.desconectarNodo(i)
		}
	}

	// Intentar someter operaciones (debe fallar)
	fmt.Println("Intentando someter operación sin mayoría...")
	indice, _, _, _, valueReply := cfg.someterOperacion("escribir", "clave3", "valor3", idLider)
	if indice != -1 || valueReply == "Se ha escrito en RAM" {
		t.Fatalf("Error: operación no debería haberse comprometido sin mayoría, pero se recibió índice %d y valor %s", indice, valueReply)
	}

	fmt.Println("Operación correctamente rechazada sin mayoría.")
	fmt.Println(".............", t.Name(), "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones(t *testing.T) {
	fmt.Println(t.Name(), ".....................")

	// Iniciar réplicas
	cfg.startDistributedProcesses()

	// Obtener líder inicial
	idLider := cfg.pruebaUnLider(3)
	fmt.Printf("Líder inicial: %d\n", idLider)

	// Someter operaciones concurrentes
	operaciones := []raft.TipoOperacion{
		{Operacion: "escribir", Clave: "clave1", Valor: "valor1"},
		{Operacion: "escribir", Clave: "clave2", Valor: "valor2"},
		{Operacion: "escribir", Clave: "clave3", Valor: "valor3"},
		{Operacion: "escribir", Clave: "clave4", Valor: "valor4"},
		{Operacion: "escribir", Clave: "clave5", Valor: "valor5"},
	}

	// Ejecutar operaciones concurrentemente
	for i, op := range operaciones {
		go func(op raft.TipoOperacion, indice int) {
			cfg.someterOperacionConcurrente(idLider, op, 1, "Se ha escrito en RAM")
		}(op, i)
	}

	// Esperar un momento para estabilizar
	time.Sleep(2 * time.Second)

	// Verificar que todos los nodos tienen logs consistentes
	if res := cfg.verificarLogsYLider(5, 1); res != 0 {
		t.Fatalf("Error: logs inconsistentes después de operaciones concurrentes")
	}

	fmt.Println(".............", t.Name(), "Superado")
}

// Test1: soloArranqueYparadaTest1 verifica que no hay ningún líder si el servidor no ha recibido latidos.
func (cfg *configDespliegue) soloArranqueYparadaTest1(t *testing.T) {
	//t.Skip("SKIPPED soloArranqueYparadaTest1")

	fmt.Println(t.Name(), ".....................")

	cfg.t = t // Actualizar la estructura de datos de tests para errores

	// Poner en marcha replicas
	cfg.startDistributedProcesses()

	// Comprobar estado replica 0
	cfg.comprobarEstadoRemoto(0, 0, false, -1)

	// Comprobar estado replica 1
	cfg.comprobarEstadoRemoto(1, 0, false, -1)

	// Comprobar estado replica 2
	cfg.comprobarEstadoRemoto(2, 0, false, -1)

	// Parar réplicas
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// Test2: elegirPrimerLiderTest2 verifica que se elige un líder correctamente en el primer intento.
func (cfg *configDespliegue) elegirPrimerLiderTest2(t *testing.T) {
	//t.Skip("SKIPPED ElegirPrimerLiderTest2")

	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas
	cfg.startDistributedProcesses()
	time.Sleep(2 * time.Second)
	fmt.Printf("Probando lider en curso\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas
	cfg.stopDistributedProcesses() // Parametros

	fmt.Println(".............", t.Name(), "Superado")
}

// Fallo de un primer lider y reeleccion de uno nuevo - 3 NODOS RAFT
func (cfg *configDespliegue) falloAnteriorElegirNuevoLiderTest3(t *testing.T) {
	//t.Skip("SKIPPED falloAnteriorElegirNuevoLiderTest3")
	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas
	cfg.startDistributedProcesses()

	fmt.Printf("Lider inicial\n")
	time.Sleep(2 * time.Second)

	liderActual := cfg.pruebaUnLider(3)

	// Desconectar un lider
	cfg.desconectarLider(liderActual)

	time.Sleep(10 * time.Second)

	fmt.Printf("Comprobar nuevo lider\n")
	cfg.pruebaUnLider(3)

	// Parar réplicas
	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable(t *testing.T) {
	//t.Skip("SKIPPED tresOperacionesComprometidasEstable")
	fmt.Println(t.Name(), ".....................")

	// Poner en marcha replicas
	cfg.startDistributedProcesses()
	time.Sleep(2 * time.Second)
	fmt.Printf("Comprobando lider\n")
	idLider := cfg.pruebaUnLider(3)

	// Se someten operaciones
	cfg.comprometerOperacion("leer", "clave0", "valor0", 0, idLider)
	time.Sleep(5 * time.Second)
	cfg.comprometerOperacion("escribir", "clave1", "valor1", 1, idLider)
	time.Sleep(5 * time.Second)
	cfg.comprometerOperacion("escribir", "clave2", "valor2", 2, idLider)

	cfg.stopDistributedProcesses()

	fmt.Println(".............", t.Name(), "Superado")
}

// --------------------------------------------------------------------------//
//                          FUNCIONES DE APOYO								 //
// --------------------------------------------------------------------------//

// pruebaUnLider verifica que hay un solo líder en el clúster Raft.
// Intenta varias veces para manejar posibles reelecciones.
// Devuelve el ID del líder.
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(500 * time.Millisecond)
		mapaLideres := make(map[int][]int)
		for i := 0; i < numreplicas; i++ {
			if cfg.conectados[i] {
				if _, mandato, eslider, _ := cfg.obtenerEstadoRemoto(i); eslider {
					mapaLideres[mandato] = append(mapaLideres[mandato], i)
				}
			}
		}

		ultimoMandatoConLider := -1
		for mandato, lideres := range mapaLideres {
			if len(lideres) > 1 {
				cfg.t.Fatalf("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	cfg.t.Fatalf("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

// desconectarLider desconecta el nodo líder especificado.
// Envía una llamada RPC para detener el nodo líder.
func (cfg *configDespliegue) desconectarLider(idLider int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
	cfg.conectados[idLider] = false
}

// obtenerEstadoRemoto obtiene el estado de un nodo Raft remoto.
// Devuelve el ID del nodo, el mandato actual, si es líder y el ID del líder.
func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

// startDistributedProcesses inicia todos los procesos distribuidos en los nodos Raft.
// Ejecuta el comando de réplica en cada nodo y marca los nodos como conectados.
func (cfg *configDespliegue) startDistributedProcesses() {
	for i, endPoint := range cfg.nodosRaft {
		despliegue.ExecMutipleHosts(EXECREPLICACMD+
			" "+strconv.Itoa(i)+" "+
			rpctimeout.HostPortArrayToString(cfg.nodosRaft),
			[]string{endPoint.Host()}, cfg.cr, PRIVKEYFILE)
		cfg.conectados[i] = true
	}

	time.Sleep(6000 * time.Millisecond)
}

// stopDistributedProcesses detiene todos los procesos distribuidos en los nodos Raft.
// Envía una llamada RPC para detener cada nodo que esté conectado.
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado ya indicado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	cfg.t.Log("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		cfg.t.Fatalf("Estado incorrecto en replica %d en subtest %s",
			idNodoDeseado, cfg.t.Name())
	}

}

// someterOperacion envía una operación al nodo líder especificado y espera una respuesta.
// Devuelve el índice de registro, mandato, si es líder, id del líder y el valor devuelto.
func (cfg *configDespliegue) someterOperacion(operacion string, clave string, value string, idLider int) (int, int, bool, int, string) {
	fmt.Printf("Sometiendo operacion %v, con clave %v y valor %v\n", operacion, clave, value)
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[idLider].CallTimeout("NodoRaft.SometerOperacionRaft",
		raft.TipoOperacion{Operacion: operacion, Clave: clave, Valor: value},
		&reply, TimeoutRpc*time.Millisecond)
	if err != nil {
		cfg.t.Fatalf("Error en llamada RPC someterOperacion")
	}
	return reply.IndiceRegistro, reply.Mandato, reply.EsLider, reply.IdLider, reply.ValorADevolver
}

// comprometerOperacion somete una operación y verifica que se haya comprometido correctamente.
// Si la operación no se compromete, falla el test.
func (cfg *configDespliegue) comprometerOperacion(operacion string, clave string, valor string,
	indice int, idLider int) {
	indexReply, _, _, _, valueReply := cfg.someterOperacion(operacion, clave, valor, idLider)

	if operacion == "escribir" {
		// Verificar respuesta para la operación "escribir"
		if indexReply != indice || valueReply != "Se ha escrito en RAM" {
			cfg.t.Fatalf("Operacion %v, con clave %v y valor %v, no lograda. Log esperado: %v, Devuelto: %v\n",
				operacion, clave, valor, "Se ha escrito en RAM", valueReply)
		}
		fmt.Printf("La operacion %v, con clave %v y valor %v ha sido comprometida correctamente. Resultado: %v\n",
			operacion, clave, valor, valueReply)
	} else if operacion == "leer" {
		// Verificar respuesta para la operación "leer"
		if valueReply == "Clave no encontrada en RAM" {
			fmt.Printf("La operacion %v, con clave %v no encontró valor en RAM. Resultado: %v\n",
				operacion, clave, valueReply)
		} else if valueReply != valor {
			cfg.t.Fatalf("Operacion %v, con clave %v esperaba valor %v, pero se recibió: %v\n",
				operacion, clave, valor, valueReply)
		} else {
			fmt.Printf("La operacion %v, con clave %v devolvió correctamente el valor %v.\n",
				operacion, clave, valueReply)
		}
	} else {
		cfg.t.Fatalf("Operacion desconocida: %v\n", operacion)
	}
}

func (cfg *configDespliegue) desconectarNodo(nodo int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
	cfg.conectados[nodo] = false
}

func (cfg *configDespliegue) obtenerLog(
	nodo int) (int, int) {
	var reply raft.EstadoEntradas
	err := cfg.nodosRaft[nodo].CallTimeout("NodoRaft.ObtenerEstadoNodoLog",
		raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemotoLog")
	return reply.IndiceReplicado, reply.Mandato
}

func (cfg *configDespliegue) obtenerCommit(indiceEsperado int) int {
	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			var reply raft.EstadoEntradas
			// Llamar al método remoto para obtener el último índice comprometido
			err := endPoint.CallTimeout("NodoRaft.ObtenerEstadoEntradas",
				raft.Vacio{}, &reply, TimeoutRpc*time.Millisecond)
			if err != nil {
				fmt.Printf("Error en llamada RPC ObtenerEstadoEntrasdas para nodo %d: %v\n", i, err)
				return 1
			}
			// Verificar si el índice comprometido coincide con el esperado
			if reply.IndiceComprometido != indiceEsperado {
				fmt.Printf("Error: Nodo %d tiene CommitIndex %d, pero se esperaba %d\n", i, reply, indiceEsperado)
				return 1
			}
		}
	}

	fmt.Printf("Todos los nodos tienen CommitIndex = %d como esperado\n", indiceEsperado)
	return 0
}

func (cfg *configDespliegue) someterOperacionConcurrente(lider int, operacion raft.TipoOperacion, mandato int, valorEsperado string) int {
	var reply raft.ResultadoRemoto

	// Realizar la llamada RPC con timeout
	err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		operacion, &reply, TimeoutRpc*time.Millisecond)
	if err != nil {
		fmt.Printf("Error en llamada RPC SometerOperacionRaft: %v\n", err)
		return 1 // Error al realizar la llamada
	}

	// Validar que el líder y el mandato sean correctos
	if (reply.IdLider != lider) || (reply.Mandato != mandato) {
		fmt.Printf("Error al someter la operación. Respuesta inválida:\n")
		fmt.Printf("IndiceRegistro: %v, IdLider: %v (esperado: %v), Mandato: %v (esperado: %v)\n",
			reply.IndiceRegistro, reply.IdLider, lider, reply.Mandato, mandato)
		return 1 // Error en el líder o mandato
	}

	// Validar el valor devuelto en operaciones de lectura
	if operacion.Operacion == "leer" {
		if reply.ValorADevolver != valorEsperado {
			fmt.Printf("Error, valor devuelto no coincide (esperado: %v, recibido: %v)\n",
				valorEsperado, reply.ValorADevolver)
			return 1 // Error en el valor esperado
		}
	}

	// Operación sometida con éxito
	fmt.Printf("Operación sometida correctamente al líder %v con mandato %v en el índice %v\n",
		reply.IdLider, reply.Mandato, reply.IndiceRegistro)
	return 0 // Todo correcto
}

func (cfg *configDespliegue) verificarLogsYLider(index int, term int) int {
	// Verificar índices y mandatos consistentes
	indexs := make([]int, cfg.numReplicas)
	terms := make([]int, cfg.numReplicas)

	for i := range cfg.nodosRaft {
		indexs[i], terms[i] = cfg.obtenerLog(i)
	}

	// Verificar el nodo 0
	if indexs[0] != index || terms[0] != term {
		fmt.Printf("Error: El índice o mandato del nodo 0 es incorrecto (esperado: índice=%d, mandato=%d)\n", index, term)
		return 1
	}

	// Verificar los demás nodos
	for i := 1; i < cfg.numReplicas; i++ {
		if indexs[0] != indexs[i] || terms[0] != terms[i] {
			fmt.Printf("Error: El estado del nodo %d no coincide con el nodo 0 (índice=%d, mandato=%d)\n", i, indexs[i], terms[i])
			return 1
		}
	}

	fmt.Println("Todos los mandatos e índices de los logs están actualizados y son consistentes.")
	return 0
}
