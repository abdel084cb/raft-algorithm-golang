package main

import (
	"fmt"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"sync"
	"time"
)

const (
	//hosts
	REPLICA1 = "ss-0.ss-service.default.svc.cluster.local:6000"
	REPLICA2 = "ss-1.ss-service.default.svc.cluster.local:6000"
	REPLICA3 = "ss-2.ss-service.default.svc.cluster.local:6000"
)

func main() {
	// Crear canal de resultados de ejecuciones ssh en maquinas remotas
	cfg := makeCfgDespliegue(
		3,
		[]string{REPLICA1, REPLICA2, REPLICA3},
		[]bool{true, true, true})

	fmt.Println("Inicio Tests")
	time.Sleep(10 * time.Second)

	cfg.tresOperacionesComprometidasEstable()
	time.Sleep(1 * time.Second)
	cfg.AcuerdoApesarDeSeguidor()
	time.Sleep(1 * time.Second)
	cfg.SinAcuerdoPorFallos()
	time.Sleep(1 * time.Second)
	cfg.SometerConcurrentementeOperaciones()
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
	conectados  []bool
	numReplicas int
	nodosRaft   []rpctimeout.HostPort
}

// Crear una configuracion de despliegue
func makeCfgDespliegue(n int, nodosraft []string,
	conectados []bool) *configDespliegue {
	cfg := &configDespliegue{}
	cfg.conectados = conectados
	cfg.numReplicas = n
	cfg.nodosRaft = rpctimeout.StringArrayToHostPortArray(nodosraft)

	return cfg
}

func (cfg *configDespliegue) stop() {
	//cfg.stopDistributedProcesses()

	time.Sleep(50 * time.Millisecond)

}

// --------------------------------------------------------------------------
// FUNCIONES DE SUBTESTS

// 3 operaciones comprometidas con situacion estable y sin fallos - 3 NODOS RAFT
func (cfg *configDespliegue) tresOperacionesComprometidasEstable() {
	// A completar ??? (Iniciar procesos distribuidos si es necesario)
	fmt.Println("tresOperacionesComprometidasEstable", ".....................")

	// Verificar que se tiene un líder inicial
	fmt.Printf("Líder inicial\n")
	lider := cfg.pruebaUnLider(3)

	// Someter las operaciones una por una y comprobar los resultados
	time.Sleep(1 * time.Second)
	if res := cfg.someterOperacion(lider, 0, raft.TipoOperacion{Operacion: "escribir", Clave: "1", Valor: "a"}, 1, ""); res != 0 {
		fmt.Println("Error en la operación 1")
		fmt.Println(".............", "tresOperacionesComprometidasEstable", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.someterOperacion(lider, 1, raft.TipoOperacion{Operacion: "leer", Clave: "1", Valor: ""}, 1, "a"); res != 0 {
		fmt.Println("Error en la operación 2")
		fmt.Println(".............", "tresOperacionesComprometidasEstable", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.someterOperacion(lider, 2, raft.TipoOperacion{Operacion: "escribir", Clave: "2", Valor: "a"}, 1, ""); res != 0 {
		fmt.Println("Error en la operación 3")
		fmt.Println(".............", "tresOperacionesComprometidasEstable", "No superado")
		return
	}

	// Esperamos un momento para que las operaciones se comprometan en todos los nodos
	time.Sleep(1 * time.Second)

	// Comprobamos si se han comprometido las tres operaciones (commitIndex debe ser 3)
	if res := cfg.comprobarUltimoIndiceComprometido(3); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 3")
		fmt.Println(".............", "tresOperacionesComprometidasEstable", "No superado")
		return
	}

	// Paramos las réplicas de almacenamiento remoto
	//cfg.stopDistributedProcesses()

	fmt.Println(".............", "tresOperacionesComprometidasEstable", "Superado")

}

// Se consigue acuerdo a pesar de desconexiones de seguidor -- 3 NODOS RAFT
func (cfg *configDespliegue) AcuerdoApesarDeSeguidor() {
	fmt.Println("AcuerdoApesarDeSeguidor", ".....................")

	// Iniciar procesos distribuidos
	//cfg.startDistributedProcesses()

	// Obtener el líder actual
	lider := cfg.pruebaUnLider(3)

	// Someter una operación inicial y comprobar que se compromete en todos los nodos
	if res := cfg.someterOperacion(lider, 3, raft.TipoOperacion{Operacion: "escribir", Clave: "1", Valor: "a"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la tercera operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(4); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 4")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	if res := cfg.someterOperacion(lider, 4, raft.TipoOperacion{Operacion: "escribir", Clave: "2", Valor: "b"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la cuarta operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(5); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 5")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	// Desconectar un seguidor
	seguidorDesconectado := (lider + 1) % 3
	fmt.Printf("Desconectando seguidor %d\n", seguidorDesconectado)
	cfg.desconectarNodo(seguidorDesconectado)
	time.Sleep(5 * time.Second)

	// Comprobar que aún se pueden comprometer operaciones con el líder y el otro seguidor
	if res := cfg.someterOperacion(lider, 5, raft.TipoOperacion{Operacion: "escribir", Clave: "3", Valor: "c"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la quinta operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(6); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 6")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	// Comprobar que aún se pueden comprometer operaciones con el líder y el otro seguidor
	if res := cfg.someterOperacion(lider, 6, raft.TipoOperacion{Operacion: "escribir", Clave: "4", Valor: "d"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la sexta operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(7); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 7")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	// Reconectar el seguidor previamente desconectado
	fmt.Printf("Reconectando seguidor %d\n", seguidorDesconectado)
	//cfg.reconectarNodo(seguidorDesconectado)
	/*
		  for i := 0; i < len(cfg.nodosRaft); i++ {
			  cfg.conectados[i] = true
			}
	*/
	cfg.conectados[seguidorDesconectado] = true
	time.Sleep(5 * time.Second)

	// Someter una nueva operación y comprobar que se compromete en todos los nodos, incluido el reconectado
	if res := cfg.someterOperacion(lider, 7, raft.TipoOperacion{Operacion: "escribir", Clave: "5", Valor: "e"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la septima operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(5 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(8); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 8")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	// Comprobar que aún se pueden comprometer operaciones con el líder y el otro seguidor
	if res := cfg.someterOperacion(lider, 8, raft.TipoOperacion{Operacion: "escribir", Clave: "6", Valor: "f"}, 1, ""); res != 0 {
		fmt.Println("Test fallido en la octava operacion")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	time.Sleep(1 * time.Second)
	if res := cfg.comprobarUltimoIndiceComprometido(9); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 9")
		fmt.Println(".............", "AcuerdoApesarDeSeguidor", "No superado")
		return
	}

	// Parar réplicas almacenamiento en remoto
	//cfg.stopDistributedProcesses() //parametros

	fmt.Println(".............", "AcuerdoApesarDeSeguidor", "Superado")
}

// NO se consigue acuerdo al desconectarse mayoría de seguidores -- 3 NODOS RAFT

func (cfg *configDespliegue) SinAcuerdoPorFallos() {

	fmt.Println("SinAcuerdoPorFallos", ".....................")

	// Paso 1: Obtener un líder
	//time.Sleep(3 * time.Second)
	fmt.Printf("Líder inicial\n")
	lider := cfg.pruebaUnLider(3)

	// Paso 2: Someter una operación inicial y comprobar que se compromete en todos los nodos
	res := cfg.someterOperacion(lider, 9, raft.TipoOperacion{Operacion: "escribir", Clave: "1", Valor: "a"}, 1, "")
	time.Sleep(1 * time.Second)
	if res == 1 {
		fmt.Println("Test fallido en la novena operacion")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}
	res = cfg.comprobarUltimoIndiceComprometido(10)
	if res == 1 {
		fmt.Println("Test fallido en la verificación del índice comprometido 10")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}

	// Paso 3: Desconectar 2 nodos (mayoría)
	for i := 0; i < len(cfg.nodosRaft); i++ {
		if i != lider { // Mantener solo al líder conectado
			cfg.desconectarNodo(i)
		}
	}
	time.Sleep(5 * time.Second)
	fmt.Println("SinAcuerdoPorFallos: Desconectados dos nodos (mayoría)")

	// Intentar someter operaciones mientras no hay quórum
	res = cfg.someterOperacion(lider, 10, raft.TipoOperacion{Operacion: "fallo", Clave: "2", Valor: "fallo"}, 1, "")
	time.Sleep(1 * time.Second)
	if res == 1 {
		fmt.Println("Test fallido en la decima operacion")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}
	res = cfg.comprobarUltimoIndiceComprometido(11)
	if res == 1 {
		fmt.Println("Test fallido en la verificación del índice comprometido 11")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}

	// Paso 4: Reconectar los nodos desconectados
	for i := 0; i < len(cfg.nodosRaft); i++ {
		if i != lider {
			cfg.conectados[i] = true
		}
	}
	time.Sleep(15 * time.Second)
	fmt.Println("SinAcuerdoPorFallos: Reconectados los nodos desconectados")

	time.Sleep(5 * time.Second)
	// Paso 5: Comprobar nuevo líder
	nuevoLider := cfg.pruebaUnLider(3)
	fmt.Printf("Nuevo líder elegido tras reconexión: nodo %d\n", nuevoLider)

	// Someter operaciones adicionales y verificar
	res = cfg.someterOperacion(lider, 11, raft.TipoOperacion{Operacion: "escribir", Clave: "4", Valor: "d"}, 1, "")
	time.Sleep(1 * time.Second)
	if res == 1 {
		fmt.Println("Test fallido en la undecima operacion")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}

	res = cfg.someterOperacion(lider, 12, raft.TipoOperacion{Operacion: "escribir", Clave: "5", Valor: "e"}, 1, "")
	time.Sleep(1 * time.Second)
	if res == 1 {
		fmt.Println("Test fallido en la duodecima operacion")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}

	res = cfg.comprobarUltimoIndiceComprometido(13)
	if res == 1 {
		fmt.Println("Test fallido en la verificación del índice comprometido 13")
		fmt.Println(".............", "SinAcuerdoPorFallos", "No superado")
		return
	}

	// Parar réplicas almacenamiento en remoto
	//cfg.stopDistributedProcesses()

	fmt.Println(".............", "SinAcuerdoPorFallos", "Superado")
}

// Se somete 5 operaciones de forma concurrente -- 3 NODOS RAFT
func (cfg *configDespliegue) SometerConcurrentementeOperaciones() {
	// Paso 1: Identificar el líder actual
	fmt.Println("Obteniendo el líder actual...")
	lider := cfg.pruebaUnLider(cfg.numReplicas)
	fmt.Printf("Líder identificado: Nodo %d\n", lider)

	// Paso 2: Definir y someter 5 operaciones concurrentes
	numOperaciones := 5
	var wg sync.WaitGroup
	wg.Add(numOperaciones)

	fmt.Println("Sometiendo operaciones concurrentes al líder...")
	errores := 0

	for i := 0; i < numOperaciones; i++ {
		go func(opIndex int) {
			defer wg.Done()
			operacion := raft.TipoOperacion{
				Operacion: "escribir",
				Clave:     fmt.Sprintf("%d", opIndex),
				Valor:     "a",
			}
			res := cfg.someterOperacionConcurrente(lider, operacion, 1, "")
			if res != 0 {
				fmt.Printf("Error al someter la operación %d\n", opIndex)
				errores++
			}
		}(i)
	}

	// Esperar a que todas las operaciones se sometan
	wg.Wait()
	if errores > 0 {
		fmt.Printf("Fallaron %d operaciones al someterse concurrentemente.\n", errores)
		fmt.Println(".............", "SometerConcurrentementeOperaciones", "No superado")
		return
	}
	fmt.Println("Operaciones concurrentes sometidas exitosamente.")

	// Paso 3: Comprobar consistencia del clúster
	time.Sleep(3 * time.Second)
	fmt.Println("Comprobando consistencia del clúster...")
	if res := cfg.comprobarUltimoIndiceComprometido(18); res != 0 {
		fmt.Println("Test fallido en la verificación del índice comprometido 18")
		fmt.Println(".............", "SometerConcurrentementeOperaciones", "No superado")
		return
	}

	// Verificar si los logs de todos los nodos son consistentes
	fmt.Println("Verificando consistencia de logs...")
	if res := cfg.verificarLogsYLider(18, 1); res != 0 {
		fmt.Println("Los logs de los nodos no son consistentes.")
		fmt.Println(".............", "SometerConcurrentementeOperaciones", "No superado")
		return
	}

	// Parar réplicas almacenamiento en remoto
	cfg.stopDistributedProcesses()

	fmt.Println(".............", "SometerConcurrentementeOperaciones", "Superado")
}

// --------------------------------------------------------------------------
// FUNCIONES DE APOYO
// Comprobar que hay un solo lider
// probar varias veces si se necesitan reelecciones
func (cfg *configDespliegue) pruebaUnLider(numreplicas int) int {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(1000 * time.Millisecond)
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
				fmt.Println("mandato %d tiene %d (>1) lideres",
					mandato, len(lideres))
				return -1
			}
			if mandato > ultimoMandatoConLider {
				ultimoMandatoConLider = mandato
			}
		}

		if len(mapaLideres) != 0 {

			return mapaLideres[ultimoMandatoConLider][0] // Termina

		}
	}
	fmt.Println("un lider esperado, ninguno obtenido")

	return -1 // Termina
}

// FUNCIONES DE APOYO
// Comprobar que desconecta un nodo
func (cfg *configDespliegue) desconectarNodo(idNodo int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[idNodo].CallTimeout("NodoRaft.ParaNodo",
		raft.Vacio{}, &reply, 5000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC Para nodo")
	cfg.conectados[idNodo] = false
}

func (cfg *configDespliegue) obtenerEstadoRemoto(
	indiceNodo int) (int, int, bool, int) {
	var reply raft.EstadoRemoto
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodo",
		raft.Vacio{}, &reply, 10000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemoto")

	return reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider
}

func (cfg *configDespliegue) obtenerEstadoRemotoLog(
	indiceNodo int) (int, int) {
	var reply raft.EstadoLog
	err := cfg.nodosRaft[indiceNodo].CallTimeout("NodoRaft.ObtenerEstadoNodoLog",
		raft.Vacio{}, &reply, 5000*time.Millisecond)
	check.CheckError(err, "Error en llamada RPC ObtenerEstadoRemotoLog")
	return reply.Indice, reply.Mandato
}

// start  gestor de vistas; mapa de replicas y maquinas donde ubicarlos;
// y lista clientes (host:puerto)
func (cfg *configDespliegue) stopDistributedProcesses() {
	var reply raft.Vacio

	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			err := endPoint.CallTimeout("NodoRaft.ParaNodo",
				raft.Vacio{}, &reply, 5000*time.Millisecond)
			check.CheckError(err, "Error en llamada RPC Para nodo")
		}
	}
}

// Comprobar estado remoto de un nodo con respecto a un estado prefijado
func (cfg *configDespliegue) comprobarEstadoRemoto(idNodoDeseado int,
	mandatoDeseado int, esLiderDeseado bool, IdLiderDeseado int) {
	idNodo, mandato, esLider, idLider := cfg.obtenerEstadoRemoto(idNodoDeseado)

	fmt.Println("Estado replica 0: ", idNodo, mandato, esLider, idLider, "\n")

	if idNodo != idNodoDeseado || mandato != mandatoDeseado ||
		esLider != esLiderDeseado || idLider != IdLiderDeseado {
		fmt.Println("Estado incorrecto en replica %d en subtest",
			idNodoDeseado)
		return
	}

}

func (cfg *configDespliegue) someterOperacion(lider int, numOperacion int, operacion raft.TipoOperacion, mandato int, valorEsperado string) int {
	var reply raft.ResultadoRemoto
	err := cfg.nodosRaft[lider].CallTimeout("NodoRaft.SometerOperacionRaft",
		operacion, &reply, 8000*time.Millisecond)
	if err != nil {
		fmt.Println("Error en llamada RPC Someter Operacion:", err)
		return 1 // Retornar 1 en caso de error de RPC
	}

	if reply.IndiceRegistro != numOperacion || reply.IdLider != lider || reply.Mandato != mandato {
		fmt.Printf("Error al someter la operacion: reply.IndiceRegistro: %v, numOperacion: %v, reply.IdLider: %v, lider: %v, reply.Mandato: %v, mandato: %v\n",
			reply.IndiceRegistro, numOperacion, reply.IdLider, lider, reply.Mandato, mandato)
		return 1 // Retornar 1 si la validación de los datos no coincide
	}

	if operacion.Operacion == "leer" {
		if reply.ValorADevolver != valorEsperado {
			fmt.Printf("Error, el valor devuelto no es el esperado, valor devuelto: %v, valor esperado: %v\n", reply.ValorADevolver, valorEsperado)
			return 1 // Retornar 1 si el valor devuelto no es el esperado
		}
	}

	// Si todo ha ido bien, retornar 0
	fmt.Println("Sometida operación a líder", reply.IdLider, "con mandato", reply.Mandato, "y con el índice", reply.IndiceRegistro)
	return 0 // Retornar 0 en caso de éxito
}

func (cfg *configDespliegue) comprobarUltimoIndiceComprometido(indiceEsperado int) int {
	for i, endPoint := range cfg.nodosRaft {
		if cfg.conectados[i] {
			var reply int
			// Llamar al método remoto para obtener el último índice comprometido
			err := endPoint.CallTimeout("NodoRaft.ObtenerIndiceComprometidoRaft",
				raft.Vacio{}, &reply, 15000*time.Millisecond)
			if err != nil {
				fmt.Printf("Error en llamada RPC ObtenerIndiceComprometido para nodo %d: %v\n", i, err)
				return 1
			}
			// Verificar si el índice comprometido coincide con el esperado
			if reply != indiceEsperado {
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
		operacion, &reply, 12000*time.Millisecond)
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
		indexs[i], terms[i] = cfg.obtenerEstadoRemotoLog(i)
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
