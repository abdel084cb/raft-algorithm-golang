// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)

// type AplicaOperacion

import (
	"fmt"
	"io"
	"log"
	"math/big"
	"os"

	//"math/rand"
	"crypto/rand"
	"sync"
	"time"

	//"net/rpc"
	"raft/internal/comun/rpctimeout"
)

const (
	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = true

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

// Constantes de configuración para los temporizadores y comportamientos del sistema Raft

// intervaloLatidos define el tiempo entre cada envío de latidos (heartbeats) por parte del líder.
const intervaloLatidos = 250 * time.Millisecond

// timeoutMin establece el tiempo mínimo de espera antes de que un nodo seguidor
// considere que no ha recibido un latido o entrada de log, y pase al estado de candidato.
const timeoutMin = 500 * time.Millisecond

// timeoutMax establece el tiempo máximo de espera antes de que un nodo seguidor
// considere que no ha recibido un latido o entrada de log, y pase al estado de candidato.
// El timeout real será aleatorio dentro del rango [timeoutMin, timeoutMax].
const timeoutMax = 1000 * time.Millisecond

// timeoutRpc define el tiempo máximo permitido para una llamada RPC.
const timeoutRpc = 45 * time.Millisecond

// mostrarLatidos es una flag para activar o desactivar la visibilidad de los latidos (heartbeats)
// en los logs de debug.
const mostrarLatidos = false

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {

	// Mutex para proteger acceso a estado compartido
	Mux sync.Mutex

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos []rpctimeout.HostPort

	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	// Se lee cuando se ha recibido un latido o una entrada de log
	AppendEntry chan bool

	// Timer que gestiona tiempo entre latidos, timeouts de candidatos
	// y timeouts de seguidores
	Timer *time.Timer

	// indice de este nodo en campo array "nodos"
	Yo int

	// indice del nodo lider en campo array "nodos"
	IdLider int

	// Estados: StateFollower, StateCandidate y StateLeader
	State string

	// El valor de este canal se utiliza para devolver "Operacion confirmada"
	// o "Fallo en consenso". Esto se envia al metodo someterOperacion despues
	// de enviar la operacion a los nodos y esperar a que se comprometa.
	Return chan string

	// Número de noods que han confirmado que han replicado la entrada
	Confirmaciones int

	CanalAplicar chan AplicaOperacion

	Ram map[string]string

	/* STATE */

	/****************************************************************
	 En todos los servidores:
	****************************************************************/

	// El término actual, que identifica las épocas de elecciones
	CurrentTerm int

	// Guarda a quién votó el nodo en el término actual
	VotedFor int

	// Aquí se almacenan todas las entradas del log
	LogEntries []Entry

	// Este es un valor entero mantenido por todos los nodos (líder y seguidores)
	// que indica el índice más alto del log que se ha comprometido.
	CommitIndex int

	// Este valor indica el índice más alto del log que ha sido aplicado a la máquina de estados del nodo.
	// LastApplied puede estar rezagado con respecto a CommitIndex, ya que los nodos aplican
	//las entradas comprometidas de forma secuencial.
	LastApplied int

	/****************************************************************
	  En el lider:
	****************************************************************/

	// Este es un array que el líder mantiene, donde NextIndex[i] representa
	// el índice de la próxima entrada del registro (log) que el líder
	// intentará enviar al nodo i (seguidor).
	NextIndex []int

	// Este es otro array que el líder mantiene, donde MatchIndex[i] representa
	// el índice más alto del log que el líder sabe que ha sido replicado
	// correctamente en el nodo i.
	MatchIndex []int
}

const (
	StateFollower  = "follower"
	StateCandidate = "candidate"
	StateLeader    = "leader"
)

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion
func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.AppendEntry = make(chan bool)
	nr.Yo = yo
	nr.IdLider = -1
	nr.State = StateFollower
	nr.Return = make(chan string)
	nr.Confirmaciones = 0
	nr.CanalAplicar = canalAplicarOperacion
	nr.Ram = make(map[string]string)
	nr.CurrentTerm = 0
	nr.VotedFor = -1
	nr.LogEntries = []Entry{}
	nr.CommitIndex = -1
	nr.LastApplied = -1
	nr.NextIndex = make([]int, len(nodos))
	nr.MatchIndex = make([]int, len(nodos))

	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
				kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(io.Discard, "", 0)
	}
	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.CurrentTerm
	var esLider bool = nr.Yo == nr.IdLider
	var idLider int = nr.IdLider

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int, bool, int, string) {
	indice := -1                   // Índice de la operación en el registro
	mandato := -1                  // Término actual del nodo
	esLider := nr.Yo == nr.IdLider // Verifica si este nodo es el líder
	idLider := nr.IdLider          // Identificador del nodo líder
	valorADevolver := ""           // Resultado a devolver al cliente

	// Si el nodo es el líder, debe procesar la operación
	if esLider {
		indice = len(nr.LogEntries) // Determina el índice de la nueva entrada
		mandato = nr.CurrentTerm    // Obtiene el término actual
		// Crear la nueva entrada para el log
		entry := Entry{
			Term:      mandato,   // Término actual
			Index:     indice,    // Índice de la nueva entrada
			Operation: operacion, // Operación solicitada por el cliente
		}

		nr.Mux.Lock()
		// Agregar la entrada al registro del nodo
		nr.LogEntries = append(nr.LogEntries, entry)
		nr.Logger.Printf("Nodo %d lider aplicó la entrada al log: Term=%d, Index=%d, Operacion=%s, Clave=%s, Valor=%s", nr.Yo,
			entry.Term, entry.Index, entry.Operation.Operacion, entry.Operation.Clave, entry.Operation.Valor)
		nr.Mux.Unlock()
		// Espera la confirmación de la operación
		valorADevolver = <-nr.Return
	} else {
		// Si no es el líder, devuelve un mensaje indicando que no puede procesar la operación
		valorADevolver = "No es lider"
		// Retorna el ID del líder actual para redirigir al cliente
		idLider = nr.IdLider
	}
	return indice, mandato, esLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	Term    int
	Granted bool
}

// Pre: min <= max
// Post: Genera un valor aleatorio entre [min, max].
func random(min, max time.Duration) time.Duration {
	if max <= min {
		panic("random: max debe ser mayor que min")
	}
	// Calcular el rango en nanosegundos y generar un valor aleatorio dentro de ese rango.
	delta := max - min
	randValue, err := rand.Int(rand.Reader, big.NewInt(int64(delta)))
	if err != nil {
		panic("random: fallo al generar valor aleatorio")
	}
	return min + time.Duration(randValue.Int64())
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {

	// Comprobar si el log está vacío antes de acceder al último índice o término
	var lastLogIndex, lastLogTerm int
	if len(nr.LogEntries) > 0 {
		// Si el log no esta vacío, se obtienen el último índice y término
		lastLogIndex = nr.LogEntries[len(nr.LogEntries)-1].Index
		lastLogTerm = nr.LogEntries[len(nr.LogEntries)-1].Term
	} else {
		// Si el log está vacío, se asignan valores por defecto
		lastLogIndex = -1
		lastLogTerm = 0
	}

	if peticion.Term < nr.CurrentTerm {
		// El término del candidato es menor al término del nodo votante,
		// se rechaza el voto y se le informa del término actual
		nr.Logger.Printf("Nodo %d rechaza voto por término menor al nodo %d (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
		reply.Granted = false
		return nil
	} else if peticion.Term == nr.CurrentTerm && peticion.CandidateID != nr.VotedFor {
		// El nodo actual ya ha votado (tiene el mismo término que el candidato),
		// y además ha votado a otro candidato
		nr.Logger.Printf("Nodo %d rechaza voto al nodo %d, porque ha votado a otro nodo antes (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
		reply.Granted = false
		return nil
	} else if peticion.Term > nr.CurrentTerm {
		// Si el nodo actual no ha votado todavía y ha cumplido todo lo anterior.
		// Entonces considera votar
		if peticion.LastLogTerm < lastLogTerm || (peticion.LastLogTerm == lastLogTerm && peticion.LastLogIndex < lastLogIndex) {
			// LastLogTerm o LastLogIndex del candidato es menor al nodo votante, se rechaza el voto
			nr.Logger.Printf("Nodo %d rechaza voto por log desactualizado al nodo %d (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
			reply.Term = nr.CurrentTerm
			reply.Granted = false
			return nil
		}
		// Votar al candidato si cumple con las condiciones
		nr.Mux.Lock()
		nr.CurrentTerm = peticion.Term
		nr.VotedFor = peticion.CandidateID
		nr.Mux.Unlock()
		nr.cambiarEstado(StateFollower)
		nr.reiniciarTimer(random(timeoutMin, timeoutMax))
		nr.Logger.Printf("Nodo %d vota al nodo %d (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
	}
	reply.Granted = true
	return nil
}

type Entry struct {
	Term      int           // El término en el que se creó esta entrada
	Index     int           // Índice de la entrada en el log
	Operation TipoOperacion // La operación que se almacena en el log
}

type ArgAppendEntries struct {
	Term              int
	LeaderID          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           Entry
	LeaderCommitIndex int
}

type Results struct {
	Term    int
	Success bool
}

// Envia entradas de log al nodo con indice indicado
func (nr *NodoRaft) EnviarEntradasLog(indice int) {

	// Si hay nuevas entradas en el log, se preparan para el envío
	entry := Entry{
		Index:     nr.NextIndex[indice],
		Term:      nr.LogEntries[nr.NextIndex[indice]].Term,
		Operation: nr.LogEntries[nr.NextIndex[indice]].Operation,
	}

	var prevLogIndex, prevLogTerm int
	if nr.NextIndex[indice] > 0 {
		// Si hay una entrada previa válida en el log
		prevLogIndex = nr.NextIndex[indice] - 1
		prevLogTerm = nr.LogEntries[prevLogIndex].Term
	} else {
		// Si no hay una entrada previa (primera entrada en el log)
		prevLogIndex = -1
		prevLogTerm = 0
	}

	// Preparar los argumentos para AppendEntries
	args := &ArgAppendEntries{
		Term:              nr.CurrentTerm,
		LeaderID:          nr.Yo,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entry,
		LeaderCommitIndex: nr.CommitIndex,
	}

	// Ejecutar la llamada RPC
	var results Results
	nr.Logger.Printf("Nodo %d lider enviando entradas de log al nodo %d: PrevLogIndex=%d, PrevLogTerm=%d, EntryIndex=%d",
		nr.Yo, indice, prevLogIndex, prevLogTerm, entry.Index)

	err := nr.Nodos[indice].CallTimeout("NodoRaft.AppendEntries", args, &results, timeoutRpc)

	if err != nil {
		// Si ocurre un error en la comunicación
		nr.Logger.Printf("Nodo %d lider no pudo enviar entradas de log al nodo %d: %v", nr.Yo, indice, err)
		return
	}

	if results.Success {
		// Si el log del seguidor es consistente, actualizar MatchIndex y NextIndex
		nr.Mux.Lock()
		nr.MatchIndex[indice] = nr.NextIndex[indice]
		nr.NextIndex[indice]++
		nr.Logger.Printf("Nodo %d lider actualizó MatchIndex[%d]=%d y NextIndex[%d]=%d (Term %d)",
			nr.Yo, indice, nr.MatchIndex[indice], indice, nr.NextIndex[indice], nr.CurrentTerm)
		nr.Mux.Unlock()

		if nr.MatchIndex[indice] > nr.CommitIndex {
			// Comprobar si hay mayoría para consolidar la entrada
			nr.Mux.Lock()
			nr.Confirmaciones++
			nr.Mux.Unlock()

			if nr.Confirmaciones > len(nr.Nodos)/2 {
				nr.Mux.Lock()
				nr.CommitIndex++
				nr.Confirmaciones = 0 // Reiniciar contador de respuestas
				nr.Mux.Unlock()
				nr.Return <- "Operacion confirmada"
				nr.Logger.Printf("Nodo %d lider consolidó entrada en CommitIndex=%d (Term %d)", nr.Yo, nr.CommitIndex, nr.CurrentTerm)
				// Aplicar entradas comprometidas a la máquina de estados
				for nr.LastApplied < nr.CommitIndex {
					nr.LastApplied++
					nr.CanalAplicar <- AplicaOperacion{
						Indice:    nr.LastApplied,                          // El índice de la operación
						Operacion: nr.LogEntries[nr.LastApplied].Operation, // La operación específica
					}
					entry := nr.LogEntries[nr.LastApplied]
					nr.Logger.Printf("Nodo %d lider aplicó entrada en máquina de estados: Term=%d, Index=%d, Operación=%s", nr.Yo,
						entry.Term, entry.Index, entry.Operation.Operacion)
				}
			}
		}

	} else {
		// Si el log del seguidor no es consistente, decrementar NextIndex para reintentar
		nr.Mux.Lock()
		if nr.NextIndex[indice] > 0 {
			nr.NextIndex[indice]--
		}
		nr.Mux.Unlock()
		nr.Logger.Printf("Nodo %d decreció NextIndex[%d] a %d debido a inconsistencias en el nodo %d (Term %d)",
			nr.Yo, indice, nr.NextIndex[indice], indice, nr.CurrentTerm)
		if nr.NextIndex[indice] > 0 {
			go nr.EnviarEntradasLog(indice)
		}
	}
}

// Decide si enviar entradas de log o simplemente un latido
func (nr *NodoRaft) EnviarAppendEntries(indice int) {
	if len(nr.LogEntries)-1 >= nr.NextIndex[indice] {
		// Si hay nuevas entradas en el log, se envían
		go nr.EnviarEntradasLog(indice)
	} else {
		// En caso contrario, se envía un latido (heartbeat)
		go nr.EnviarLatido(indice)
	}
}

// Función auxiliar para calcular el mínimo entre dos valores
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries, results *Results) error {

	// Caso 1: Se recibe un latido (sin nuevas entradas)
	if args.Entries.Operation.Operacion == "" {
		if args.Term < nr.CurrentTerm {
			// El término del líder que envió el latido es obsoleto, se rechaza
			nr.Logger.Printf("Nodo %d rechaza el latido del nodo %d por término menor (Term %d)", nr.Yo, args.LeaderID, nr.CurrentTerm)
			results.Success = false
			results.Term = nr.CurrentTerm
		} else {
			if mostrarLatidos {
				nr.Logger.Printf("Nodo %d recibe el latido del nodo %d (Term %d)", nr.Yo, args.LeaderID, nr.CurrentTerm)
			}
			// Actualizar término y líder si el término es válido
			nr.Mux.Lock()
			nr.CurrentTerm = args.Term
			nr.IdLider = args.LeaderID
			// Verificar si LeaderCommitIndex indica nuevos compromisos
			if args.LeaderCommitIndex > nr.CommitIndex {
				nr.CommitIndex = min(args.LeaderCommitIndex, len(nr.LogEntries)-1)
				nr.Logger.Printf("Nodo %d actualizó CommitIndex a %d por latido", nr.Yo, nr.CommitIndex)

				// Aplicar entradas comprometidas a la máquina de estados
				for nr.LastApplied < nr.CommitIndex {
					nr.LastApplied++
					nr.CanalAplicar <- AplicaOperacion{
						Indice:    nr.LastApplied,
						Operacion: nr.LogEntries[nr.LastApplied].Operation,
					}
					entry := nr.LogEntries[nr.LastApplied]
					nr.Logger.Printf("Nodo %d aplicó entrada en máquina de estados: Term=%d, Index=%d, Operación=%s", nr.Yo,
						entry.Term, entry.Index, entry.Operation.Operacion)
				}
			}
			nr.Mux.Unlock()
			results.Success = true
			nr.AppendEntry <- true // Notificar que se recibió un latido
		}
		return nil
	}
	// Caso 2: Se reciben nuevas entradas para replicar
	nr.Logger.Printf("Nodo %d recibe entrada de log del nodo %d (Term %d)", nr.Yo, args.LeaderID, nr.CurrentTerm)

	// Verificar si el término del líder es válido
	if args.Term < nr.CurrentTerm {
		nr.Logger.Printf("Nodo %d rechaza entrada de log del nodo %d por término menor (Term %d)", nr.Yo, args.LeaderID, nr.CurrentTerm)
		results.Success = false
		results.Term = nr.CurrentTerm
		return nil
	}

	// Verificar si el log del seguidor coincide con el del líder en PrevLogIndex
	if args.PrevLogIndex >= len(nr.LogEntries) || (args.PrevLogIndex >= 0 && nr.LogEntries[args.PrevLogIndex].Term != args.PrevLogTerm) {
		// La entrada previa no coincide, rechazar la solicitud
		nr.Logger.Printf("Nodo %d rechaza entrada de log por inconsistencia en PrevLogIndex o PrevLogTerm", nr.Yo)
		results.Success = false
		return nil
	}

	// Truncar el log si hay entradas conflictivas
	if args.PrevLogIndex+1 < len(nr.LogEntries) {
		nr.LogEntries = nr.LogEntries[:args.PrevLogIndex+1]
		nr.Logger.Printf("Nodo %d truncó su log hasta el índice %d", nr.Yo, args.PrevLogIndex)
	}

	// Añadir las nuevas entradas al log
	nr.LogEntries = append(nr.LogEntries, args.Entries)
	nr.Logger.Printf("Nodo %d aplicó nueva entrada al log: Term=%d, Index=%d, Operación=%s", nr.Yo,
		args.Entries.Term, args.Entries.Index, args.Entries.Operation.Operacion)

	// Actualizar el CommitIndex
	if args.LeaderCommitIndex > nr.CommitIndex {
		nr.CommitIndex = min(args.LeaderCommitIndex, len(nr.LogEntries)-1)
		nr.Logger.Printf("Nodo %d actualizó CommitIndex a %d", nr.Yo, nr.CommitIndex)

		// Aplicar entradas comprometidas a la máquina de estados
		for nr.LastApplied < nr.CommitIndex {
			nr.LastApplied++
			nr.CanalAplicar <- AplicaOperacion{
				Indice:    nr.LastApplied,                          // El índice de la operación
				Operacion: nr.LogEntries[nr.LastApplied].Operation, // La operación específica
			}
			entry := nr.LogEntries[nr.LastApplied]
			nr.Logger.Printf("Nodo %d aplicó entrada al log: Term=%d, Index=%d, Operación=%s", nr.Yo,
				entry.Term, entry.Index, entry.Operation.Operacion)
		}
	}

	results.Term = nr.CurrentTerm
	results.Success = true
	nr.AppendEntry <- true // Notificar que se recibió una entrada de log
	return nil
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	// Argumentos para la petición del voto
	args.Term = nr.CurrentTerm
	args.CandidateID = nr.Yo

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto", args, &reply, timeoutRpc)

	return err == nil
}

func (nr *NodoRaft) EnviarLatido(indice int) {
	// Validar índices antes de acceder al log
	var prevLogIndex, prevLogTerm int
	var entry Entry

	if len(nr.LogEntries) > 0 && nr.NextIndex[indice] < len(nr.LogEntries) {
		prevLogIndex = nr.NextIndex[indice] - 1
		if prevLogIndex >= 0 {
			prevLogTerm = nr.LogEntries[prevLogIndex].Term
		} else {
			prevLogTerm = 0
		}
		entry = Entry{
			Index:     nr.NextIndex[indice],
			Term:      nr.LogEntries[nr.NextIndex[indice]].Term,
			Operation: TipoOperacion{Operacion: ""}, // Latido vacío
		}
	} else {
		prevLogIndex = -1
		prevLogTerm = 0
		entry = Entry{
			Index:     prevLogIndex,
			Term:      prevLogTerm,
			Operation: TipoOperacion{Operacion: ""}, // Latido vacío
		}
	}

	args := &ArgAppendEntries{
		Term:              nr.CurrentTerm,
		LeaderID:          nr.Yo,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entry,
		LeaderCommitIndex: nr.CommitIndex,
	}
	if mostrarLatidos {
		nr.Logger.Printf("Nodo %d envia latido al nodo %d (Term %d)", nr.Yo, indice, nr.CurrentTerm)
	}
	var reply Results
	err := nr.Nodos[indice].CallTimeout("NodoRaft.AppendEntries", args, &reply, timeoutRpc)

	if err != nil {
		nr.Logger.Printf("Nodo %d fallo al enviar latido al nodo %d: %v", nr.Yo, indice, err)
		return
	}

	if reply.Term > nr.CurrentTerm {
		nr.Logger.Printf("Nodo %d detecta un término mayor y cambia a seguidor (Term %d)", nr.Yo, reply.Term)
		nr.Mux.Lock()
		nr.CurrentTerm = reply.Term
		nr.Mux.Unlock()
		nr.cambiarEstado(StateFollower)
		nr.reiniciarTimer(random(timeoutMin, timeoutMax))
	}
}

// El nodo raft pasa a tener el estado "nuevoEstado"
func (nr *NodoRaft) cambiarEstado(nuevoEstado string) {
	nr.Mux.Lock()
	nr.Logger.Printf("Nodo %d cambia estado de %s a %s (Term %d)", nr.Yo, nr.State, nuevoEstado, nr.CurrentTerm)
	nr.State = nuevoEstado
	nr.Mux.Unlock()
}

// Inicializa un timer con un desfase inicial aleatorio
func (nr *NodoRaft) inicializarTimerAleatorio() {
	// Desfase aleatorio inicial
	initialOffset := random(50*time.Millisecond, 150*time.Millisecond)
	timeout := random(timeoutMin, timeoutMax)
	nr.reiniciarTimer(timeout + initialOffset)
}

// Reinicia el temporizador con una nueva duración
func (nr *NodoRaft) reiniciarTimer(duracion time.Duration) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	if nr.Timer == nil {
		nr.Timer = time.NewTimer(duracion)
	} else {
		if !nr.Timer.Stop() {
			select {
			case <-nr.Timer.C:
			default:
			}
		}
		nr.Timer.Reset(duracion)
	}
}

// Funcion que se encarga de aplicar las operaciones a la máquina de estados
func (nr *NodoRaft) AplicarOperacionesInf() {
	for op := range nr.CanalAplicar {
		nr.Mux.Lock()
		switch op.Operacion.Operacion {
		case "escribir":
			nr.Ram[op.Operacion.Clave] = op.Operacion.Valor
			nr.Logger.Printf("Nodo %d: Escribiendo en RAM: Clave=%s, Valor=%s", nr.Yo, op.Operacion.Clave, op.Operacion.Valor)

		case "leer":
			valor, existe := nr.Ram[op.Operacion.Clave]
			if existe {
				nr.Logger.Printf("Nodo %d: Leyendo de RAM: Clave=%s, Valor=%s", nr.Yo, op.Operacion.Clave, valor)
			} else {
				nr.Logger.Printf("Nodo %d: Clave no encontrada en RAM: Clave=%s", nr.Yo, op.Operacion.Clave)
			}

		default:
			nr.Logger.Printf("Nodo %d: Operación desconocida: %s", nr.Yo, op.Operacion.Operacion)
		}
		nr.Mux.Unlock()
	}
}

// Lógica base de raft
func (nr *NodoRaft) GestionarLiderazgo() {
	nr.inicializarTimerAleatorio()
	go nr.AplicarOperacionesInf()
	for {
		select {
		case <-nr.AppendEntry:
			switch nr.State {
			case StateFollower:
				nr.reiniciarTimer(random(timeoutMin, timeoutMax))
			case StateCandidate:
				nr.Logger.Printf("Nodo %d pierde candidatura, pasa a ser follower (Term %d)", nr.Yo, nr.CurrentTerm)
				nr.cambiarEstado(StateFollower)
				nr.reiniciarTimer(random(timeoutMin, timeoutMax))
			case StateLeader:
				nr.Logger.Printf("Nodo %d pierde liderazgo, pasa a ser follower (Term %d)", nr.Yo, nr.CurrentTerm)
				nr.cambiarEstado(StateFollower)
				nr.reiniciarTimer(random(timeoutMin, timeoutMax))
			default:
				nr.Logger.Printf("Nodo %d en estado desconocido %s", nr.Yo, nr.State)
			}

		case <-nr.Timer.C:
			switch nr.State {
			case StateFollower:
				nr.Logger.Printf("Nodo %d inicia elección (Term %d)", nr.Yo, nr.CurrentTerm)
				nr.Mux.Lock()
				nr.CurrentTerm++
				nr.Mux.Unlock()
				nr.cambiarEstado(StateCandidate)
				nr.reiniciarTimer(random(timeoutMin, timeoutMax))
				go nr.empezarEleccion()
			case StateCandidate:
				nr.Logger.Printf("Nodo %d reinicia elección (Term %d)", nr.Yo, nr.CurrentTerm)
				nr.Mux.Lock()
				nr.CurrentTerm++
				nr.Mux.Unlock()
				go nr.empezarEleccion()
			case StateLeader:
				for i := 0; i < len(nr.Nodos); i++ {
					if i != nr.Yo {
						go nr.EnviarAppendEntries(i)
					}
				}
				nr.reiniciarTimer(intervaloLatidos)
			default:
				nr.Logger.Printf("Nodo %d en estado desconocido %s", nr.Yo, nr.State)
			}
		}
	}
}

func (nr *NodoRaft) empezarEleccion() {
	nr.Mux.Lock()
	nr.VotedFor = nr.Yo
	nr.Mux.Unlock()

	// Contador de votos (empezamos con 1 porque el nodo se vota a sí mismo)
	votantes := 1
	var votosMutex sync.Mutex
	var wg sync.WaitGroup // WaitGroup para esperar a todas las solicitudes de votos

	// Canal para monitorear si se ha ganado la elección
	eleccionGanada := make(chan bool, 1)
	mayoriaNecesaria := len(nr.Nodos)/2 + 1

	var lastLogIndex, lastLogTerm int
	if len(nr.LogEntries) > 0 {
		// Si el log no está vacío, obtener los valores del último elemento
		lastLogIndex = nr.LogEntries[len(nr.LogEntries)-1].Index
		lastLogTerm = nr.LogEntries[len(nr.LogEntries)-1].Term
	} else {
		// Si el log está vacío, inicializar los valores como 0
		lastLogIndex = -1
		lastLogTerm = 0
	}

	// Argumentos para las solicitudes de votos
	args := ArgsPeticionVoto{
		Term:         nr.CurrentTerm,
		CandidateID:  nr.Yo,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Enviar solicitudes de voto a los demás nodos
	for i := 0; i < len(nr.Nodos); i++ {
		if i == nr.Yo {
			continue // No enviar petición de voto a sí mismo
		}

		wg.Add(1) // Incrementar el contador del WaitGroup

		go func(nodo int) {
			defer wg.Done() // Decrementar el contador al finalizar

			var reply RespuestaPeticionVoto
			valido := nr.enviarPeticionVoto(nodo, &args, &reply)

			if valido {
				if reply.Term > nr.CurrentTerm {
					// Si el término del receptor es mayor, el candidato se convierte en follower
					nr.Mux.Lock()
					nr.CurrentTerm = reply.Term
					nr.Mux.Unlock()
					nr.cambiarEstado(StateFollower)
					nr.reiniciarTimer(random(timeoutMin, timeoutMax))
					eleccionGanada <- false // Finalizar la elección
				} else if reply.Granted {
					// Voto concedido
					nr.Logger.Printf("Nodo %d ha recibido voto de nodo %d", nr.Yo, nodo)
					votosMutex.Lock()
					votantes++
					// Si ya se alcanzó la mayoría, notificamos la victoria
					if votantes >= mayoriaNecesaria {
						select {
						case eleccionGanada <- true:
						default:
						}
					}
					votosMutex.Unlock()
				}
			}
		}(i)
	}

	// Lanzar una goroutine para esperar el final de todas las solicitudes de voto
	go func() {
		wg.Wait()
		select {
		case eleccionGanada <- false: // Si no se ha ganado la elección, cerrar el canal
		default:
		}
	}()

	// Esperar el resultado de la elección
	if <-eleccionGanada {
		nr.Logger.Printf("Nodo %d gana elección (Term %d)", nr.Yo, nr.CurrentTerm)
		nr.Mux.Lock()
		nr.IdLider = nr.Yo
		nr.Mux.Unlock()
		nr.reiniciarTimer(intervaloLatidos)
		nr.cambiarEstado(StateLeader)
	}
}
