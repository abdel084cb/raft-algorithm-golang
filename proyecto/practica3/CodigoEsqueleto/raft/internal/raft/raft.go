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

const intervaloLatidos = 250 * time.Millisecond
const timeoutMin = 500 * time.Millisecond
const timeoutMax = 1000 * time.Millisecond
const timeoutRpc = 45 * time.Millisecond

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

	// Se lee cuando se ha recibido un latido
	Latido chan bool
	// Timer que gestiona tiempo entre latidos, timeouts de candidatos
	// y timeouts de seguidores
	Timer *time.Timer
	// indice de este nodo en campo array "nodos"
	Yo int
	// indice del nodo lider en campo array "nodos"
	IdLider int
	// Estados: StateFollower, StateCandidate y StateLeader
	State string

	/* STATE */
	// En todos los servidores:
	// El término actual, que identifica las épocas de elecciones
	CurrentTerm int
	// Guarda a quién votó el nodo en el término actual
	VotedFor int
	// Aquí se almacenan todas las entradas del log
	LogEntries []Entry
	// índice de la entrada de registro (log) más reciente que se ha "comprometido"
	// (es decir, que ha sido confirmada como replicada en la mayoría de los nodos)
	// y puede ser aplicada a la máquina de estados del sistema
	CommitIndex int
	// índice del registro (log) más reciente que se ha aplicado a la state machine
	LastApplied int

	// En el lider:
	// Índice de la próxima entrada del log para enviar a cada nodo
	NextIndex []int
	// Índice más alto del log replicado en cada nodo
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
	nr.Yo = yo
	nr.IdLider = -1
	nr.CurrentTerm = 0
	nr.VotedFor = -1
	nr.Latido = make(chan bool)
	nr.State = StateFollower

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
	indice := -1
	mandato := -1
	esLider := nr.Yo == nr.IdLider
	idLider := nr.IdLider
	valorADevolver := ""

	if esLider {
		// Generar nueva entrada en el log
		//¿Actualizar el registro de operaciones del lider, para la siguiente practica?
		// De momento no existe tal registro.
		indice = nr.CommitIndex
		mandato = nr.CurrentTerm
		// ¿El líder se cuenta a sí mismo?
		confirmados := 1
		// Argumentos de llamada RPC
		entry := Entry{
			Term:      mandato,
			Index:     indice, // De momento asumimos que es CommitIndex
			Operation: operacion,
		}

		// Se incluye en el registro del lider la operacion solicitada por el cliente
		nr.LogEntries = append(nr.LogEntries, entry)
		nr.Logger.Printf("Nodo %d lider: Term=%d, Index=%d, Operacion=%s, Clave=%s, Valor=%s", nr.Yo,
			entry.Term, entry.Index, entry.Operation.Operacion, entry.Operation.Clave, entry.Operation.Valor)
		var reply Results
		var args ArgAppendEntries
		//args.PrevLogIndex ¿Practica 4?
		//args.PrevLogIndex ¿Practica 4?
		args.LeaderID = idLider
		args.Term = mandato
		args.Entries = entry
		args.LeaderCommitIndex = indice

		// Se somete la operación a todos los nodos
		for i := 0; i < len(nr.Nodos); i++ {
			if i == nr.Yo {
				continue
			}

			// Realiza la llamada RPC para replicar la entrada en el nodo
			err := nr.Nodos[i].CallTimeout("NodoRaft.AppendEntries", args, &reply, timeoutRpc)
			// Verifica si no hubo errores en la llamada RPC
			if err != nil {
				nr.Logger.Printf("Error replicando entrada en nodo %d: %v", i, err)
				continue
			}
			// Si la llamada fue exitosa, verifica que el nodo confirmó la entrada
			if reply.Success {
				nr.Logger.Printf("Nodo %d acepta la entrada (Term %d)", i, nr.CurrentTerm)
				confirmados++
			} else {
				nr.Logger.Printf("Nodo %d rechaza la entrada (Term %d)", i, nr.CurrentTerm)
			}

		}
		// Verificar si se confirma en la mayoría
		if confirmados > len(nr.Nodos)/2 {
			nr.CommitIndex++
			valorADevolver = "Operacion confirmada"
		} else {
			valorADevolver = "Fallo en consenso"
		}
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
	if peticion.Term < nr.CurrentTerm {
		/*El término del candidato es menor al término del nodo votante, se rechaza el voto y se le informa
		del término actual*/
		nr.Logger.Printf("Nodo %d rechaza voto por término menor al nodo %d (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
		reply.Granted = false
	} else if peticion.Term == nr.CurrentTerm && peticion.CandidateID != nr.VotedFor {
		/*El nodo actual ya ha votado (tiene el mismo término que el candidato), y además ha votado a otro candidato*/
		nr.Logger.Printf("Nodo %d rechaza voto al nodo %d, porque ha votado a otro nodo antes (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
		reply.Granted = false
	} else if peticion.Term > nr.CurrentTerm {
		/* Si el nodo actual no ha votado todavía (tiene menor término que el candidato), entonces vota
		y actualiza su término */
		nr.Mux.Lock()
		nr.CurrentTerm = peticion.Term
		nr.VotedFor = peticion.CandidateID
		nr.Mux.Unlock()
		nr.cambiarEstado(StateFollower)
		nr.reiniciarTimer(random(timeoutMin, timeoutMax))
		nr.Logger.Printf("Nodo %d vota al nodo %d (Term %d)", nr.Yo, peticion.CandidateID, nr.CurrentTerm)
		reply.Term = nr.CurrentTerm
		reply.Granted = true
	}
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

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	results.Success = true
	if args.Entries.Operation.Operacion == "" { // Se ha recibido un latido
		if args.Term < nr.CurrentTerm {
			// El término del lider que ha mandado el latido es obsoleto, se rechaza
			nr.Logger.Printf("Nodo %d rechaza el latido del nodo %d por termino menor (Term %d)", nr.Yo, args.LeaderID, nr.CurrentTerm)
			results.Success = false
			results.Term = nr.CurrentTerm
		} else {
			// El término del lider es actual o mayor, se acepta
			nr.Mux.Lock()
			nr.CurrentTerm = args.Term
			nr.IdLider = args.LeaderID
			nr.Mux.Unlock()
			nr.Latido <- true
			// Succes es true
		}
	} else { // Se ha recibido una entrada al registro (log)
		nr.LogEntries = append(nr.LogEntries, args.Entries)
		nr.Logger.Printf("Nodo %d follower: Term=%d, Index=%d, Operacion=%s, Clave=%s, Valor=%s", nr.Yo,
			args.Entries.Term, args.Entries.Index, args.Entries.Operation.Operacion, args.Entries.Operation.Clave,
			args.Entries.Operation.Valor)
	}
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

// Pre: true
// Post: Envia un latido a todos los nodos
func (nr *NodoRaft) enviarLatido(indice int) {
	// Argumentos para la llamada RPC
	var args ArgAppendEntries
	var reply Results
	args.Term = nr.CurrentTerm
	args.LeaderID = nr.Yo
	args.Entries.Operation.Operacion = ""
	nr.Logger.Printf("Nodo %d envia latido al nodo %d (Term %d)", nr.Yo, indice, nr.CurrentTerm)
	err := nr.Nodos[indice].CallTimeout("NodoRaft.AppendEntries", args, &reply, timeoutRpc)
	if err != nil {
		nr.Logger.Printf("Nodo %d no ha enviado el latido correctamente (Term %d)", nr.Yo, nr.CurrentTerm)
	}
	if reply.Term > args.Term {
		nr.Logger.Printf("Nodo %d ha descubierto que es lider con termino obsoleto (Term %d)", nr.Yo, nr.CurrentTerm)
		nr.Mux.Lock()
		nr.CurrentTerm = reply.Term
		nr.Mux.Unlock()
		nr.cambiarEstado(StateFollower)
		nr.reiniciarTimer(random(timeoutMin, timeoutMax))
	}
}

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

// Detiene y limpia el temporizador
func (nr *NodoRaft) detenerTimer() {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	if nr.Timer != nil {
		if !nr.Timer.Stop() {
			select {
			case <-nr.Timer.C:
			default:
			}
		}
		nr.Timer = nil
	}
}

// Lógica base de raft
func (nr *NodoRaft) GestionarLiderazgo() {
	nr.inicializarTimerAleatorio()

	for {
		select {
		case <-nr.Latido:
			switch nr.State {
			case StateFollower:
				nr.Logger.Printf("Nodo %d recibe latido (Term %d)", nr.Yo, nr.CurrentTerm)
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
				nr.Logger.Printf("Nodo %d envia latidos (Term %d)", nr.Yo, nr.CurrentTerm)
				for i := 0; i < len(nr.Nodos); i++ {
					if i != nr.Yo {
						go nr.enviarLatido(i)
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

	// Argumentos para las solicitudes de votos
	args := ArgsPeticionVoto{
		Term:        nr.CurrentTerm,
		CandidateID: nr.Yo,
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
