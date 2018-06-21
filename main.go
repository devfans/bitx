package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
	// "runtime"
)

type ArgList []string

func (list *ArgList) String() string {
	return fmt.Sprintf("%s", *list)
}

func (list *ArgList) Set(v string) error {
	*list = append(*list, v)
	return nil
}

var (
	pHost      *string = flag.String("i", "", "host ip/remote receivor ip")
	pPort      *string = flag.String("p", "1200", "port")
	pRepeat    *int    = flag.Int("r", 1, "repeat count for shards request")
	pIsServer  *bool   = flag.Bool("s", false, "serve as receivor")
	pDirectory *string = flag.String("d", "", "directory of files to send")
	pSimSize   *int    = flag.Int("c", 10, "numbers in batch")
	pMax       *int    = flag.Int("m", 512, "max datagram size")
	pFiles     ArgList
)

type Message struct {
	Addr *net.UDPAddr
	Buf  []byte
}

type Client struct {
	connection *net.UDPConn
	udpAddr    *net.UDPAddr
	simSize    int // max job threads at the same time
	alive      bool
	inventory  *Inventory
	bufChan    chan []byte // message queue for sending out
	revChan    chan []byte
	simChan    chan int    // job broking
	jobChan    chan string // job queue
	jobId      int
	counter    int64
}

type Server struct {
	Conn     *net.UDPConn
	Sessions map[string]*Session
	sendChan chan Message // message queue for sending out
	counter  int64
}

type Session struct {
	Address    *net.UDPAddr
	Addr       string
	inventory  *Inventory
	revChan    chan []byte
	finishChan chan int // finished job queue for cleaning up and release memory
}

func (sess *Session) Process(buf []byte) {
	sess.revChan <- buf
}

func (sess *Session) process(sendChan chan Message) {
	for {
		select {
		case b := <-sess.revChan:
			sess._process(b, sendChan)
		case id := <-sess.finishChan:
			sess.Remove(id)
		}
	}
}

func (sess *Session) check(b []byte, sendChan chan Message) {
	id := int(b[1])
	j, ok := sess.inventory.Jobs[id]
	if !ok {
		log.Printf("Invalid job id %d to check\n", id)
		return
	}
	log.Printf("Received check request for job %s id %d\n", j.Meta.Name, j.Meta.Id)
	bufs, finish := j.Check()
	if finish {
		go func(job *Job) {
			time.Sleep(30 * time.Second)
			// if job is not updated(replaced) it will be removed for GC
			if job.GetTsp() < time.Now().Unix()-10 {
				sess.finishChan <- job.Meta.Id
			}
		}(j)
	}

	for _, buf := range bufs {
		sendChan <- Message{sess.Address, buf}
	}
}

func (sess *Session) Remove(id int) {
	log.Printf("Cleaning job with id %d \n", id)
	delete(sess.inventory.Jobs, id)
}

func (sess *Session) _process(buf []byte, sendChan chan Message) {
	switch int(buf[0]) {
	case META:
		sess.processMeta(buf, sendChan)
	case DATA:
		sess.processData(buf)
	case CHECK:
		sess.check(buf, sendChan)
	default:
		log.Printf("Unhandled data: %s\n", string(buf))
	}
}

func (sess *Session) processMeta(buf []byte, sendChan chan Message) {
	id := int(buf[1])
	j := sess.inventory.GetJob(id)
	if j.Finished {
		log.Printf("New replacing job with id %d\n", id)
		j.Clear()
		j.Meta.Parse(buf)

		// runtime.GC()
		// log.Println("Manually gc triggered!")

	} else if j.Count > 0 {
		log.Printf("Duplicate job id %d, dropping now\n", id)
	} else {
		log.Printf("New job with id %d\n", id)
		j.Clear()
		j.Meta.Parse(buf)
	}
	metaCFM := j.MakeMetaCFM()
	sendChan <- Message{sess.Address, metaCFM}
}

func (sess *Session) processData(buf []byte) {
	id := int(buf[1])
	j, ok := sess.inventory.Jobs[id]
	if !ok {
		log.Printf("Dropping data for no such job id %d", id)
		return
	}
	j.ParseData(buf)
}

func (sess *Session) Tick() {
	for {
		time.Sleep(30 * time.Second)
		log.Printf("Session %s Tick:\n", sess.Addr)
		sess.inventory.Tick()
	}
}

func (sess *Session) Init(sendChan chan Message) {
	sess.inventory = NewInventory()
	sess.revChan = make(chan []byte, 20000)
	sess.finishChan = make(chan int, 2000)
	go sess.process(sendChan)
}

func NewSession(sendChan chan Message) *Session {
	sess := &Session{}
	sess.Init(sendChan)
	return sess
}

func (s *Server) GetSession(addr string) *Session {
	sess, ok := s.Sessions[addr]
	if !ok {
		sess = NewSession(s.sendChan)
		sess.Addr = addr
		log.Printf("New client %s is connected!\n", addr)
		s.Sessions[addr] = sess
	}
	return sess
}

func (s *Server) HandleMessage() {
	buf := make([]byte, MAX_REV)
	n, addr, err := s.Conn.ReadFromUDP(buf[0:])
	if err != nil {
		return
	}

	sess := s.GetSession(addr.String())
	sess.Address = addr
	sess.Process(buf[0:n])
}

func (s *Server) Init() {
	s.sendChan = make(chan Message, 20000)
	s.Sessions = make(map[string]*Session)
	s.counter = 0
}

func NewServer() *Server {
	s := &Server{}
	s.Init()
	return s
}

func (s *Server) SendMessage() {
	for {
		m := <-s.sendChan
		_, err := s.Conn.WriteToUDP(m.Buf, m.Addr)
		checkError(err)
	}
}

func checkError(err error) {
	if err != nil {
		log.Printf("Fatal error:%s\n", err.Error())
		os.Exit(1)
	}
}

func (s *Server) Start(udpAddress *net.UDPAddr) {
	var err error
	s.Conn, err = net.ListenUDP("udp", udpAddress)
	checkError(err)

	go s.SendMessage()

	// speed reporting
	go func() {
		var tickTime int64 = time.Now().Unix()
		var tickCount int64 = s.counter
		var count, tsp int64
		for {
			time.Sleep(10 * time.Second)
			count = s.counter
			tsp = time.Now().Unix()
			log.Printf("Speed %d KB/s\n", (count-tickCount)*int64(MAX_SEND)/(1000*(tsp-tickTime)))
			tickTime = tsp
			tickCount = count
		}
	}()

	// main loop
	for {
		s.HandleMessage()
		s.counter++
	}
}

//-------------  client ------------------//

// add jobs
func (c *Client) addFiles(files []string, directory string) {
	if directory != "" {
		err := filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			log.Printf("Adding file %s into the working queue \n", path)
			c.jobChan <- path
			return nil
		})
		checkError(err)
	} else {
		for _, f := range files {
			c.jobChan <- f
			log.Printf("Adding file %s into the working queue \n", f)
		}
	}
	c.jobChan <- "end"
}

func (c *Client) sendFiles() {
	for {
		file := <-c.jobChan
		c.simChan <- 1
		if file == "end" {
			log.Println("Jobs are consumed up, now waitting to finish...")
			for si := 0; si < c.simSize-1; si++ {
				c.simChan <- 1
			}
			log.Println("All jobs are finished, now ending")
			os.Exit(0)
			return
		}
		j := c.initJob(file)
		go j.Process(c.bufChan, c.simChan)
	}
	// log.Println("Sending of all files has been running...")
}

func (c *Client) newId() int {
	if c.jobId == 256 {
		c.jobId = 0
	}
	id := c.jobId
	c.jobId++

	j, ok := c.inventory.Jobs[id]
	if ok {
		for j.Empty.Load().(bool) == false {
			time.Sleep(time.Second)
			log.Printf("Waiting job with Id %d to be read...\n", id)
		}
	}
	return id
}

func (c *Client) sendData() {
	var wait bool
	go func() {
		var tickTime int64 = time.Now().Unix()
		var tickCount int64 = c.counter
		var count, tsp int64
		for {
			time.Sleep(2 * time.Second)
			count = c.counter
			tsp = time.Now().Unix()
			log.Printf("Speed %d KB/s\n", (count-tickCount)*int64(MAX_SEND)/(1000*(tsp-tickTime)))
			tickTime = tsp
			tickCount = count
		}
	}()

	for {
		b := <-c.bufChan
		wait = true
		for wait {
			_, err := c.connection.Write(b)
			if err != nil {
				log.Println(err)
				time.Sleep(1 * time.Second)
			} else {
				wait = false
			}
		}
		c.counter++
	}
}

func (c *Client) receiveMessage() {
	buf := make([]byte, MAX_REV)
	for c.alive {
		n, err := c.connection.Read(buf[0:])
		checkError(err)
		data := make([]byte, n)
		copy(data, buf[0:n])
		c.revChan <- data
	}
}

func (c *Client) Handle() {
	for {
		b := <-c.revChan
		c.handle(b)
	}
}

func (c *Client) handle(b []byte) {
	id := int(b[1])
	j, ok := c.inventory.Jobs[id]
	if !ok {
		log.Printf("Invalid job id %d\n", id)
		return
	}
	if j.Empty.Load().(bool) {
		log.Printf("Job %d is empty, message is dropped!\n", id)
		return
	}
	j.Chan <- b
}

func (c *Client) initJob(name string) *Job {
	id := c.newId()
	log.Printf("Allocated id %d for job %s\n", id, name)
	j := NewJob()
	j.Meta.Id = id
	j.Meta.Name = name
	c.inventory.Jobs[id] = j
	return j
}

func (c *Client) Init(udpAddr *net.UDPAddr, simSize int) {
	c.udpAddr = udpAddr
	c.simSize = simSize
	c.jobId = 0
	c.bufChan = make(chan []byte, 20000)
	c.revChan = make(chan []byte, 20000)
	c.simChan = make(chan int, simSize)
	c.jobChan = make(chan string, 20000)
	c.alive = true
	c.counter = 0
	c.inventory = NewInventory()
}

func (c *Client) Start(files []string, directory string) {
	var err error
	c.connection, err = net.DialUDP("udp", nil, c.udpAddr)
	checkError(err)
	defer c.connection.Close()
	go c.receiveMessage()
	go c.Handle()
	go c.sendData()
	go c.addFiles(files, directory)
	c.sendFiles()
}

func NewClient(udpAddr *net.UDPAddr, simSize int) *Client {
	c := &Client{}
	c.Init(udpAddr, simSize)
	return c
}

//------------------- main -----------------------------

func startClient() {
	addrStr := fmt.Sprintf("%s:%s", *pHost, *pPort)
	log.Printf("Dialing to %s\n", addrStr)
	udpAddr, err := net.ResolveUDPAddr("udp4", addrStr)
	checkError(err)

	c := NewClient(udpAddr, *pSimSize)
	c.Start(pFiles, *pDirectory)
}

func startServer() {
	addrStr := fmt.Sprintf("%s:%s", *pHost, *pPort)

	log.Printf("Listening on %s\n", addrStr)
	udpAddress, err := net.ResolveUDPAddr("udp4", addrStr)
	checkError(err)

	s := NewServer()
	s.Start(udpAddress)
}

func main() {
	flag.Var(&pFiles, "f", "files to send, multiple -f is allowed")
	flag.Parse()

	if flag.Lookup("help") != nil {
		flag.PrintDefaults()
		os.Exit(0)
	}

	MAX_REV = uint32(*pMax)
	MAX_SEND = MAX_REV - 5

	if *pIsServer {
		startServer()
	} else {
		startClient()
	}
}
