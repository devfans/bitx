package main

import (
	"crypto/md5"
	"io/ioutil"
	"log"
	"math"
	"os"
	"sync/atomic"
	"time"
)

// Signals for the first byte of the datagram
const (
	META  int = 178 // meta info
	DATA  int = 191 // data
	CHECK int = 193 // check
	CFM   int = 201 // confirm
)

var (
	MAX_SEND uint32 = 500
	MAX_REV  uint32 = 512
)

// convert byte to int
func b2i(b []byte) uint32 {
	var total uint32 = 0
	var delta uint32
	l := len(b) - 1
	for i := l; i >= 0; i-- {
		delta = uint32(int32(b[i]) << uint(8*(l-i)))
		if delta < uint32(b[i]) {
			log.Fatal("Overflow on b2i")
		}
		total += delta
	}
	return total
}

// convert int to byte with length: l
func _i2b(n int64, l int) []byte {
	var b []byte
	var t int64
	for i := l - 1; i >= 0; i-- {
		t = n >> uint(8*i)
		if t > 0 {
			b = append(b, byte(t))
			n -= t << uint(8*i)
		} else {
			b = append(b, byte(0))
		}
	}
	return b
}

func i2b(n int64) []byte {
	_b := _i2b(n, 5)
	var b []byte
	started := false
	for _, v := range _b {
		if int(v) != 0 || started {
			started = true
			b = append(b, v)
		}
	}
	return b
}

// job meta info
type Meta struct {
	Id   int
	Name string
	Hash string
	Seqs uint32

	seqLength  int
	hashLength int
	nameLength int

	seq  []byte
	name []byte
	hash [16]byte
}

// job
type Job struct {
	Meta     *Meta
	Finished bool
	Empty    atomic.Value
	Data     map[uint32][]byte
	Count    uint32
	Cursor   uint32
	Chan     chan []byte
	Size     uint64
	update   atomic.Value
	buf      []byte
}

type Inventory struct {
	Jobs map[int]*Job
}

// parse job meta from bytes
func (m *Meta) Parse(b []byte) {
	m.Id = int(b[1])
	m.seqLength = int(b[2])
	m.hashLength = int(b[3])
	m.nameLength = int(b[4])

	seqEnd := 5 + m.seqLength
	m.Seqs = b2i(b[5:seqEnd])

	hashEnd := seqEnd + m.hashLength
	copy(m.hash[:], b[seqEnd:hashEnd])
	m.Hash = string(b[seqEnd:hashEnd])

	nameEnd := hashEnd + m.nameLength
	m.Name = string(b[hashEnd:nameEnd])
}

// dump job meta into bytes
func (m *Meta) Dump() []byte {
	m.compute()
	b := []byte{}
	b = append(b, byte(META), byte(m.Id), byte(m.seqLength), byte(m.hashLength), byte(m.nameLength))
	b = append(b, m.seq...)
	b = append(b, m.hash[:]...)
	b = append(b, m.name...)
	return b
}

// compute job meta
func (m *Meta) compute() {
	m.seq = i2b(int64(m.Seqs))
	m.name = []byte(m.Name)

	m.seqLength = len(m.seq)
	m.nameLength = len(m.name)
	m.hashLength = len(m.hash)
}

// get last update timestamp
func (j *Job) GetTsp() int64 {
	return j.update.Load().(int64)
}

// clear job to make job as empty
func (j *Job) Remove() {
	j.Clear()
	j.Empty.Store(true)
}

func (j *Job) Clear() {
	j.Data = make(map[uint32][]byte)
	j.buf = make([]byte, 0)
	j.Chan = make(chan []byte, 2000) // TOFIX: sent job may block the client processing without consuming this chan
	j.Count = 0
	j.Cursor = 0
	j.Finished = false
	j.Size = 0
	j.update.Store(time.Now().Unix())
}

// compose data shard
func (j *Job) Compose(s uint32, data []byte) []byte {
	b := make([]byte, 0)
	seq := _i2b(int64(s), j.Meta.seqLength)
	b = append(b, byte(DATA), byte(j.Meta.Id))
	b = append(b, seq...)
	b = append(b, data...)
	return b
}

// check job data receiving state
func (j *Job) Check() ([][]byte, bool) {
	log.Printf("Checking %s\n", j.Meta.Name)
	bufs := make([][]byte, 0)
	if j.Finished {
		bufs = append(bufs, j.MakeCFM())
		return bufs, j.Finished
	}

	var seq uint32 = 0
	var ok bool
	var piece []byte
	count := 0
	var size uint64 = 0

	// check loss
	loss := make([]uint32, 0)
	for seq < j.Meta.Seqs {
		piece, ok = j.Data[seq]
		if !ok {
			loss = append(loss, seq)
			count++
		} else {
			size += uint64(len(piece))
		}
		seq++
	}

	// make requests for lost shards
	lossCount := len(loss)
	if lossCount > 0 {
		reqMax := int(MAX_SEND) / j.Meta.seqLength
		reqCursor := 0
		var reqEnd int
		// var reqShards int
		for reqCursor < lossCount {
			reqEnd = reqCursor + reqMax
			if reqEnd > lossCount {
				reqEnd = lossCount
			}
			req := make([]byte, 0)
			req = append(req, byte(DATA), byte(j.Meta.Id))
			// reqShards = reqEnd - reqCursor
			for reqCursor < reqEnd {
				reqSeq := _i2b(int64(loss[reqCursor]), j.Meta.seqLength)
				req = append(req, reqSeq...)
				reqCursor++
			}
			// log.Printf("Requesting %d shards for job %s id %d, last: %d\n",
			// reqShards, j.Meta.Name, j.Meta.Id, loss[reqCursor - 1])
			bufs = append(bufs, req)
		}
	}

	// check job status
	j.Size = size
	log.Printf("Progress of job %s ================== %d/%d === waiting %d\n", j.Meta.Name, j.Meta.Seqs-uint32(count), j.Meta.Seqs, count)
	if count == 0 {
		j.Finished = true
		bufs = append(bufs, j.MakeCFM())
		j.Sum()
	}
	return bufs, j.Finished
}

// save data as file
func (j *Job) Save(b []byte) {
	file, err := os.Create(j.Meta.Name)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Write(b)
	log.Printf("File %s is saved .\n", j.Meta.Name)
}

func (j *Job) Sum() {
	b := make([]byte, j.Size)
	var seq uint32 = 0
	var cursor uint32 = 0
	var size uint32 = 0
	for seq < j.Meta.Seqs {
		size = uint32(len(j.Data[seq]))
		copy(b[cursor:cursor+size], j.Data[seq])
		cursor += size
		seq++
	}
	log.Printf("Job total length %d\n", len(b))
	hash := md5.Sum(b)
	log.Printf("Job %s hash:\n", j.Meta.Name)
	log.Println(hash)
	log.Printf("File %s desires hash:\n", j.Meta.Name)
	log.Println(j.Meta.hash)
	if hash != j.Meta.hash {
		log.Fatal("Unexpected failed job receiving for invalid hash detected!\n")
	}
	j.Save(b)
}

// make shard request
func (j *Job) MakeShardReq(seq uint32) []byte {
	b := make([]byte, 0)
	seqBytes := _i2b(int64(seq), j.Meta.seqLength)
	b = append(b, byte(DATA), byte(j.Meta.Id))
	b = append(b, seqBytes...)
	// log.Printf("Sending shard %d request for job %s id %d\n",
	// seq, j.Meta.Name, j.Meta.Id)
	return b
}

// make job confirmation message
func (j *Job) MakeCFM() []byte {
	b := make([]byte, 0)
	b = append(b, byte(CFM), byte(j.Meta.Id))
	log.Printf("Sending CFG for job %s id %d\n", j.Meta.Name, j.Meta.Id)
	// log.Println(b)
	return b
}

// make job meta confirmation message
func (j *Job) MakeMetaCFM() []byte {
	b := make([]byte, 0)
	b = append(b, byte(META), byte(j.Meta.Id))
	log.Printf("Sending META CFG for job %s id %d\n", j.Meta.Name, j.Meta.Id)
	// log.Println(b)
	return b
}

// make check request
func (j *Job) MakeCheckReq() []byte {
	b := make([]byte, 0)
	b = append(b, byte(CHECK), byte(j.Meta.Id))
	log.Printf("Making check request for job %s id %d\n", j.Meta.Name, j.Meta.Id)
	return b
}

// main process for sending job
func (j *Job) Process(bufChan chan []byte, simChan chan int) {
	var err error
	j.buf, err = ioutil.ReadFile(j.Meta.Name) // just pass the job name
	if err != nil {
		log.Fatal(err)
	}
	j.Empty.Store(false)
	end := len(j.buf)
	// step one: send job meta
	j.Meta.Seqs = uint32(math.Ceil(float64(end) / float64(MAX_SEND)))
	j.Meta.hash = md5.Sum(j.buf)
	metaBuf := j.Meta.Dump()
	metaSent := false
	for !metaSent {
		bufChan <- metaBuf
		time.Sleep(600 * time.Millisecond)
		select {
		case revBuf := <-j.Chan:
			if int(revBuf[0]) == META {
				metaSent = true
			}
		default:
			time.Sleep(2 * time.Second)
		}
	}
	log.Printf("Sent job meta for %s id %d\n", j.Meta.Name, j.Meta.Id)

	// step two: send job data
	j.send(bufChan)

	// step three: launch check loop
	stop := make(chan int, 0)
	go func() {
		var now int64
		j.update.Store(time.Now().Unix())
		for {
			select {
			case <-stop:
				return
			default:
				now = time.Now().Unix()
				if now-j.update.Load().(int64) > 3 {
					bufChan <- j.MakeCheckReq()
					j.update.Store(now) // deplay another 10 seconds
				}
			}
		}
	}()

	// step four: wait finishing confirm
	for !j.Finished {
		revBuf := <-j.Chan
		j.handle(revBuf, bufChan)
		j.update.Store(time.Now().Unix())
	}
	stop <- 1
	log.Printf("Job total length %d \n", len(j.buf))
	log.Printf("Finished job %s\n", j.Meta.Name)
	<-simChan
	j.Remove()
}

// handle message from receivors
func (j *Job) handle(b []byte, bufChan chan []byte) {
	sig := int(b[0])
	switch sig {
	case DATA:
		j.resendData(b, bufChan)
	case CFM:
		j.Finished = true
	}
}

// reset job data shard
func (j *Job) resendData(b []byte, bufChan chan []byte) {
	var cursor int = 2
	var seqEnd int
	var seq, cstart, cend uint32
	end := uint32(len(j.buf))
	count := 0
	for cursor < len(b) {
		seqEnd = cursor + j.Meta.seqLength
		seq = b2i(b[cursor:seqEnd])
		// log.Printf("Received resending shard %d request of job %s\n", seq, j.Meta.Name)
		// log.Println(b)
		cstart = MAX_SEND * seq
		cend = cstart + MAX_SEND
		if cend > end {
			cend = end
		}
		dataBuf := j.Compose(seq, j.buf[cstart:cend])
		bufChan <- dataBuf
		// log.Printf("Resent shard %d of job %s\n", seq, j.Meta.Name)
		cursor = seqEnd
		count++
	}
	// log.Printf("Resent %d shards for job %s id %d, last: %d\n", count, j.Meta.Name, j.Meta.Id, seq)
}

// send job data
func (j *Job) send(bufChan chan []byte) {
	end := len(j.buf)
	var seq uint32 = 0
	cursor := 0
	cend := cursor
	var dataBuf []byte
	for cend < end {
		cend = cursor + int(MAX_SEND)
		if cend > end {
			cend = end
		}
		// log.Printf("Sending %d slice %d to %d \n", seq, cursor, cend)
		dataBuf = j.Compose(seq, j.buf[cursor:cend])
		bufChan <- dataBuf
		cursor = cend
		seq++
	}
	// check check request immediately
	bufChan <- j.MakeCheckReq()
}

// parse data bytes and register it
func (j *Job) ParseData(buf []byte) {
	seqEnd := 2 + j.Meta.seqLength
	seq := b2i(buf[2:seqEnd])
	if seq >= j.Meta.Seqs {
		log.Printf("Wrong shard %d detected max: %d, bytes: \n", seq, j.Meta.Seqs)
		log.Println(buf[2:seqEnd])
		return
	}
	_, ok := j.Data[seq]
	if ok {
		log.Printf("Duplicate data for job %d, seq %d\n", j.Meta.Id, seq)
		return
	}
	j.Data[seq] = buf[seqEnd:]
	j.Count++
}

func NewJob() *Job {
	j := &Job{}
	j.Meta = &Meta{}
	j.Data = make(map[uint32][]byte)
	j.Finished = false
	j.Empty.Store(true)
	j.Chan = make(chan []byte, 1000)
	j.update.Store(time.Now().Unix())
	return j
}

// get job handler
func (i *Inventory) GetJob(id int) *Job {
	j, ok := i.Jobs[id]
	if !ok {
		log.Printf("Creating new job with id: %d\n", id)
		j = NewJob()
		j.Meta.Id = id
		i.Jobs[id] = j
	}
	return j
}

func (i *Inventory) Tick() {
	for id, j := range i.Jobs {
		log.Printf("Job %d %s %d/%d\n", id, j.Meta.Name, j.Count, j.Meta.Seqs)
	}
}

func NewInventory() *Inventory {
	i := &Inventory{}
	i.Jobs = make(map[int]*Job)
	return i
}
