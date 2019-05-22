package main

import (
  "log"
  "math"
  "os"
  "path"
  "time"
  "fmt"
  "io"
  "strings"
  "crypto/md5"
  "sync/atomic"
)

// Signals for the first byte of the datagram
const (
  META  int = 178 // meta info
  DATA  int = 191 // data
  CHECK int = 193 // check
  CFM   int = 201 // confirm
  BLOCK int = 220 // ShiftBlock
  SYNC  int = 222 // sync missing seq
)

var (
  MAX_SEND uint32 = 500
  MAX_REV  uint32 = 512
  BLOCK_SIZE uint32 = 1000000
  BLOCK_SIM  uint32 = 3
  MAX_WAIT   uint32 = 3
  LOOP       time.Duration = time.Duration(1) * time.Second
  NO_FILE_HASH  bool = false
)

var (
  T_SIZE      byte = 0
  T_SPEED     byte = 1
  T_SEQS      byte = 2
  T_SEQ       byte = 3
  T_BLOCK     byte = 4
  T_BLOCKS    byte = 5
  T_BLOCKSIM    byte = 6
  T_BLOCKSIZE   byte = 7
  T_FILE      byte = 8
  T_LOG       byte = 9
  T_NEW       byte = 10
  T_SYNC      byte = 11
  T_STOP      byte = 12
  T           *Terminal
  DOTS        string = "--------------------------------------------"
)

type Output struct {
  Type   byte
  Int    uint32
  String string
}

func Update(t byte, i uint32, s string) {
  o := Output {t, i, s}
  if T != nil {
    T.Send(o)
  }
}

func Log(format string, a ...interface{}) {
  if T != nil {
    T.Send(Output { T_LOG, 0, fmt.Sprintf(format, a...) })
  } else {
    log.Println(fmt.Sprintf(format, a...))
  }
}

func Logln(a ...interface{}) {
  if T != nil {
    T.Send(Output { T_LOG, 0, fmt.Sprint(a...)})
  } else {
    log.Println(a...)
  }
}

func checkError(err error) {
  if err != nil {
    // log.Printf("Fatal error:%s\n", err.Error())
    // os.Exit(1)
    log.Fatal(err)
  }
}

// convert byte to int
func b2i(b []byte) uint32 {
  l := len(b)
  if l > 4 {
    log.Fatalf("Overflow %d bytes to uint32\n", l)
  }
  var total uint32 = 0
  for i := l - 1; i >= 0; i-- {
    total += uint32(b[i]) << uint(8*(l - i - 1))
  }
  return total
}

// convert int to byte with length: l
func i2bl(n uint32, l int) []byte {
  b := make([]byte, l)
  for i := l - 1; i >= 0; i-- {
    if (l - i) <= 4 {
      b[i] = byte((n >> uint(8 * (l - i - 1))) & 0xff)
    }
  }
  return b
}

func i2b(n uint32) []byte {
  b := i2bl(n, 4)
  cursor := 0
  for i, v := range b {
    if uint8(v) != 0{
      cursor = i
      break
    }
  }
  return b[cursor:]
}

// job meta info
type Meta struct {
  Id   int
  Name string
  Hash string
  Seqs uint32
  Blocks     uint32
  BlockSize  uint32
  BlockSim   uint32

  seqLength  int
  hashLength int
  nameLength int

  seq  []byte
  name []byte
  hash [16]byte
}

// block 
type Block struct {
  Full     bool
  Start    uint32
  End      uint32
  Data     []byte
  On       bool
  Size     int
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
  update   int64

  Blocks   map[uint32]*Block
  Map      map[uint32]uint32
  cursor   uint32 // cursor of seqs
  bursor   uint32 // cursor of blocks
  tursor   chan uint32 // cursor for allow max resend seq request
  tsp      int64

  bytes    int64 // client side file bytes
}

type Inventory struct {
  Jobs map[int]*Job
}

type Terminal struct {
  logs        []string
  logNum     int
  file       string
  seq        uint32
  seqs       uint32
  block      uint32
  blocks     uint32
  blockSim   uint32
  blockSize  uint32
  sync       uint32
  speed      uint32
  size       uint32
  lines      int
  next       bool
  start      int64
  percents   int

  bus        chan Output
  tick       chan int
  stop       chan int
  exit       chan int
}

func (t *Terminal) goBack() {
  for i := 0; i < t.lines; i++ {
    fmt.Printf("\033[F")
  }
}

func GetDots(percents int) string {
  return strings.Replace(DOTS, "-", "=", len(DOTS) * percents/100)
}

func (t *Terminal) flush () {
  if !t.next {
    t.goBack()
  } else {
    t.next = false
    t.percents = 0
    t.seq = 0
    t.block = 0
    t.speed = 0
    t.start = time.Now().Unix()
  }
  if t.seqs > 0 {
    t.percents = int(t.seq*100/t.seqs)
  }
  duration := time.Now().Unix() - t.start
  if duration == 0 {
    t.speed = 0
  } else {
    t.speed = (t.seq * MAX_SEND)/ uint32(1024*duration)
  }
  fmt.Printf("\033[K\033[1;36mFile %s (%v MB) Seqs: %v/%v Blocks(Size %vMB): %v/%v Parallel: %v\033[0m \n", t.file, t.size, t.seq, t.seqs, t.blockSize, t.block + 1, t.blocks, t.blockSim)
  fmt.Println("\033[K------------------------------------------------------------------")
  for _, msg := range t.logs {
    fmt.Println("\033[K" + msg)
  }
  fmt.Printf("\033[KTransfered [%s] %v%% Avg: %vKB/s\n", GetDots(t.percents), t.percents, t.speed)
  t.lines = len(t.logs) + 3
}

func NewTerminal() *Terminal {
  t := &Terminal{}
  t.Init()
  go t.Start()
  go func (c chan int, stop chan int) {
    for {
      select {
      case <-stop:
        return
      default:
        c <- 0
        time.Sleep(100*time.Millisecond)
      }
    }
  } (t.tick, t.stop)
  return t
}

func (t *Terminal) Init() {
  t.logNum = 10
  t.logs = make([]string, 0)
  t.lines = 0
  t.bus = make(chan Output, 100000)
  t.tick = make(chan int, 100000)
  t.stop = make(chan int, 0)
  t.start = time.Now().Unix()
  t.exit = make(chan int, 1)
}

func (t *Terminal) Send (o Output) {
  t.bus <- o
}

func (t *Terminal) Log (msg string) {
  if len(t.logs) >= t.logNum {
    t.logs = t.logs[1:]
  }
  t.logs = append(t.logs, msg)
}

func (t *Terminal) handle(o Output) {
  switch o.Type {
    case T_FILE:
      t.file = o.String
    case T_LOG:
      t.Log(o.String)
    case T_SIZE:
      t.size = o.Int
    case T_SEQ:
      t.seq = o.Int
    case T_SEQS:
      t.seqs = o.Int
    case T_BLOCK:
      t.block = o.Int
    case T_BLOCKS:
      t.blocks = o.Int
    case T_SYNC:
      t.sync = o.Int
    case T_NEW:
      t.next = true
    case T_SPEED:
      t.speed = o.Int
    case T_BLOCKSIM:
      t.blockSim = o.Int
    case T_BLOCKSIZE:
      t.blockSize = o.Int
  }
}

func (t *Terminal) flushAll() {
  for {
    select {
    case o := <-t.bus:
      t.handle(o)
      t.flush()
    default:
      return
    }
  }
}

func (t *Terminal) Start () {
  for {
    select {
    case <- t.tick:
      t.flush()
    case o := <-t.bus:
      if o.Type == T_STOP {
        t.stop <-0
        t.flushAll()
        t.exit <-0
        return
      } else {
        t.handle(o)
      }
    default:
    }
  }
}

func (t *Terminal) Exit() {
  <-t.exit
}

// parse job meta from bytes
func (m *Meta) Parse(b []byte) {
  m.Id = int(b[1])
  m.seqLength = int(b[2])
  m.hashLength = int(b[3])
  m.nameLength = int(b[4])

  cur := 5 + m.seqLength
  m.Seqs = b2i(b[5:cur])

  m.BlockSize = b2i(b[cur:cur+4])
  cur += 4
  m.BlockSim = b2i(b[cur:cur+4])
  cur += 4
  m.Blocks = b2i(b[cur:cur+4])
  cur += 4

  hashEnd := cur + m.hashLength
  copy(m.hash[:], b[cur:hashEnd])
  m.Hash = string(b[cur:hashEnd])

  nameEnd := hashEnd + m.nameLength
  m.Name = string(b[hashEnd:nameEnd])
}

// dump job meta into bytes
func (m *Meta) Dump() []byte {
  m.compute()
  b := []byte{}
  b = append(b, byte(META), byte(m.Id), byte(m.seqLength), byte(m.hashLength), byte(m.nameLength))
  b = append(b, m.seq...)
  b = append(b, i2bl(m.BlockSize, 4)...)
  b = append(b, i2bl(m.BlockSim, 4)...)
  b = append(b, i2bl(m.Blocks, 4)...)
  b = append(b, m.hash[:]...)
  b = append(b, m.name...)
  return b
}

func (m *Meta) GetHash() [16]byte {
  return m.hash
}

// compute job meta
func (m *Meta) compute() {
  m.seq = i2b(m.Seqs)
  m.name = []byte(m.Name)

  m.seqLength = len(m.seq)
  m.nameLength = len(m.name)
  m.hashLength = len(m.hash)
  m.BlockSize = BLOCK_SIZE
  m.BlockSim = BLOCK_SIM
  m.Blocks = uint32(math.Ceil(float64(m.Seqs) / float64(BLOCK_SIZE)))
  Update(T_BLOCKS, m.Blocks, "")
  Update(T_BLOCKSIZE, m.BlockSize*MAX_SEND/(1024*1024), "")
  Update(T_BLOCKSIM, m.BlockSim, "")
}

// clear job to make job as empty
func (j *Job) Remove() {
  j.Clear()
  j.Empty.Store(true)
}

func (j *Job) Clear() {
  j.Data = make(map[uint32][]byte)
  j.Chan = make(chan []byte, 2000) // TOFIX: sent job may block the client processing without consuming this chan
  j.Count = 0
  j.Cursor = 0
  j.Finished = false
  j.Size = 0
  j.update = time.Now().Unix()

  j.cursor = 0
  j.tsp = time.Now().Unix()
  j.bytes = 0
  j.bursor = 0
  j.Blocks = make(map[uint32]*Block, 0)
  j.Map = make(map[uint32]uint32)
}

// compose data shard
func (j *Job) Compose(s uint32, data []byte) []byte {
  b := make([]byte, 0)
  seq := i2bl(s, j.Meta.seqLength)
  b = append(b, byte(DATA), byte(j.Meta.Id))
  b = append(b, seq...)
  b = append(b, data...)
  return b
}

// compose sync
func (j *Job) ComposeSync(cursor uint32) []byte {
  b := make([]byte, 0)
  seq := i2bl(cursor, j.Meta.seqLength)
  b = append(b, byte(SYNC), byte(j.Meta.Id))
  b = append(b, seq...)
  return b
}

// NewBlock
func NewBlock() *Block {
  block := &Block{}
  block.Full = false
  block.On = false
  block.Size = 0
  return block
}

func (b *Block) CommitSequence(seq uint32, buf []byte) {
  if seq == b.End && len(buf) != int(MAX_SEND) {
    log.Printf("Resizing block for last sequence %v length %v", seq, len(buf))
    b.Size = b.Size - (int(MAX_SEND) - len(buf))
  }

  cursor := MAX_SEND * (seq - b.Start)
  copy(b.Data[cursor:int(cursor)+len(buf)], buf)
}

// both side 
func (j *Job) AllocateBlock(bursor uint32) {
  if j.Meta.Blocks <= bursor {
    log.Fatalf("Block index over-allocating %v/%v\n", bursor, j.Meta.Blocks)
  }
  _, ok := j.Blocks[bursor]
  if !ok {
    block := NewBlock()
    block.Start = j.Meta.BlockSize * bursor
    if bursor == j.Meta.Blocks - 1 {
      block.End = j.Meta.Seqs - 1
    } else {
      block.End = j.Meta.BlockSize * (bursor + 1) - 1
    }
    block.Size = int(MAX_SEND) * int(block.End - block.Start + 1)
    block.Data = make([]byte, block.Size)
    j.Blocks[bursor] = block
    Log("Allocated block %v start %v end %v", bursor, block.Start, block.End)
  }
}

// server side job validation
func (j *Job) Validate(counter uint32) ([][]byte, bool) {
  bufs := make([][]byte, 0)
  blocks := make([]uint32, 0)
  var ok bool
  for b:= j.bursor; b < j.Meta.Blocks; b++ {
    block, ok := j.Blocks[b]
    if ok {
      if block.Full && b == j.bursor {
        j.SaveBlock(b)
        next := j.bursor + j.Meta.BlockSim
        if next < j.Meta.Blocks {
          j.AllocateBlock(next)
        }
        j.bursor++
      } else if !block.On {
        blocks = append(blocks, b)
      }
    } else {
      break
    }
  }
  // send block request
  if len(blocks) > 0 {
    req := make([]byte, 0)
    req = append(req, byte(BLOCK), byte(j.Meta.Id), byte(len(blocks)))
    req = append(req, i2bl(j.bursor, 4)...)
    for _, b := range blocks {
      req = append(req, i2bl(b, 4)...)
    }
    if len(req) < int(MAX_REV) {
      if len(req) < int(MAX_REV) - 50 {
        req = append(req, make([]byte, int(MAX_REV) - 50 - len(req))...)
      }
      bufs = append(bufs, req)
    } else {
      log.Fatalln("Allocating too many blocks in an unhandled condition!")
    }
  }

  // check job seqs
  var v uint32
  loss := make([]uint32, 0)

  for cindex := j.cursor; cindex < j.Cursor; cindex++ {
    v, ok = j.Map[cindex]
    if ok {
      if v == 0 {
        if cindex == j.cursor {
          j.cursor++
        }
      } else if counter > v + MAX_WAIT {
        loss = append(loss, cindex)
        j.Map[cindex] = counter
      }
    } else {
      j.Map[cindex] = counter
    }
  }

  reqSync := make([]byte, 0)
  reqSync = append(reqSync, byte(SYNC), byte(j.Meta.Id))
  reqSync = append(reqSync, i2bl(j.cursor, j.Meta.seqLength)...)
  bufs = append(bufs, reqSync)

  // Mark full blokcs
  fullBlocks := 0
  for bf:= j.bursor; bf < j.Meta.Blocks; bf++ {
    block, ok := j.Blocks[bf]
    if ok {
      if j.cursor > block.End {
        block.Full = true
        fullBlocks++
      }
    } else {
      break
    }
  }

  // Make seq requests
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
        reqSeq := i2bl(loss[reqCursor], j.Meta.seqLength)
        req = append(req, reqSeq...)
        reqCursor++
      }
      // log.Printf("Requesting %d shards for job %s id %d, last: %d\n",
      // reqShards, j.Meta.Name, j.Meta.Id, loss[reqCursor - 1])
      bufs = append(bufs, req)
    }
  }
  if v, ok = j.Map[j.Meta.Seqs - 1]; ok && v == 0 && j.bursor == j.Meta.Blocks {
    j.Finished = true
    j.Finalize()
  }
  // log.Printf("%v Reqs(max request %v): %v blocks(%v/%v) %v seqs(%v/%v) filled %v blocks", len(bufs), j.Cursor, len(blocks), j.bursor, j.Meta.Blocks, len(loss), j.cursor, j.Meta.Seqs, fullBlocks)
  return bufs, j.Finished
}

// start job loop at server end
func (j *Job) Start() {
  // allocate and request blocks
  var i uint32
  for i = 0; i < j.Meta.BlockSim && i < j.Meta.Blocks; i++ {
    j.AllocateBlock(i)
  }
}

// server side append block to file
func (j *Job) SaveBlock(b uint32) {
  block, ok := j.Blocks[b]
  if ! ok {
    log.Fatal("Invalid block to append")
  }
  // ensure dir exists
  _ = os.MkdirAll(path.Dir(j.Meta.Name), os.ModePerm)
  if b == 0 {
    if _, err := os.Stat(j.Meta.Name); err == nil {
      err = os.Rename(j.Meta.Name, j.Meta.Name+string(time.Now().Format(time.RFC3339)))
      if err != nil {
        log.Fatalf("Failed to rename old config file %v.\n", j.Meta.Name)
      }
    }
  }
  f, err := os.OpenFile(j.Meta.Name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
  checkError(err)
  size, err := f.Write(block.Data[0:block.Size])
  checkError(err)

  err = f.Close()
  checkError(err)

  delete(j.Blocks, b)
  log.Printf("Appended %v block(%v bytes) to file %s.\n", b, size, j.Meta.Name)
}

func (j *Job) Finalize() {
  var hash [16]byte = [16]byte {}
  if j.Meta.hash != hash && !NO_FILE_HASH {
    f, err := os.Open(j.Meta.Name)
    checkError(err)
    h := md5.New()
    _, err = io.Copy(h, f)
    f.Close()
    checkError(err)
    copy(hash[0:], h.Sum(nil))
  } else {
    log.Println("Skipping file hashing verification!")
  }

  log.Printf("Job %s hash:\n", j.Meta.Name)
  log.Println(hash)
  log.Printf("File %s desires hash:\n", j.Meta.Name)
  log.Println(j.Meta.hash)
  if hash != j.Meta.hash {
    log.Println("Unexpected failed job receiving for invalid hash detected!\n")
  }
}

// make shard request
func (j *Job) MakeShardReq(seq uint32) []byte {
  b := make([]byte, 0)
  seqBytes := i2bl(seq, j.Meta.seqLength)
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
  log.Printf("Sending CFM for job %s id %d\n", j.Meta.Name, j.Meta.Id)
  // log.Println(b)
  return b
}

// make job meta confirmation message
func (j *Job) MakeMetaCFM() []byte {
  b := make([]byte, 0)
  b = append(b, byte(META), byte(j.Meta.Id))
  Log("Sending META CFM for job %s id %d", j.Meta.Name, j.Meta.Id)
  // log.Println(b)
  return b
}

func (j *Job) Prepare() {
  Update(T_FILE, 0, j.Meta.Name)
  var err error
  // j.buf, err = ioutil.ReadFile(j.Meta.Name) // just pass the job name
  info, err := os.Stat(j.Meta.Name);
  checkError(err)
  j.bytes = info.Size()
  Update(T_SIZE, uint32(j.bytes / 1000000), "")
  j.Empty.Store(false)
  // check sequence index overflow
  if j.bytes > 4294967295 * int64(MAX_SEND) {
    log.Fatalf("File size is too big %s %d\n", j.Meta.Name, j.bytes)
  }
  // end := (uint32)info.Size
  // step one: send job meta
  j.Meta.Seqs = uint32(math.Ceil(float64(j.bytes) / float64(MAX_SEND)))
  Update(T_SEQS, j.Meta.Seqs, "")
  if NO_FILE_HASH {
    j.Meta.hash = [16]byte {}
  } else {
    f, err := os.Open(j.Meta.Name)
    checkError(err)
    hash := md5.New()
    _, err = io.Copy(hash, f)
    f.Close()
    checkError(err)
    copy(j.Meta.hash[0:], hash.Sum(nil))
  }
}

// client side sending file
func (j *Job) Process(bufChan chan []byte, simChan chan int) {
  j.Prepare()

  metaBuf := j.Meta.Dump()
  Log("Starting job for file %s size %v seqs %v max tx %v max rev %v", j.Meta.Name, j.bytes, j.Meta.Seqs, MAX_SEND, MAX_REV)
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
  Log("Sent job meta for %s id %d", j.Meta.Name, j.Meta.Id)

  // step two: load into blocks and send job data
  j.sendInitialBlocks(bufChan)

  // step four: wait finishing confirmation
  for !j.Finished {
    revBuf := <-j.Chan
    j.handle(revBuf, bufChan)
    j.update = time.Now().Unix()
  }
  Log("Job total length %d", j.bytes)
  var speed int64
  var duration = time.Now().Unix() - j.tsp
  if duration > 0 {
    speed = j.bytes / (1024*duration)
  }

  Log("Finished job %s Average speed: %v KB/s", j.Meta.Name, speed)
  <-simChan
  j.Remove()
}

// handle message from receivors
func (j *Job) handle(b []byte, bufChan chan []byte) {
  sig := int(b[0])
  switch sig {
  case DATA:
    j.resendData(b, bufChan)
  case BLOCK:
    j.processBlock(b, bufChan)
  case SYNC:
    j.cursor = b2i(b[2:2+j.Meta.seqLength])
    Update(T_SEQ, j.cursor, "")
  case CFM:
    j.Finished = true
  }
}

// client side resend seq
func (j *Job) resendData(b []byte, bufChan chan []byte) {
  var cursor int = 2
  var seqEnd int
  var seq, cstart, cend uint32
  count := 0
  for cursor < len(b) {
    seqEnd = cursor + j.Meta.seqLength
    seq = b2i(b[cursor:seqEnd])
    // log.Printf("Received resending shard %d request of job %s\n", seq, j.Meta.Name)
    // log.Println(b)

    blockIndex := seq / j.Meta.BlockSize
    block, ok := j.Blocks[blockIndex]
    if !ok || !block.Full {
      Log("Can not resend seq %v for Block %v is unavailable", seq, blockIndex)
      continue
    }
    cstart = MAX_SEND * (seq - block.Start)
    cend = cstart + MAX_SEND
    if seq == block.End {
      cend = uint32(block.Size)
    }
    dataBuf := j.Compose(seq, block.Data[cstart:cend])
    bufChan <- dataBuf
    // log.Printf("Resent shard %d of job %s\n", seq, j.Meta.Name)
    cursor = seqEnd
    count++
  }
  // log.Printf("Resent %d shards for job %s id %d, last: %d\n", count, j.Meta.Name, j.Meta.Id, seq)
}

func (j *Job) processBlock(b []byte, bufChan chan []byte) {
  count := int(b[2])
  bursor := b2i(b[3:7])
  // Log("Moving bursor to %v", bursor)
  for j.bursor < bursor {
    delete(j.Blocks, j.bursor)
    j.bursor++
  }
  Update(T_BLOCK, j.bursor, "")

  cursor := 7
  for i := 0; i < count ; i++ {
    block := b2i(b[cursor: cursor+4])
    j.AllocateBlock(block)
    j.sendBlock(block, bufChan)
    cursor += 4
  }
}

// client side send initial blocks
func (j *Job) sendInitialBlocks(bufChan chan []byte) {
  var i uint32
  for i = 0; i < j.Meta.BlockSim && i < j.Meta.Blocks; i++ {
    j.AllocateBlock(i)
    j.sendBlock(i, bufChan)
  }
}

// client side send block data
func (j *Job) sendBlock(b uint32, bufChan chan []byte) {
  block, ok := j.Blocks[b]
  if ok {
    // Log("Sending block data index %v", b)
    if !block.Full {
      j.LoadBlock(block)
      j.sendBlockData(block, bufChan) // Only send once right after it's loaded
    }
    // j.sendBlockData(block, bufChan)
  } else {
    Log("Block %v unavailable yet", b)
  }
}

// client side
func (j *Job) Sync(bufChan chan []byte) {
  var cursor uint32
  for {
    select {
    case v := <-j.tursor:
      if v > cursor {
        Update(T_SEQ, cursor, "")
        cursor = v
      } else if v == 0 {
        if cursor > 0 {
          Log("Stopping job %v sync", j.Meta.Id)
          return
        }
      } else {
        Log("Unexpected lower tursor %v -> %v", cursor, v)
      }
     default:
    }
    // log.Printf("Sync tursor %v to server\n", cursor)
    bufChan <-j.ComposeSync(cursor)
    time.Sleep(time.Second)
  }
}

func (j *Job) sendBlockData(block *Block, bufChan chan []byte) {
  seq := block.Start
  var cursor int = 0
  max := int(MAX_SEND)
  for seq < block.End {
    bufChan <-j.Compose(seq, block.Data[cursor:cursor+max])
    cursor += max
    seq++
  }
  // Log("Sending last seq %v of block cursor %v block size %v, block end %v", seq, cursor, block.Size, len(block.Data))
  bufChan <-j.Compose(seq, block.Data[cursor:block.Size])
  block.On = true
  j.updateTursor()
}

func (j *Job) updateTursor() {
  var tursor uint32
  for b:= j.bursor; b < j.bursor + j.Meta.BlockSim; b++ {
    block, ok := j.Blocks[b]
    if ok {
      if block.On {
        tursor = block.End + 1
      } else {
        break
      }
    } else {
      break
    }
  }

  go func (c chan uint32, t uint32) {
    time.Sleep(3 * time.Second)
    // Log("Updating tursor to %v", t)
    c <- t
  } (j.tursor, tursor)
}

// client side load block data from file
func (j *Job) LoadBlock(block *Block) {
  f, err := os.Open(j.Meta.Name)
  checkError(err)
  var cursor int64 = int64(block.Start * MAX_SEND)
  size, err := f.ReadAt(block.Data, cursor)
  if err == io.EOF {
    block.Size = size
  } else if err != nil {
    Log("Failed to load block data file cursor %v", cursor)
    log.Fatal(err)
  }
  Log("Read %v bytes (offset %v) of file into block seq %v to %v", size, cursor, block.Start, block.End)
  if uint32(size) <= (block.End - block.Start) * MAX_SEND {
    log.Fatalf("Invalid data size %v for this block desiring more than %v\n", size, (block.End - block.Start) * MAX_SEND)
  }
  block.Full = true
}


// server side
func (j *Job) ParseSync(buf []byte) {
  seqEnd := 2 + j.Meta.seqLength
  seq := b2i(buf[2:seqEnd])
  if seq != j.Cursor {
    log.Printf("Updating tursor to %v\n", seq)
    j.Cursor = seq
  }
}

// parse data bytes and register it
func (j *Job) ParseData(buf []byte) {
  seqEnd := 2 + j.Meta.seqLength
  seq := b2i(buf[2:seqEnd])
  var bursor uint32 = seq / j.Meta.BlockSize
  if bursor > j.Meta.Blocks {
    log.Printf("Unknown seq %v %v/%v \n", seq, bursor, j.Meta.Blocks)
  }

  block, ok := j.Blocks[bursor]
  if !ok {
    log.Printf("Block %v is unavailable for this seq %v\n", bursor, seq)
  }

  if !block.On {
    block.On = true
  }

  v, ok := j.Map[seq]
  if !ok {
    // save seq
    block.CommitSequence(seq, buf[seqEnd:])
    j.Map[seq]  = 0
  } else if v == 0 {
    log.Printf("Duplicate data for job %d, seq %d\n", j.Meta.Id, seq)
  } else {
    block.CommitSequence(seq, buf[seqEnd:])
    j.Map[seq]  = 0
  }

  j.Count++
}

func NewJob() *Job {
  j := &Job{}
  j.Meta = &Meta{}
  j.Data = make(map[uint32][]byte)
  j.Finished = false
  j.Empty.Store(true)
  j.Chan = make(chan []byte, 1000)
  j.update = time.Now().Unix()
  j.Blocks = make(map[uint32]*Block, 0)
  j.Map = make(map[uint32]uint32)
  j.cursor = 0
  j.bursor = 0
  j.bytes = 0
  j.tsp = time.Now().Unix()
  j.tursor = make(chan uint32, 64)
  return j
}

// get job handler
func (i *Inventory) GetJob(id int) *Job {
  j, ok := i.Jobs[id]
  if !ok {
    Log("Creating new job with id: %d", id)
    j = NewJob()
    j.Meta.Id = id
    i.Jobs[id] = j
  }
  return j
}

// TODO remove this useless tick
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
