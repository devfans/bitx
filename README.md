# bitx

[![Build Status](https://travis-ci.org/devfans/bitx.svg?branch=master)](https://travis-ci.org/devfans/bitx)
[![Go Report Card](https://goreportcard.com/badge/github.com/devfans/bitx)](https://goreportcard.com/report/github.com/devfans/bitx) [![Join the chat at https://gitter.im/devfans/bitx](https://badges.gitter.im/devfans/bitx.svg)](https://gitter.im/devfans/bitx?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Bytes transfer via pure udp. Currently supports: linux, macOS, windows. Try it: [Download](https://github.com/devfans/bitx/releases) 

In some situations, a tcp connection based data transfer is unstable and has a low efficient, while with udp protocal, it can use limited resource to tranfer bytes as fast as possible. The main issue with udp tranfer is high packets loss, and no ways to get rid of it. But there are some suggestions to optimize it:
+ Adjust the datagram size(with ```-m```) accordingly. Normally smaller datagram get lower data loss rate, when transfering data through long distance since a frame loss can cause the whole datagram to be dropped.
+ Adjust the OS level udp buffer size. Please refer to this [gist](https://gist.github.com/devfans/b19516ec5616cacfe59156194f9b68a2)

# Benchmark

Transfer a file about 2.1GB from China to Japan through public Internet

Avg speed: 3934 KB/s
<p align="center">
  <img src="https://raw.githubusercontent.com/devfans/bitx/master/benchmark_avg.png" alt="Sublime's custom image" width="200" height="200"/>
</p>

Peak at: 30MB (Potential optimization of logic can be applied)
<p align="center">
  <img src="https://raw.githubusercontent.com/devfans/bitx/master/benchmark_peak.png" alt="Sublime's custom image" width="200" height="200"/>
  
# Get Started

```
./bitx -h
Usage of ./bitx:
  -b int
    	datagrams to store in one block, set on client side (default 1000000)
  -bs int
    	blocks in memory, set on client size (default 3)
  -c int
    	batch size (default 10)
  -d string
    	directory of files to send
  -f value
    	files to send, multiple -f is allowed
  -i string
    	host ip/remote receivor ip
  -loop int
    	session loop check interval (default 1)
  -m int
    	max datagram size in bytes, set on both size (default 512)
  -p string
    	port (default "1200")
  -r int
    	repeat count for shards request (default 1)
  -s	serve as receivor
  -t	Optimize output for terminal
  -wait int
    	data max wait time before send new request for it (default 3)
```

Data Receivor:

```
./bitx -s 
```

Data Sender:

```
./bitx -f file1 -f file2
```

# Terminal Display Option(-t) (The display is valid only for signle file transfer)

```
bitx -f test.data -bs 1 -t
File test.data Seqs: 2138928/2138928 Blocks(Size 1000000MB): 2/3 Parallel: 1 (1073 MB)  
------------------------------------------------------------------
Sent job meta for test.data id 0
Allocated block 0 start 0 end 999999
Read 502000000 bytes (offset 0) of file into block seq 0 to 999999
Allocated block 1 start 1000000 end 1999999
Read 502000000 bytes (offset 502000000) of file into block seq 1000000 to 1999999
Allocated block 2 start 2000000 end 2138927
Read 69741824 bytes (offset 1004000000) of file into block seq 2000000 to 2138927
Job total length 1073741824
Finished job test.data
All jobs are finished, now ending
Transfered [============================================] 100% 10699KB/s
```
