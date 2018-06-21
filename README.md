# bitx
[![Build Status](https://travis-ci.org/devfans/bitx.svg?branch=master)](https://travis-ci.org/devfans/bitx)
[![Go Report Card](https://goreportcard.com/badge/github.com/devfans/bitx)](https://goreportcard.com/report/github.com/devfans/bitx)
bytes transfer via udp.

Currently supports: linux, macOS, windows. Try it: [Download](https://github.com/devfans/bitx/releases) 

In some situations, a tcp connection based data transfer is unstable and has a low efficient, while with udp protocal, it can use limited resource to tranfer bytes as fast as possible. The main issue with udp tranfer is high packets loss, and no ways to get rid of it. But there are some suggestions to optimize it:
+ Adjust the datagram size(with ```-m```) accordingly. Normally smaller datagram get lower data loss rate, when transfering data through long distance since a frame loss can cause the whole datagram to be dropped.
+ Adjust the OS level udp buffer size. Please refer to this [gist](https://gist.github.com/devfans/b19516ec5616cacfe59156194f9b68a2)

# Get Started

```
./bitx -h
Usage of ./bitx:
  -c int
    	numbers in batch (default 10)
  -d string
    	directory of files to send
  -f value
    	files to send, multiple -f is allowed
  -i string
    	host ip/remote receivor ip
  -m int
    	max datagram size (default 512)
  -p string
    	port (default "1200")
  -r int
    	repeat count for shards request (default 1)
  -s	serve as receivor
```

Data Receivor:

```
./bitx -s 
```

Data Sender:

```
./bitx -f file1 -f file2
```
