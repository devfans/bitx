# bitx
bytes transfer via udp.

Try it: [Download](https://github.com/devfans/bitx/releases)

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
