# bitx
bytes transfer via udp

# Get Started

```
./bitx -h
Usage of ./bitx:
  -alsologtostderr
    	log to standard error as well as files
  -c int
    	numbers in batch (default 10)
  -d string
    	directory of files to send
  -f value
    	files to send, multiple -f is allowed
  -i string
    	host ip/remote receivor ip
  -log_backtrace_at value
    	when logging hits line file:N, emit a stack trace
  -log_dir string
    	If non-empty, write log files in this directory
  -logtostderr
    	log to standard error instead of files
  -m int
    	max datagram size (default 512)
  -p string
    	port (default "1200")
  -r int
    	repeat count for shards request (default 1)
  -s	serve as receivor
  -stderrthreshold value
    	logs at or above this threshold go to stderr
  -v value
    	log level for V logs
  -vmodule value
    	comma-separated list of pattern=N settings for file-filtered logging
```

Server (data receivor)

```
./bitx -s 
```
Client (data sender)

```
./bitx -f file1 -f file2
```
