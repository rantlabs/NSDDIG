# NSDIG

`nsdig` scans text streams for IPv4 addresses, performs DNS lookups, and prefixes each line with the resolved hostname (or `NONAME` when no record exists). The tool is optimized for large files and supports both streaming stdout and buffered file outputs.

## Build

```bash
go build -o nsdig .
```

Use Go 1.21 or newer. The command above emits an executable named `nsdig` in the repository root.

## Help

```
./nsdig -h
Usage of nsdig:
  -file string
    	Path to an input file
  -ip string
    	Single IPv4 address to process
  -o string
    	Path to an output file (defaults to stdout)
  -workers int
    	Number of concurrent lookup workers (default 10)
```

## Usage

```bash
./nsdig -file INPUT.txt             # Read from a file
./nsdig -ip 8.8.8.8                 # Single address
cat INPUT.txt | ./nsdig             # Pipe/STDIN
./nsdig < INPUT.txt                 # File redirect / STDIN
./nsdig -file INPUT.txt -o out.txt  # Buffered write to a file (no streaming)
./nsdig -file INPUT.txt -workers 32 # Custom worker pool size
```

If no input source is provided (flag, pipe, or redirect), the program prints the help text.

## Sample Input & Output

Input text file:
```text
Edge01 | log | Internet  8.8.8.8    ...
Edge02 | log | Internet  1.1.1.1    ...
```

Output STDOUT:
```text
dns.google Edge01 | log | Internet  8.8.8.8    ...
one.one.one.one Edge02 | log | Internet  1.1.1.1    ...
```

When a lookup fails:
```text
NONAME Edge03 | log | Internet  203.0.113.10 ...
```

## Binary version available in this repo
```
nsdig_linux32	nsdig_linux64	nsdig_mac	nsdig_mac_arm64	nsdig_rpi_arm64	nsdig_rpi_armv6	nsdig_rpi_armv7	nsdig_win32.exe	nsdig_win64.exe
```

## Instructions for compiling binaries
```
GOOS=windows GOARCH=386 go build -o nsdig_win32.exe .    <-- You can call the file anything you want after the -o (output) flag
GOOS=windows GOARCH=amd64 go build -o nsdig_win64.exe .
GOOS=linux GOARCH=386 go build -o nsdig_linux32 .
GOOS=linux GOARCH=amd64 go build -o nsdig_linux64 .
GOOS=darwin GOARCH=amd64 go build -o nsdig_mac .
GOOS=linux GOARCH=arm GOARM=6 go build -o nsdig_rpi_armv6 .
GOOS=linux GOARCH=arm GOARM=7 go build -o nsdig_rpi_armv7 .
GOOS=linux GOARCH=arm64 go build -o nsdig_rpi_arm64 .
GOOS=darwin GOARCH=arm64 go build -o nsdig_mac_arm64 .
```
## Notes

- STDOUT mode streams each line as soon as its lookup completes; `-o` defers writing until the entire file is processed.
- Hostnames are resolved with Go’s `net.LookupAddr`; if a PTR record contains characters Go rejects, `nsdig` falls back to parsing the system `nslookup` output.
