package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

var (
	filePath = flag.String("file", "", "Path to an input file")
	ipInput  = flag.String("ip", "", "Single IPv4 address to process")
	outPath  = flag.String("o", "", "Path to an output file (defaults to stdout)")
	workers  = flag.Int("workers", runtime.NumCPU(), "Number of concurrent lookup workers")
)

var errNoInput = errors.New("no input provided; use -file, -ip, pipe data, or redirect a file")

func main() {
	log.SetFlags(0)
	flag.Parse()

	if err := validateInputOptions(); err != nil {
		log.Fatalf("input error: %v", err)
	}

	reader, cleanupReader, err := selectInputReader()
	if err != nil {
		if errors.Is(err, errNoInput) {
			flag.Usage()
			return
		}
		log.Fatalf("input error: %v", err)
	}
	defer cleanupReader()

	writer, cleanupWriter, stream, err := selectOutputWriter()
	if err != nil {
		log.Fatalf("output error: %v", err)
	}
	defer cleanupWriter()

	if stream {
		// Stream results line-by-line when writing to stdout.
		if err := processStreaming(reader, writer, *workers); err != nil {
			log.Fatalf("stream error: %v", err)
		}
		return
	}

	lines, jobs, err := readLines(reader)
	if err != nil {
		log.Fatalf("read error: %v", err)
	}

	results := runLookups(jobs, *workers)
	if err := emitOutput(writer, lines, results); err != nil {
		log.Fatalf("write error: %v", err)
	}
}

func validateInputOptions() error {
	if *filePath != "" && *ipInput != "" {
		return errors.New("only one of -file or -ip can be specified")
	}
	if *workers <= 0 {
		return errors.New("-workers must be positive")
	}
	return nil
}

func selectInputReader() (io.Reader, func(), error) {
	switch {
	case *filePath != "":
		f, err := os.Open(*filePath)
		if err != nil {
			return nil, func() {}, err
		}
		return f, func() { _ = f.Close() }, nil
	case *ipInput != "":
		data := *ipInput
		if !strings.HasSuffix(data, "\n") {
			data += "\n"
		}
		return strings.NewReader(data), func() {}, nil
	default:
		stat, err := os.Stdin.Stat()
		if err != nil {
			return nil, func() {}, err
		}
		if (stat.Mode() & os.ModeCharDevice) != 0 {
			return nil, func() {}, errNoInput
		}
		return os.Stdin, func() {}, nil
	}
}

func selectOutputWriter() (io.Writer, func(), bool, error) {
	if *outPath == "" {
		return os.Stdout, func() {}, true, nil
	}
	f, err := os.Create(*outPath)
	if err != nil {
		return nil, func() {}, false, err
	}
	return f, func() { _ = f.Close() }, false, nil
}

type lineInfo struct {
	text string
	ip   string
}

type lookupJob struct {
	index int
	ip    string
}

type lookupResult struct {
	index int
	host  string
}

type streamJob struct {
	index int
	line  string
	ip    string
}

type streamLine struct {
	text  string
	host  string
	ready bool
}

func readLines(r io.Reader) ([]lineInfo, []lookupJob, error) {
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 2*1024*1024)

	var (
		lines []lineInfo
		jobs  []lookupJob
	)

	for scanner.Scan() {
		line := scanner.Text()
		ip := extractIPv4(line)
		lines = append(lines, lineInfo{text: line, ip: ip})
		if ip != "" {
			jobs = append(jobs, lookupJob{index: len(lines) - 1, ip: ip})
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return lines, jobs, nil
}

func runLookups(jobs []lookupJob, workerCount int) map[int]string {
	if len(jobs) == 0 {
		return map[int]string{}
	}

	jobCh := make(chan lookupJob)
	resultCh := make(chan lookupResult)
	var wg sync.WaitGroup

	workerCount = min(workerCount, len(jobs))

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				host := lookupHostname(job.ip)
				resultCh <- lookupResult{index: job.index, host: host}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	go func() {
		for _, job := range jobs {
			jobCh <- job
		}
		close(jobCh)
	}()

	results := make(map[int]string, len(jobs))
	for res := range resultCh {
		results[res.index] = res.host
	}
	return results
}

func emitOutput(w io.Writer, lines []lineInfo, lookups map[int]string) error {
	bw := bufio.NewWriter(w)
	for idx, line := range lines {
		if host, ok := lookups[idx]; ok && host != "" {
			if _, err := fmt.Fprintf(bw, "%s %s\n", host, line.text); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(bw, line.text); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

func processStreaming(r io.Reader, w io.Writer, workerCount int) error {
	// Streaming keeps stdout responsive by wiring the scanner, lookup workers, and writer together.
	if workerCount <= 0 {
		workerCount = 1
	}

	lineCh := make(chan streamJob)
	jobCh := make(chan lookupJob)
	resultCh := make(chan lookupResult)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobCh {
				host := lookupHostname(job.ip)
				resultCh <- lookupResult{index: job.index, host: host}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	aggErrCh := make(chan error, 1)
	go func() {
		aggErrCh <- streamAggregator(lineCh, resultCh, w)
	}()

	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 2*1024*1024)

	idx := 0
	for scanner.Scan() {
		line := scanner.Text()
		ip := extractIPv4(line)
		lineCh <- streamJob{index: idx, line: line, ip: ip}
		if ip != "" {
			jobCh <- lookupJob{index: idx, ip: ip}
		}
		idx++
	}
	close(lineCh)
	close(jobCh)

	if err := scanner.Err(); err != nil {
		return err
	}

	return <-aggErrCh
}

func streamAggregator(lineCh <-chan streamJob, resultCh <-chan lookupResult, w io.Writer) error {
	// Aggregator preserves input order by buffering lines until their DNS work completes.
	bw := bufio.NewWriter(w)
	defer bw.Flush()

	pending := make(map[int]*streamLine)
	next := 0

	for lineCh != nil || resultCh != nil {
		select {
		case job, ok := <-lineCh:
			if !ok {
				lineCh = nil
				continue
			}
			entry := &streamLine{text: job.line}
			if job.ip == "" {
				entry.ready = true
			}
			pending[job.index] = entry
			if entry.ready {
				if err := flushReadyLines(bw, pending, &next); err != nil {
					return err
				}
			}
		case res, ok := <-resultCh:
			if !ok {
				resultCh = nil
				continue
			}
			entry, exists := pending[res.index]
			if !exists {
				// Should not happen, but guard to avoid panic.
				entry = &streamLine{}
				pending[res.index] = entry
			}
			entry.host = res.host
			entry.ready = true
			if err := flushReadyLines(bw, pending, &next); err != nil {
				return err
			}
		}
	}
	return nil
}

func flushReadyLines(bw *bufio.Writer, pending map[int]*streamLine, next *int) error {
	// Flush consecutive ready lines so stdout appears continuous even though lookups are async.
	wrote := false
	for {
		entry, ok := pending[*next]
		if !ok || !entry.ready {
			break
		}
		if entry.host != "" {
			if _, err := fmt.Fprintf(bw, "%s %s\n", entry.host, entry.text); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintln(bw, entry.text); err != nil {
				return err
			}
		}
		delete(pending, *next)
		*next++
		wrote = true
	}
	if wrote {
		return bw.Flush()
	}
	return nil
}

var ipv4Regex = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)

func extractIPv4(line string) string {
	matches := ipv4Regex.FindAllString(line, -1)
	for _, candidate := range matches {
		if ip := validateIPv4(candidate); ip != "" {
			return ip
		}
	}
	return ""
}

func validateIPv4(candidate string) string {
	ip := net.ParseIP(candidate)
	if ip == nil {
		return ""
	}
	ipv4 := ip.To4()
	if ipv4 == nil {
		return ""
	}
	return ipv4.String()
}

func lookupHostname(ip string) string {
	// Prefer Go's resolver for speed, then fall back to parsing the system nslookup
	// output to handle PTR records with characters (e.g., slash) Go refuses.
	if host := lookupViaGo(ip); host != "" {
		return host
	}
	if host := lookupViaNSLookup(ip); host != "" {
		return host
	}
	return "NONAME"
}

func lookupViaGo(ip string) string {
	names, err := net.LookupAddr(ip)
	if err != nil || len(names) == 0 {
		return ""
	}
	return strings.TrimSuffix(names[0], ".")
}

func lookupViaNSLookup(ip string) string {
	cmd := exec.Command("nslookup", ip)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return ""
	}
	return parseNSLookup(bytes.NewReader(output))
}

func parseNSLookup(r io.Reader) string {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.Index(line, "name ="); idx != -1 {
			host := strings.TrimSpace(line[idx+len("name ="):])
			return strings.TrimSuffix(host, ".")
		}
		if idx := strings.Index(line, "Name:"); idx != -1 {
			host := strings.TrimSpace(line[idx+len("Name:"):])
			return strings.TrimSuffix(host, ".")
		}
	}
	return ""
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
