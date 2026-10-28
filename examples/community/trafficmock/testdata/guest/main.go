// Command guest is the wasip1 function the real-stack tests of edgerun-trafficmock
// deploy. What it does depends on the path, mirroring FakeIngress:
//
//	/status/N   answer N
//	/sleep/MS   sleep, then 200
//	/bytes/N    200 with N bytes
//	/echo       200 with the request body
//	/trap       exit 3 (FUNCTION_TRAPPED)
//	/spin       burn CPU forever (the budget must stop it)
//	/headers    200 listing the CGI environment the request produced
//	anything else: 200 "ok"
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	parts := strings.Split(strings.Trim(os.Getenv("PATH_INFO"), "/"), "/")
	arg := 0
	if len(parts) > 1 {
		arg, _ = strconv.Atoi(parts[1])
	}
	out := bufio.NewWriterSize(os.Stdout, 64<<10)
	defer out.Flush()
	switch parts[0] {
	case "status":
		fmt.Fprintf(out, "Status: %d\nContent-Type: text/plain\n\nstatus %d\n", arg, arg)
	case "sleep":
		time.Sleep(time.Duration(arg) * time.Millisecond)
		fmt.Fprint(out, "Content-Type: text/plain\n\nslept\n")
	case "bytes":
		fmt.Fprint(out, "Content-Type: application/octet-stream\n\n")
		chunk := []byte(strings.Repeat("x", 4096))
		for left := arg; left > 0; {
			n := min(left, len(chunk))
			out.Write(chunk[:n])
			left -= n
		}
	case "echo":
		body, _ := io.ReadAll(os.Stdin)
		fmt.Fprint(out, "Content-Type: application/octet-stream\n\n")
		out.Write(body)
	case "trap":
		out.Flush()
		os.Exit(3)
	case "spin":
		var x uint64
		for {
			x++
			if x == 1<<62 {
				fmt.Fprintln(os.Stderr, "unreachable")
			}
		}
	case "headers":
		body, _ := io.ReadAll(os.Stdin)
		var env []string
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "HTTP_") || strings.HasPrefix(e, "CONTENT_") || strings.HasPrefix(e, "REQUEST_") ||
				strings.HasPrefix(e, "PATH_INFO") || strings.HasPrefix(e, "QUERY_") || strings.HasPrefix(e, "SERVER_NAME") || strings.HasPrefix(e, "REMOTE_ADDR") {
				env = append(env, e)
			}
		}
		sort.Strings(env)
		fmt.Fprint(out, "Content-Type: text/plain\n\n")
		fmt.Fprintf(out, "%s\nbody_len=%d\n", strings.Join(env, "\n"), len(body))
	default:
		fmt.Fprint(out, "Content-Type: text/plain\n\nok\n")
	}
}
