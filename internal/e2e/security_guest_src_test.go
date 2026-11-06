package e2e

// secguestSource is a wagi-2 guest written for the security tests. Its behaviour is
// chosen by the request path, like conformance/guest/netguest, but it can also
// follow redirects (what a real HTTP client library does) and probe what a
// sandboxed module should not be able to do: read files, see the host's
// environment, open sockets, run programs, hold secrets next to another
// invocation, and remember memory.
const secguestSource = `package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

//go:wasmimport edgerun_http fetch
func fetch(req, n uint32) int32

//go:wasmimport edgerun_http fetch_read
func fetchRead(dst, n uint32) int32

func addr(b []byte) uint32 {
	if len(b) == 0 {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(&b[0])))
}

func call(req []byte) (int32, []byte) {
	code := fetch(addr(req), uint32(len(req)))
	runtime.KeepAlive(req)
	if code < 0 {
		return code, nil
	}
	var out []byte
	buf := make([]byte, 4096)
	for {
		n := fetchRead(addr(buf), uint32(len(buf)))
		runtime.KeepAlive(buf)
		if n <= 0 {
			return code, out
		}
		out = append(out, buf[:n]...)
	}
}

var keep [][]byte

func main() {
	q, _ := url.ParseQuery(os.Getenv("QUERY_STRING"))
	num := func(k string, def int) int {
		if v, err := strconv.Atoi(q.Get(k)); err == nil {
			return v
		}
		return def
	}
	body, _ := io.ReadAll(os.Stdin)
	fmt.Print("Content-Type: text/plain\n\n")

	switch os.Getenv("PATH_INFO") {
	case "/follow":
		// What an HTTP client library does: follow redirects, each hop a new fetch.
		req := body
		for hop := 0; hop < num("max", 6); hop++ {
			first, _, _ := strings.Cut(string(req), "\n")
			code, resp := call(req)
			fmt.Printf("hop=%d req=%s code=%d\n", hop, first, code)
			if code < 0 {
				break
			}
			head, rest, _ := strings.Cut(string(resp), "\n\n")
			status, loc := 0, ""
			for i, l := range strings.Split(head, "\n") {
				if i == 0 {
					fmt.Sscanf(l, "Status: %d", &status)
					continue
				}
				if k, v, ok := strings.Cut(l, ":"); ok && strings.EqualFold(k, "location") {
					loc = strings.TrimSpace(v)
				}
			}
			fmt.Printf("hop=%d status=%d location=%s\n", hop, status, loc)
			if status < 300 || status > 399 || loc == "" {
				fmt.Printf("body=%s\n", rest)
				break
			}
			fields := strings.Fields(first)
			cu, err := url.Parse(fields[1])
			lu, err2 := url.Parse(loc)
			if err != nil || err2 != nil {
				fmt.Printf("unparsable location\n")
				break
			}
			req = []byte("GET " + cu.ResolveReference(lu).String() + "\n\n")
		}
	case "/files":
		paths := []string{"/", "/etc/passwd", "/etc/hosts", "/etc/resolv.conf", "/proc/self/environ", "/proc/self/maps", "/proc/1/environ",
			"/proc/self/cmdline", "/tmp", "/root", "/home", ".", "..", "../..", "../../etc/passwd", "/dev/null", "/dev/urandom", "/sys", "/usr", "/workspace"}
		for _, p := range paths {
			b, err := os.ReadFile(p)
			fmt.Printf("read %s: n=%d err=%v\n", p, len(b), err)
			es, err := os.ReadDir(p)
			fmt.Printf("readdir %s: n=%d err=%v\n", p, len(es), err)
		}
		f, err := os.Create("/tmp/escape")
		fmt.Printf("create /tmp/escape: ok=%v err=%v\n", f != nil, err)
		err = os.WriteFile("/escape", []byte("x"), 0o644)
		fmt.Printf("write /escape: err=%v\n", err)
		wd, err := os.Getwd()
		fmt.Printf("getwd: %q err=%v\n", wd, err)
	case "/env":
		env := os.Environ()
		sort.Strings(env)
		fmt.Printf("%s\n", strings.Join(env, "\n"))
	case "/dial":
		c, err := net.DialTimeout("tcp", q.Get("addr"), 2*time.Second)
		fmt.Printf("dial %s: conn=%v err=%v\n", q.Get("addr"), c != nil, err)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		fmt.Printf("listen: ok=%v err=%v\n", l != nil, err)
	case "/exec":
		out, err := exec.Command("/bin/sh", "-c", "id").Output()
		fmt.Printf("exec: out=%q err=%v\n", out, err)
	case "/stderrflood":
		line := bytes.Repeat([]byte("E"), 1023)
		line = append(line, '\n')
		for i := 0; i < num("mb", 64)*1024; i++ {
			os.Stderr.Write(line)
		}
		fmt.Printf("done\n")
	case "/hold":
		time.Sleep(time.Duration(num("ms", 200)) * time.Millisecond)
		env := os.Environ()
		sort.Strings(env)
		fmt.Printf("%s\nbody=%s\n", strings.Join(env, "\n"), body)
	case "/plant":
		n := num("mb", 32) << 20
		b := make([]byte, n)
		pat := []byte(q.Get("p"))
		for i := 0; i+len(pat) <= n; i += len(pat) {
			copy(b[i:], pat)
		}
		keep = append(keep, b)
		fmt.Printf("planted %d bytes\n", n)
	case "/scan":
		n := num("mb", 64) << 20
		b := make([]byte, n)
		nz := 0
		for _, c := range b {
			if c != 0 {
				nz++
			}
		}
		fmt.Printf("scanned=%d nonzero=%d pattern=%d\n", n, nz, bytes.Count(b, []byte(q.Get("p"))))
	case "/spinsys":
		for {
			_ = time.Now()
		}
	case "/sleepforever":
		time.Sleep(time.Hour)
	case "/noop":
		fmt.Printf("noop\n")
	default:
		fmt.Printf("unknown\n")
	}
}
`
