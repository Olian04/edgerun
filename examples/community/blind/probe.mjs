// Raw-socket probes of the frontend at 127.0.0.1:18080 for cases the suite may not cover.
import net from "node:net";

function raw(bytes, wait = 400) {
  return new Promise((res) => {
    const c = net.connect(18080, "127.0.0.1", () => c.write(Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes, "latin1")));
    const d = [];
    c.on("data", (x) => d.push(x));
    c.on("error", () => {});
    setTimeout(() => {
      c.destroy();
      res(Buffer.concat(d).toString("latin1").split("\r\n\r\n")[0].replace(/\r\n/g, " | "));
    }, wait);
  });
}
const cases = {
  "OPTIONS *": "OPTIONS * HTTP/1.1\r\nHost: x\r\n\r\n",
  "GET *": "GET * HTTP/1.1\r\nHost: x\r\n\r\n",
  "absolute-form": "GET http://x/a HTTP/1.1\r\nHost: x\r\n\r\n",
  CONNECT: "CONNECT x:80 HTTP/1.1\r\nHost: x:80\r\n\r\n",
  upgrade: "GET /ws HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
  "http/1.0 no host": "GET /a HTTP/1.0\r\n\r\n",
  "no host 1.1": "GET /a HTTP/1.1\r\n\r\n",
  "bad pct": "GET /a%zz HTTP/1.1\r\nHost: x\r\n\r\n",
  "space in target": "GET /a b HTTP/1.1\r\nHost: x\r\n\r\n",
  "ctl in header": "GET /a HTTP/1.1\r\nHost: x\r\nX-A: a\x01b\r\n\r\n",
  "DEL in header": "GET /a HTTP/1.1\r\nHost: x\r\nX-A: a\x7fb\r\n\r\n",
  "obs-fold": "GET /a HTTP/1.1\r\nHost: x\r\nX-A: a\r\n b\r\n\r\n",
  "two content-length": "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab",
  "cl+te": "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
  "huge cl": "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999999\r\n\r\n",
  "expect": "POST /a HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999\r\nExpect: 100-continue\r\n\r\n",
  "dup request-id": "GET /a HTTP/1.1\r\nHost: x\r\nX-Request-Id: a\r\nX-Request-Id: b\r\n\r\n",
  "long request-id": "GET /a HTTP/1.1\r\nHost: x\r\nX-Request-Id: " + "a".repeat(129) + "\r\n\r\n",
  "128 request-id": "GET /a HTTP/1.1\r\nHost: x\r\nX-Request-Id: " + "a".repeat(128) + "\r\n\r\n",
  "empty request-id": "GET /a HTTP/1.1\r\nHost: x\r\nX-Request-Id:\r\n\r\n",
  "highbit request-id": Buffer.from("GET /a HTTP/1.1\r\nHost: x\r\nX-Request-Id: caf\xe9\r\n\r\n", "latin1"),
  "fragment": "GET /a#frag HTTP/1.1\r\nHost: x\r\n\r\n",
  "%00": "GET /a%00 HTTP/1.1\r\nHost: x\r\n\r\n",
  "%2F": "GET /a%2Fb HTTP/1.1\r\nHost: x\r\n\r\n",
  "query ctl": "GET /a?x=%00 HTTP/1.1\r\nHost: x\r\n\r\n",
};
for (const [k, v] of Object.entries(cases)) console.log(k.padEnd(20), "=>", (await raw(v)).slice(0, 230));
