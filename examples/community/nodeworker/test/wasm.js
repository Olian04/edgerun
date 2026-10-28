// Tiny hand-assembled wasip1 guests, so the worker can be tested without a
// Go/Rust toolchain. Every module is a valid wagi-1 command: it imports
// wasi_snapshot_preview1.{fd_write,proc_exit} and exports `_start` + `memory`.
const leb = (n) => { const o = []; do { let b = n & 0x7f; n >>>= 7; if (n) b |= 0x80; o.push(b); } while (n); return o; };
const sleb = (n) => { const o = []; for (;;) { const b = n & 0x7f; n >>= 7; if ((n === 0 && !(b & 0x40)) || (n === -1 && (b & 0x40))) { o.push(b); return o; } o.push(b | 0x80); } };
const str = (s) => [...leb(Buffer.byteLength(s)), ...Buffer.from(s)];
const section = (id, body) => [id, ...leb(body.length), ...body];
const vec = (items) => [...leb(items.length), ...items.flat()];

export const i32 = (n) => [0x41, ...sleb(n)];
const FD_WRITE = 0, PROC_EXIT = 1;
const call = (f) => [0x10, f];
const drop = [0x1a];
const loop = (body) => [0x03, 0x40, ...body, 0x0c, 0, 0x0b];

/** Write texts[idx] to fd (1 = stdout, 2 = stderr). iovec idx is at 8*idx; the texts start at 256. */
export const write = (fd, idx = 0) => [...i32(fd), ...i32(8 * idx), ...i32(1), ...i32(200), ...call(FD_WRITE), ...drop];
export const exit = (code) => [...i32(code), ...call(PROC_EXIT)];

/**
 * @param {number[]} body  _start's instructions (without trailing `end`)
 * @param {{texts?: string[], memoryPages?: number, maxPages?: number, imports?: number[][]}} o
 */
export function guest(body, { texts = [], memoryPages = 1, maxPages, imports = [] } = {}) {
  const mem = maxPages === undefined ? [0, ...leb(memoryPages)] : [1, ...leb(memoryPages), ...leb(maxPages)];
  const data = Buffer.alloc(256);
  const blobs = [];
  let off = 256;
  texts.forEach((t, i) => {
    const b = Buffer.from(t);
    data.writeUInt32LE(off, 8 * i); data.writeUInt32LE(b.length, 8 * i + 4);
    blobs.push(b); off += b.length;
  });
  const payload = Buffer.concat([data, ...blobs]);
  return Uint8Array.from([
    0, 0x61, 0x73, 0x6d, 1, 0, 0, 0,
    ...section(1, vec([[0x60, 0, 0], [0x60, 4, 0x7f, 0x7f, 0x7f, 0x7f, 1, 0x7f], [0x60, 1, 0x7f, 0]])),
    ...section(2, vec([
      [...str("wasi_snapshot_preview1"), ...str("fd_write"), 0, 1],
      [...str("wasi_snapshot_preview1"), ...str("proc_exit"), 0, 2],
      ...imports,
    ])),
    ...section(3, vec([[0]])),
    ...section(5, vec([mem])),
    ...section(7, vec([[...str("memory"), 2, 0], [...str("_start"), 0, 2 + imports.length]])),
    ...section(10, vec([[...leb(body.length + 2), 0, ...body, 0x0b]])),
    ...section(11, vec([[0, ...i32(0), 0x0b, ...leb(payload.length), ...payload]])),
  ]);
}

const OK = "Content-Type: text/plain\n\nhi";
export const guests = {
  /** a valid response */
  respond: (text = OK) => guest(write(1), { texts: [text] }),
  /** writes `log` to stderr `times` times, then a valid response */
  logs: (log, times = 1) => guest([...Array(times)].flatMap(() => write(2, 0)).concat(write(1, 1)), { texts: [log, OK] }),
  exit: (code) => guest(exit(code)),
  /** grows memory by `pages` then spins: no system call at all */
  growThenSpin: (pages) => guest([...i32(pages), 0x40, 0, ...drop, ...loop([])]),
  /** traps (unreachable) if memory.grow(pages) fails */
  growOrTrap: (pages) => guest([...i32(pages), 0x40, 0, ...i32(-1), 0x46, 0x04, 0x40, 0x00, 0x0b, ...write(1)], { texts: [OK] }),
  spin: () => guest(loop([])),
  /** writes text forever */
  flood: (text = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") => guest(loop(write(1)), { texts: [text] }),
  unreachable: () => guest([0x00]),
  nothing: () => guest([]),
};

/** imports env.f: not a wagi-1 module */
export const importsEnv = () => guest([], { imports: [[...str("env"), ...str("f"), 0, 0]] });
