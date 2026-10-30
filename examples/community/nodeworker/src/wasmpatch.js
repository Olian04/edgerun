// Enforcing a memory limit on a guest.
//
// The WebAssembly JS API offers no way to cap how far a module's *own* linear
// memory may grow (only a memory the module imports can be given a maximum by
// the embedder), and node:wasi has no memory option either. What a host can do
// is rewrite the module's memory declaration: give it a `maximum` of the limit
// (or lower an existing one). memory.grow past the maximum then fails inside
// the guest exactly as it would on a machine out of memory, and the guest's
// runtime reacts (Go: fatal "out of memory", exit 2; Rust: abort) with no help
// from, and no way around, the host.
//
// Only the bytes that were verified against the digest are patched, in memory,
// after verification; the artifact itself is never altered.

const PAGE = 65536;

class Reader {
  constructor(b) { this.b = b; this.p = 0; }
  u8() { return this.b[this.p++]; }
  leb() {
    let r = 0, shift = 0, byte;
    do { byte = this.u8(); r += (byte & 0x7f) * 2 ** shift; shift += 7; } while (byte & 0x80);
    return r;
  }
}

function encodeLeb(n) {
  const out = [];
  do { let byte = n % 128; n = Math.floor(n / 128); if (n > 0) byte |= 0x80; out.push(byte); } while (n > 0);
  return out;
}

/**
 * @param {Uint8Array} wasm a module (already verified)
 * @param {number} maxPages the limit in 64 KiB pages
 * @returns {{bytes: Uint8Array, initialPages: number}|null} null if the module
 *   has no memory section of its own (nothing to cap) or cannot be parsed
 */
export function capMemory(wasm, maxPages) {
  if (wasm.length < 8 || wasm[0] !== 0 || wasm[1] !== 0x61 || wasm[2] !== 0x73 || wasm[3] !== 0x6d) return null;
  const r = new Reader(wasm);
  r.p = 8;
  while (r.p < wasm.length) {
    const sectionStart = r.p;
    const id = r.u8();
    const size = r.leb();
    const bodyStart = r.p;
    if (id === 5) {
      const count = r.leb();
      const out = [...encodeLeb(count)];
      let initialPages = 0;
      for (let i = 0; i < count; i++) {
        const flags = r.u8();
        if (flags & 0x06) return null; // shared or 64-bit memory: not wasip1 territory; leave alone
        const min = r.leb();
        const max = flags & 1 ? r.leb() : Infinity;
        initialPages = Math.max(initialPages, min);
        out.push(1, ...encodeLeb(min), ...encodeLeb(Math.min(max, Math.max(maxPages, min))));
      }
      const section = [5, ...encodeLeb(out.length), ...out];
      const patched = new Uint8Array(sectionStart + section.length + (wasm.length - (bodyStart + size)));
      patched.set(wasm.subarray(0, sectionStart), 0);
      patched.set(section, sectionStart);
      patched.set(wasm.subarray(bodyStart + size), sectionStart + section.length);
      return { bytes: patched, initialPages };
    }
    r.p = bodyStart + size;
  }
  return null;
}

export { PAGE };
