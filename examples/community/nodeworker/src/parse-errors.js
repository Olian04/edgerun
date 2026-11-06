// Connect-ES answers a request message it cannot decode (for us: a string that
// is not UTF-8, such as a path "/\xff") with `internal`, whatever the cause; the
// protocol wants `invalid_argument`, with no reason ("HTTP messages": a worker
// answers invalid_argument to a path that is not text). The decoding happens
// inside the framework, before any handler or interceptor runs, so the only
// place left to correct the code is the end-of-stream message of a Connect
// streaming response: the envelope with flag 0x02 carrying `{"error":{...}}`.
// Nothing else is touched: a decode error is recognised by the framework's own
// message prefix, "parse binary: ".
import { gunzipSync } from "node:zlib";

const COMPRESSED = 0x01, END_STREAM = 0x02;

function fix(flags, data) {
  let json = data;
  if (flags & COMPRESSED) {
    try { json = gunzipSync(data); } catch { return null; } // another codec: leave it as it is
  }
  let end;
  try { end = JSON.parse(Buffer.from(json).toString("utf8")); } catch { return null; }
  if (end?.error?.code !== "internal" || typeof end.error.message !== "string" || !end.error.message.startsWith("parse binary: ")) return null;
  end.error = { code: "invalid_argument", message: end.error.message };
  return Buffer.from(JSON.stringify(end));
}

/** The chunk with such an end-of-stream error corrected, or the chunk itself. */
export function correctParseErrors(chunk) {
  if (typeof chunk === "string" || chunk.length < 5) return chunk;
  const parts = [];
  let changed = false;
  for (let at = 0; at < chunk.length;) {
    if (chunk.length - at < 5) return chunk;
    const flags = chunk[at];
    const len = Buffer.from(chunk.buffer, chunk.byteOffset + at + 1, 4).readUInt32BE(0);
    if (chunk.length - at - 5 < len) return chunk; // an envelope split across writes: not ours to read
    const data = chunk.subarray(at + 5, at + 5 + len);
    const fixed = flags & END_STREAM ? fix(flags, data) : null;
    if (fixed) {
      const head = Buffer.alloc(5);
      head[0] = END_STREAM; // uncompressed now
      head.writeUInt32BE(fixed.length, 1);
      parts.push(head, fixed);
      changed = true;
    } else {
      parts.push(chunk.subarray(at, at + 5 + len));
    }
    at += 5 + len;
  }
  return changed ? Buffer.concat(parts) : chunk;
}

/** Wraps res.write of a Connect response so that decode errors go out as invalid_argument. */
export function invalidArgumentOnParseError(res) {
  const write = res.write.bind(res);
  res.write = (chunk, ...rest) => write(correctParseErrors(chunk), ...rest);
}
