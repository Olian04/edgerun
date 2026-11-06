// Allow-list entries and network-service coverage entries, from docs/protocol.md ("Network access (egress)").
import { createHash } from "node:crypto";

// ---- addresses -------------------------------------------------------------------------------------
export function parseIPv4(s) {
  const m = /^(0|[1-9]\d{0,2})\.(0|[1-9]\d{0,2})\.(0|[1-9]\d{0,2})\.(0|[1-9]\d{0,2})$/.exec(s);
  if (!m) return null;
  const o = m.slice(1).map(Number);
  if (o.some((x) => x > 255)) return null;
  return o;
}

/** Parse an IPv6 address (no brackets, no zone) to 8 groups, or null. */
export function parseIPv6(s) {
  if (!/^[0-9a-fA-F:.]+$/.test(s) || s.includes(":::")) return null;
  let tail = [];
  const lastColon = s.lastIndexOf(":");
  if (s.slice(lastColon + 1).includes(".")) {
    const v4 = parseIPv4(s.slice(lastColon + 1));
    if (!v4) return null;
    tail = [(v4[0] << 8) | v4[1], (v4[2] << 8) | v4[3]];
    s = s.slice(0, lastColon + 1) + "0:0"; // placeholder groups replaced below
  }
  const parts = s.split("::");
  if (parts.length > 2) return null;
  const grp = (t) => (t === "" ? [] : t.split(":"));
  const head = grp(parts[0]);
  const rest = parts.length === 2 ? grp(parts[1]) : [];
  const all = [...head, ...rest];
  if (all.some((g) => !/^[0-9a-fA-F]{1,4}$/.test(g))) return null;
  let groups;
  if (parts.length === 2) {
    const fill = 8 - head.length - rest.length;
    if (fill < 1) return null;
    groups = [...head, ...Array(fill).fill("0"), ...rest];
  } else {
    if (head.length !== 8) return null;
    groups = head;
  }
  const n = groups.map((g) => parseInt(g, 16));
  if (tail.length) {
    n[6] = tail[0];
    n[7] = tail[1];
  }
  return n.length === 8 ? n : null;
}

export function formatIPv6(n) {
  // longest run of zeros (length >= 2), leftmost wins
  let best = -1, bestLen = 0;
  for (let i = 0; i < 8; ) {
    if (n[i] === 0) {
      let j = i;
      while (j < 8 && n[j] === 0) j++;
      if (j - i > bestLen) { best = i; bestLen = j - i; }
      i = j;
    } else i++;
  }
  if (bestLen < 2) return n.map((g) => g.toString(16)).join(":");
  const h = n.slice(0, best).map((g) => g.toString(16)).join(":");
  const t = n.slice(best + bestLen).map((g) => g.toString(16)).join(":");
  return `${h}::${t}`;
}

const isMapped = (n) => n.slice(0, 5).every((x) => x === 0) && n[5] === 0xffff;
const mappedToV4 = (n) => [n[6] >> 8, n[6] & 255, n[7] >> 8, n[7] & 255];

// ---- host names ------------------------------------------------------------------------------------
function validHostName(h) {
  if (h.length === 0 || h.length > 253) return false;
  const labels = h.split(".");
  for (const l of labels) {
    if (l.length < 1 || l.length > 63) return false;
    if (!/^[a-z0-9-]+$/.test(l) || l.startsWith("-") || l.endsWith("-")) return false;
  }
  const last = labels[labels.length - 1];
  if (/^(\d+|0x[0-9a-f]*)$/.test(last)) return false; // ends in a number: legacy address spelling
  return true;
}

/**
 * Parse an allow-list entry. Returns {kind:"host"|"wild"|"ip", host, port, canon} or null.
 * host: lower-case name (for wild: the suffix); for ip: the canonical address text without brackets.
 */
export function parseAllow(raw) {
  if (typeof raw !== "string" || raw === "") return null;
  if (!/^[\x21-\x7e]+$/.test(raw)) return null;
  let s = raw.toLowerCase();
  let wild = false;
  if (s.startsWith("*.")) {
    wild = true;
    s = s.slice(2);
  }
  let hostPart, port = "";
  if (s.startsWith("[")) {
    if (wild) return null;
    const e = s.indexOf("]");
    if (e < 0) return null;
    hostPart = s.slice(1, e);
    const after = s.slice(e + 1);
    if (after !== "") {
      if (!after.startsWith(":")) return null;
      port = after.slice(1);
      if (!/^[1-9]\d{0,4}$/.test(port) || Number(port) > 65535) return null;
    }
    const g = parseIPv6(hostPart);
    if (!g) return null;
    if (hostPart.includes("%")) return null;
    if (isMapped(g)) {
      const v4 = mappedToV4(g);
      return mk("ip", v4.join("."), port, false);
    }
    return mk("ip", formatIPv6(g), port, true);
  }
  const colons = s.split(":").length - 1;
  if (colons > 1) return null;
  if (colons === 1) {
    const i = s.indexOf(":");
    port = s.slice(i + 1);
    s = s.slice(0, i);
    if (!/^[1-9]\d{0,4}$/.test(port) || Number(port) > 65535) return null;
  }
  const v4 = parseIPv4(s);
  if (v4) {
    if (wild) return null;
    return mk("ip", v4.join("."), port, false);
  }
  if (!validHostName(s)) return null;
  return mk(wild ? "wild" : "host", s, port, false);

  function mk(kind, host, port, v6) {
    const h = v6 ? `[${host}]` : host;
    const canon = (kind === "wild" ? "*." : "") + h + (port ? ":" + port : "");
    return { kind, host, port, canon, v6 };
  }
}

/** Canonical, deduplicated, sorted allow list; throws a string message on a malformed entry. */
export function canonicalAllow(list) {
  if (list.length > 64) throw new Error("at most 64 egress entries");
  const out = new Set();
  for (const e of list) {
    const p = parseAllow(e);
    if (!p) throw new Error(`malformed egress entry ${JSON.stringify(e)}`);
    out.add(p.canon);
  }
  return [...out].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
}

// ---- coverage --------------------------------------------------------------------------------------
function ipToBig(g) {
  let v = 0n;
  for (const x of g) v = (v << 16n) | BigInt(x);
  return v;
}
function v4ToBig(o) {
  return o.reduce((a, b) => (a << 8n) | BigInt(b), 0n);
}

/**
 * Parse a coverage entry: "*", host, "*.suffix", address, CIDR. Returns
 * {kind:"all"|"host"|"wild"|"ip"|"range", ..., canon} or null.
 */
export function parseCover(raw) {
  if (typeof raw !== "string" || raw === "") return null;
  if (raw === "*") return { kind: "all", canon: "*" };
  const s = raw.toLowerCase();
  if (s.includes("/")) {
    const [addr, bits, extra] = s.split("/");
    if (extra !== undefined || !/^\d{1,3}$/.test(bits) || (bits.length > 1 && bits.startsWith("0"))) return null;
    const nb = Number(bits);
    const a6 = addr.startsWith("[") && addr.endsWith("]") ? addr.slice(1, -1) : addr;
    const v4 = parseIPv4(addr);
    if (v4) {
      if (nb > 32) return null;
      const mask = nb === 0 ? 0n : ((1n << BigInt(nb)) - 1n) << BigInt(32 - nb);
      const net = v4ToBig(v4) & mask;
      const o = [24n, 16n, 8n, 0n].map((sh) => Number((net >> sh) & 255n));
      return { kind: "range", v6: false, bits: nb, net, canon: `${o.join(".")}/${nb}` };
    }
    const g = parseIPv6(a6);
    if (!g || a6.includes("%")) return null;
    if (nb > 128) return null;
    const full = (1n << 128n) - 1n;
    const mask = nb === 0 ? 0n : (full >> BigInt(128 - nb)) << BigInt(128 - nb);
    const net = ipToBig(g) & mask;
    const ng = [];
    for (let i = 7; i >= 0; i--) ng.push(Number((net >> BigInt(i * 16)) & 0xffffn));
    return { kind: "range", v6: true, bits: nb, net, canon: `${formatIPv6(ng)}/${nb}` };
  }
  let w = false;
  let h = s;
  if (h.startsWith("*.")) {
    w = true;
    h = h.slice(2);
  }
  if (h.startsWith("[") && h.endsWith("]")) h = h.slice(1, -1);
  if (!w) {
    const v4 = parseIPv4(h);
    if (v4) return { kind: "ip", v6: false, big: v4ToBig(v4), canon: v4.join(".") };
    if (h.includes(":")) {
      const g = parseIPv6(h);
      if (!g || h.includes("%")) return null;
      if (isMapped(g)) {
        const v = mappedToV4(g);
        return { kind: "ip", v6: false, big: v4ToBig(v), canon: v.join(".") };
      }
      return { kind: "ip", v6: true, big: ipToBig(g), canon: `[${formatIPv6(g)}]` };
    }
  }
  if (!validHostName(h)) return null;
  return { kind: w ? "wild" : "host", host: h, canon: (w ? "*." : "") + h };
}

export function canonicalCovers(list) {
  if (list.length === 0) throw new Error("a network service must declare what it carries");
  if (list.length > 64) throw new Error("at most 64 coverage entries");
  const out = new Set();
  for (const e of list) {
    const p = parseCover(e);
    if (!p) throw new Error(`malformed coverage entry ${JSON.stringify(e)}`);
    out.add(p.canon);
  }
  return [...out].sort((a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)));
}

const under = (name, suffix) => name === suffix || name.endsWith("." + suffix);

/**
 * How specifically coverage entry c (parsed) carries allow entry a (parsed): a sortable
 * rank [class, tiebreak] (lower is more specific), or null if it does not carry it.
 * class: 0 exact, 1 wildcard (longer suffix first), 2 range (longer prefix first), 3 catch-all.
 */
function carries(c, a) {
  if (c.kind === "all") return [3, 0];
  if (a.kind === "host") {
    if (c.kind === "host" && c.host === a.host) return [0, 0];
    if (c.kind === "wild" && a.host.endsWith("." + c.host)) return [1, -c.host.length];
    return null;
  }
  if (a.kind === "wild") {
    if (c.kind === "wild" && under(a.host, c.host)) return [1, -c.host.length];
    return null;
  }
  // ip
  const g = a.v6 ? parseIPv6(a.host) : parseIPv4(a.host);
  const big = a.v6 ? ipToBig(g) : v4ToBig(g);
  if (c.kind === "ip" && c.v6 === a.v6 && c.big === big) return [0, 0];
  if (c.kind === "range" && c.v6 === a.v6) {
    const total = a.v6 ? 128 : 32;
    if (c.bits === 0 ? true : big >> BigInt(total - c.bits) === c.net >> BigInt(total - c.bits)) return [2, -c.bits];
  }
  return null;
}

/** Best (most specific) rank among the service's coverage entries, or null. */
export function carryRank(coversParsed, allowParsed) {
  let best = null;
  for (const c of coversParsed) {
    const r = carries(c, allowParsed);
    if (r && (!best || r[0] < best[0] || (r[0] === best[0] && r[1] < best[1]))) best = r;
  }
  return best;
}

export function rendezvous(entry, endpoint) {
  return createHash("sha256").update(entry).update("\0").update(endpoint).digest("hex");
}
