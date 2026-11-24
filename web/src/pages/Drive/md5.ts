/**
 * MD5 of a part's bytes, for proving to the store that a part is the one the
 * file holds: the store checks a part signed with its MD5 and reports each
 * part it holds by that digest. Not for security; S3's part checks are MD5.
 * Run in a worker (`partDigests.ts`), since a part is 16 MiB.
 */

/**
 * Folds the 64-byte blocks of `x` from word `from` to word `to` into `h`.
 * Words are little-endian, as every browser's are. The 64 steps are written
 * out: as calls they stop being inlined and the hash runs at a fifth the speed.
 */
function blocks(h: Int32Array, x: Int32Array, from: number, to: number): void {
  let [a, b, c, d] = h;
  for (let o = from; o < to; o += 16) {
    const a0 = a, b0 = b, c0 = c, d0 = d;
    let n: number;
    n = (a + ((b & c) | (~b & d)) + x[o + 0] + -680876936) | 0;
    a = (((n << 7) | (n >>> 25)) + b) | 0;
    n = (d + ((a & b) | (~a & c)) + x[o + 1] + -389564586) | 0;
    d = (((n << 12) | (n >>> 20)) + a) | 0;
    n = (c + ((d & a) | (~d & b)) + x[o + 2] + 606105819) | 0;
    c = (((n << 17) | (n >>> 15)) + d) | 0;
    n = (b + ((c & d) | (~c & a)) + x[o + 3] + -1044525330) | 0;
    b = (((n << 22) | (n >>> 10)) + c) | 0;
    n = (a + ((b & c) | (~b & d)) + x[o + 4] + -176418897) | 0;
    a = (((n << 7) | (n >>> 25)) + b) | 0;
    n = (d + ((a & b) | (~a & c)) + x[o + 5] + 1200080426) | 0;
    d = (((n << 12) | (n >>> 20)) + a) | 0;
    n = (c + ((d & a) | (~d & b)) + x[o + 6] + -1473231341) | 0;
    c = (((n << 17) | (n >>> 15)) + d) | 0;
    n = (b + ((c & d) | (~c & a)) + x[o + 7] + -45705983) | 0;
    b = (((n << 22) | (n >>> 10)) + c) | 0;
    n = (a + ((b & c) | (~b & d)) + x[o + 8] + 1770035416) | 0;
    a = (((n << 7) | (n >>> 25)) + b) | 0;
    n = (d + ((a & b) | (~a & c)) + x[o + 9] + -1958414417) | 0;
    d = (((n << 12) | (n >>> 20)) + a) | 0;
    n = (c + ((d & a) | (~d & b)) + x[o + 10] + -42063) | 0;
    c = (((n << 17) | (n >>> 15)) + d) | 0;
    n = (b + ((c & d) | (~c & a)) + x[o + 11] + -1990404162) | 0;
    b = (((n << 22) | (n >>> 10)) + c) | 0;
    n = (a + ((b & c) | (~b & d)) + x[o + 12] + 1804603682) | 0;
    a = (((n << 7) | (n >>> 25)) + b) | 0;
    n = (d + ((a & b) | (~a & c)) + x[o + 13] + -40341101) | 0;
    d = (((n << 12) | (n >>> 20)) + a) | 0;
    n = (c + ((d & a) | (~d & b)) + x[o + 14] + -1502002290) | 0;
    c = (((n << 17) | (n >>> 15)) + d) | 0;
    n = (b + ((c & d) | (~c & a)) + x[o + 15] + 1236535329) | 0;
    b = (((n << 22) | (n >>> 10)) + c) | 0;
    n = (a + ((b & d) | (c & ~d)) + x[o + 1] + -165796510) | 0;
    a = (((n << 5) | (n >>> 27)) + b) | 0;
    n = (d + ((a & c) | (b & ~c)) + x[o + 6] + -1069501632) | 0;
    d = (((n << 9) | (n >>> 23)) + a) | 0;
    n = (c + ((d & b) | (a & ~b)) + x[o + 11] + 643717713) | 0;
    c = (((n << 14) | (n >>> 18)) + d) | 0;
    n = (b + ((c & a) | (d & ~a)) + x[o + 0] + -373897302) | 0;
    b = (((n << 20) | (n >>> 12)) + c) | 0;
    n = (a + ((b & d) | (c & ~d)) + x[o + 5] + -701558691) | 0;
    a = (((n << 5) | (n >>> 27)) + b) | 0;
    n = (d + ((a & c) | (b & ~c)) + x[o + 10] + 38016083) | 0;
    d = (((n << 9) | (n >>> 23)) + a) | 0;
    n = (c + ((d & b) | (a & ~b)) + x[o + 15] + -660478335) | 0;
    c = (((n << 14) | (n >>> 18)) + d) | 0;
    n = (b + ((c & a) | (d & ~a)) + x[o + 4] + -405537848) | 0;
    b = (((n << 20) | (n >>> 12)) + c) | 0;
    n = (a + ((b & d) | (c & ~d)) + x[o + 9] + 568446438) | 0;
    a = (((n << 5) | (n >>> 27)) + b) | 0;
    n = (d + ((a & c) | (b & ~c)) + x[o + 14] + -1019803690) | 0;
    d = (((n << 9) | (n >>> 23)) + a) | 0;
    n = (c + ((d & b) | (a & ~b)) + x[o + 3] + -187363961) | 0;
    c = (((n << 14) | (n >>> 18)) + d) | 0;
    n = (b + ((c & a) | (d & ~a)) + x[o + 8] + 1163531501) | 0;
    b = (((n << 20) | (n >>> 12)) + c) | 0;
    n = (a + ((b & d) | (c & ~d)) + x[o + 13] + -1444681467) | 0;
    a = (((n << 5) | (n >>> 27)) + b) | 0;
    n = (d + ((a & c) | (b & ~c)) + x[o + 2] + -51403784) | 0;
    d = (((n << 9) | (n >>> 23)) + a) | 0;
    n = (c + ((d & b) | (a & ~b)) + x[o + 7] + 1735328473) | 0;
    c = (((n << 14) | (n >>> 18)) + d) | 0;
    n = (b + ((c & a) | (d & ~a)) + x[o + 12] + -1926607734) | 0;
    b = (((n << 20) | (n >>> 12)) + c) | 0;
    n = (a + (b ^ c ^ d) + x[o + 5] + -378558) | 0;
    a = (((n << 4) | (n >>> 28)) + b) | 0;
    n = (d + (a ^ b ^ c) + x[o + 8] + -2022574463) | 0;
    d = (((n << 11) | (n >>> 21)) + a) | 0;
    n = (c + (d ^ a ^ b) + x[o + 11] + 1839030562) | 0;
    c = (((n << 16) | (n >>> 16)) + d) | 0;
    n = (b + (c ^ d ^ a) + x[o + 14] + -35309556) | 0;
    b = (((n << 23) | (n >>> 9)) + c) | 0;
    n = (a + (b ^ c ^ d) + x[o + 1] + -1530992060) | 0;
    a = (((n << 4) | (n >>> 28)) + b) | 0;
    n = (d + (a ^ b ^ c) + x[o + 4] + 1272893353) | 0;
    d = (((n << 11) | (n >>> 21)) + a) | 0;
    n = (c + (d ^ a ^ b) + x[o + 7] + -155497632) | 0;
    c = (((n << 16) | (n >>> 16)) + d) | 0;
    n = (b + (c ^ d ^ a) + x[o + 10] + -1094730640) | 0;
    b = (((n << 23) | (n >>> 9)) + c) | 0;
    n = (a + (b ^ c ^ d) + x[o + 13] + 681279174) | 0;
    a = (((n << 4) | (n >>> 28)) + b) | 0;
    n = (d + (a ^ b ^ c) + x[o + 0] + -358537222) | 0;
    d = (((n << 11) | (n >>> 21)) + a) | 0;
    n = (c + (d ^ a ^ b) + x[o + 3] + -722521979) | 0;
    c = (((n << 16) | (n >>> 16)) + d) | 0;
    n = (b + (c ^ d ^ a) + x[o + 6] + 76029189) | 0;
    b = (((n << 23) | (n >>> 9)) + c) | 0;
    n = (a + (b ^ c ^ d) + x[o + 9] + -640364487) | 0;
    a = (((n << 4) | (n >>> 28)) + b) | 0;
    n = (d + (a ^ b ^ c) + x[o + 12] + -421815835) | 0;
    d = (((n << 11) | (n >>> 21)) + a) | 0;
    n = (c + (d ^ a ^ b) + x[o + 15] + 530742520) | 0;
    c = (((n << 16) | (n >>> 16)) + d) | 0;
    n = (b + (c ^ d ^ a) + x[o + 2] + -995338651) | 0;
    b = (((n << 23) | (n >>> 9)) + c) | 0;
    n = (a + (c ^ (b | ~d)) + x[o + 0] + -198630844) | 0;
    a = (((n << 6) | (n >>> 26)) + b) | 0;
    n = (d + (b ^ (a | ~c)) + x[o + 7] + 1126891415) | 0;
    d = (((n << 10) | (n >>> 22)) + a) | 0;
    n = (c + (a ^ (d | ~b)) + x[o + 14] + -1416354905) | 0;
    c = (((n << 15) | (n >>> 17)) + d) | 0;
    n = (b + (d ^ (c | ~a)) + x[o + 5] + -57434055) | 0;
    b = (((n << 21) | (n >>> 11)) + c) | 0;
    n = (a + (c ^ (b | ~d)) + x[o + 12] + 1700485571) | 0;
    a = (((n << 6) | (n >>> 26)) + b) | 0;
    n = (d + (b ^ (a | ~c)) + x[o + 3] + -1894986606) | 0;
    d = (((n << 10) | (n >>> 22)) + a) | 0;
    n = (c + (a ^ (d | ~b)) + x[o + 10] + -1051523) | 0;
    c = (((n << 15) | (n >>> 17)) + d) | 0;
    n = (b + (d ^ (c | ~a)) + x[o + 1] + -2054922799) | 0;
    b = (((n << 21) | (n >>> 11)) + c) | 0;
    n = (a + (c ^ (b | ~d)) + x[o + 8] + 1873313359) | 0;
    a = (((n << 6) | (n >>> 26)) + b) | 0;
    n = (d + (b ^ (a | ~c)) + x[o + 15] + -30611744) | 0;
    d = (((n << 10) | (n >>> 22)) + a) | 0;
    n = (c + (a ^ (d | ~b)) + x[o + 6] + -1560198380) | 0;
    c = (((n << 15) | (n >>> 17)) + d) | 0;
    n = (b + (d ^ (c | ~a)) + x[o + 13] + 1309151649) | 0;
    b = (((n << 21) | (n >>> 11)) + c) | 0;
    n = (a + (c ^ (b | ~d)) + x[o + 4] + -145523070) | 0;
    a = (((n << 6) | (n >>> 26)) + b) | 0;
    n = (d + (b ^ (a | ~c)) + x[o + 11] + -1120210379) | 0;
    d = (((n << 10) | (n >>> 22)) + a) | 0;
    n = (c + (a ^ (d | ~b)) + x[o + 2] + 718787259) | 0;
    c = (((n << 15) | (n >>> 17)) + d) | 0;
    n = (b + (d ^ (c | ~a)) + x[o + 9] + -343485551) | 0;
    b = (((n << 21) | (n >>> 11)) + c) | 0;
    a = (a + a0) | 0;
    b = (b + b0) | 0;
    c = (c + c0) | 0;
    d = (d + d0) | 0;
  }
  h[0] = a;
  h[1] = b;
  h[2] = c;
  h[3] = d;
}

/** The MD5 of `data`, as 16 bytes. */
export function md5(data: Uint8Array): Uint8Array {
  const h = new Int32Array([0x67452301, -271733879, -1732584194, 0x10325476]);
  const whole = data.length - (data.length % 64);
  if (whole > 0) {
    // A view needs 4-byte alignment; a part read from a file has it, a sub-view may not.
    const aligned = data.byteOffset % 4 === 0 ? data : new Uint8Array(data.subarray(0, whole));
    blocks(h, new Int32Array(aligned.buffer, aligned.byteOffset, whole / 4), 0, whole / 4);
  }
  // The rest, a 1 bit, zeros, and the length in bits: one block, or two.
  const rest = data.length - whole;
  const tail = new Uint8Array(rest < 56 ? 64 : 128);
  tail.set(data.subarray(whole));
  tail[rest] = 0x80;
  const bits = tail.length - 8;
  const view = new DataView(tail.buffer);
  view.setUint32(bits, (data.length * 8) >>> 0, true);
  view.setUint32(bits + 4, Math.floor(data.length / 0x20000000), true);
  blocks(h, new Int32Array(tail.buffer), 0, tail.length / 4);
  return new Uint8Array(h.buffer.slice(0));
}

export const toHex = (digest: Uint8Array): string => Array.from(digest, (b) => b.toString(16).padStart(2, '0')).join('');

export const toBase64 = (digest: Uint8Array): string => btoa(String.fromCharCode(...digest));
