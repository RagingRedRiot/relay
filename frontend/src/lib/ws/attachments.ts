// Attachment upload/download helpers built on the chunked binary protocol.
//
// Upload:   GetMaxChunkSize → SendMessage{attachments} → (MessageCreated gives
//           attachment_ids) → stream binary chunks → AttachmentComplete.
// Download: DownloadAttachment → binary _BinaryChunk frames in seq order →
//           AttachmentEnd. The server only serves downloads once is_complete.

import { writable } from 'svelte/store';
import { send, on, sendChunk } from './index';
import type { AttachmentSummary, NewMessageAttachment } from './types';

// ── Upload progress (sender-side) ─────────────────────────────────────────────
//
// Per-attachment status while an upload is in flight. 'uploading' means chunks
// have been streamed and we're waiting for the server's AttachmentComplete ack;
// 'complete' is shown briefly once that ack lands, then the entry is dropped.
// Only the uploader observes this — receivers never get AttachmentComplete.

export type UploadStatus = 'uploading' | 'complete' | 'error';

export interface UploadState {
  status: UploadStatus;
  reason?: string; // set when status === 'error' (the server's rejection reason)
}

const _uploads = writable<Record<string, UploadState>>({});
export const uploadProgress = { subscribe: _uploads.subscribe };

function setUploadState(id: string, state: UploadState) {
  _uploads.update((m) => ({ ...m, [id]: state }));
}
function clearUploadStatus(id: string) {
  _uploads.update((m) => {
    const next = { ...m };
    delete next[id];
    return next;
  });
}

const COMPLETION_TIMEOUT_MS = 30000;
const FILE_READ_TIMEOUT_MS = 15000;
const MESSAGE_CREATED_TIMEOUT_MS = 10000;

// Resolve an in-flight upload's indicator: 'complete' on the server's ack, or
// 'error' if the server rejects the file by content-type policy. Both states are
// transient; rejected attachments are removed from the room view by the message
// event handlers, so this only covers any short-lived upload UI still mounted.
function watchCompletion(id: string) {
  let done = false;
  const settle = (state: UploadState, ttl: number) => {
    if (done) return;
    done = true;
    offComplete();
    offRejected();
    clearTimeout(timer);
    setUploadState(id, state);
    setTimeout(() => clearUploadStatus(id), ttl);
  };
  const offComplete = on('AttachmentComplete', ({ attachment_id }) => {
    if (attachment_id === id) settle({ status: 'complete' }, 1500);
  });
  const offRejected = on('AttachmentRejected', ({ attachment_id, reason }) => {
    if (attachment_id !== id) return;
    // The server has cancelled the upload and deleted the row, so the file is
    // gone server-side. Drop the sender's local-preview blob too, otherwise
    // getAttachmentUrl would keep serving it from cache.
    dropLocalAttachment(id);
    settle({ status: 'error', reason }, 1500);
  });
  const timer = setTimeout(() => settle({ status: 'complete' }, 1500), COMPLETION_TIMEOUT_MS);
}

// ── Max chunk size (fetched once, cached) ─────────────────────────────────────

let maxChunkPromise: Promise<number> | null = null;

function getMaxChunkBytes(): Promise<number> {
  if (maxChunkPromise) return maxChunkPromise;
  maxChunkPromise = new Promise<number>((resolve) => {
    const off = on('MaxChunkSize', ({ bytes }) => {
      off();
      resolve(bytes);
    });
    send('GetMaxChunkSize');
  });
  return maxChunkPromise;
}

// Web Crypto is unavailable in non-secure browser contexts, which commonly
// includes phones opening the app over http:// on a LAN address.
async function sha256Bytes(buf: ArrayBuffer): Promise<number[]> {
  try {
    const subtle = globalThis.crypto?.subtle;
    if (subtle) {
      const digest = await subtle.digest('SHA-256', buf);
      return Array.from(new Uint8Array(digest));
    }
  } catch {
    // Fall through to the local implementation below.
  }

  return sha256Fallback(new Uint8Array(buf));
}

function sha256Fallback(bytes: Uint8Array): number[] {
  const k = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  ];
  let h0 = 0x6a09e667;
  let h1 = 0xbb67ae85;
  let h2 = 0x3c6ef372;
  let h3 = 0xa54ff53a;
  let h4 = 0x510e527f;
  let h5 = 0x9b05688c;
  let h6 = 0x1f83d9ab;
  let h7 = 0x5be0cd19;

  const bitLenHigh = Math.floor(bytes.length / 0x20000000);
  const bitLenLow = (bytes.length << 3) >>> 0;
  const paddedLen = (((bytes.length + 9 + 63) >> 6) << 6);
  const padded = new Uint8Array(paddedLen);
  padded.set(bytes);
  padded[bytes.length] = 0x80;
  const view = new DataView(padded.buffer);
  view.setUint32(paddedLen - 8, bitLenHigh, false);
  view.setUint32(paddedLen - 4, bitLenLow, false);

  const w = new Uint32Array(64);
  for (let offset = 0; offset < paddedLen; offset += 64) {
    for (let i = 0; i < 16; i++) w[i] = view.getUint32(offset + i * 4, false);
    for (let i = 16; i < 64; i++) {
      const s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >>> 3);
      const s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) >>> 0;
    }

    let a = h0;
    let b = h1;
    let c = h2;
    let d = h3;
    let e = h4;
    let f = h5;
    let g = h6;
    let h = h7;

    for (let i = 0; i < 64; i++) {
      const s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
      const ch = (e & f) ^ (~e & g);
      const temp1 = (h + s1 + ch + k[i] + w[i]) >>> 0;
      const s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (s0 + maj) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    h0 = (h0 + a) >>> 0;
    h1 = (h1 + b) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
    h5 = (h5 + f) >>> 0;
    h6 = (h6 + g) >>> 0;
    h7 = (h7 + h) >>> 0;
  }

  const out = new Uint8Array(32);
  const outView = new DataView(out.buffer);
  [h0, h1, h2, h3, h4, h5, h6, h7].forEach((word, i) => outView.setUint32(i * 4, word, false));
  return Array.from(out);
}

function rotr(value: number, bits: number): number {
  return (value >>> bits) | (value << (32 - bits));
}

// ── Object-URL cache (attachment_id → blob URL) ───────────────────────────────

const urlCache = new Map<string, string>();

// Sender-side instant preview: register the local file under its attachment_id
// so rendering skips the download round-trip (and works before is_complete).
export function registerLocalAttachment(attachmentId: string, file: Blob): string {
  const url = URL.createObjectURL(file);
  urlCache.set(attachmentId, url);
  return url;
}

// Forget (and revoke) a cached object URL — used when an upload is rejected so the
// sender's instant-preview blob can't keep masquerading as a downloadable file.
export function dropLocalAttachment(attachmentId: string): void {
  const url = urlCache.get(attachmentId);
  if (url) {
    URL.revokeObjectURL(url);
    urlCache.delete(attachmentId);
  }
}

// ── Upload ────────────────────────────────────────────────────────────────────

// Send a message carrying a single file attachment. The caller (Room) keeps its
// normal MessageCreated handling for appending the message; this only owns the
// SendMessage and the chunk streaming.
export async function uploadAttachment(
  roomName: string,
  content: string,
  file: File,
): Promise<void> {
  const maxBytes = await getMaxChunkBytes();
  const buf = await readFileBuffer(file);
  const bytes = new Uint8Array(buf);
  const sha = await sha256Bytes(buf);

  const size = bytes.byteLength;
  if (size === 0) throw new Error('empty attachment');
  const chunkCount = Math.max(1, Math.ceil(size / maxBytes));

  const meta: NewMessageAttachment = {
    filename: attachmentFilename(file),
    content_type: file.type || 'application/octet-stream',
    size_bytes: size,
    chunk_count: chunkCount,
    content_sha256: sha,
  };

  return new Promise<void>((resolve, reject) => {
    let done = false;
    const finish = (err?: Error) => {
      if (done) return;
      done = true;
      offCreated();
      offFailed();
      offError();
      clearTimeout(timer);
      if (err) reject(err);
      else resolve();
    };

    // The next MessageCreated with a non-empty attachment list is ours: sends
    // are serialized over one socket and the server acks in order.
    const offCreated = on('MessageCreated', ({ attachment_ids }) => {
      if (!attachment_ids || attachment_ids.length === 0) return;
      const id = attachment_ids[0];
      // Instant local preview for the sender.
      registerLocalAttachment(id, file);
      // Mark in-flight and watch for the server's completion/rejection.
      setUploadState(id, { status: 'uploading' });
      watchCompletion(id);
      // Stream the chunks.
      for (let seq = 0; seq < chunkCount; seq++) {
        const start = seq * maxBytes;
        const end = Math.min(start + maxBytes, size);
        sendChunk(id, seq, bytes.subarray(start, end));
      }
      finish();
    });
    const offFailed = on('Failed', () => finish(new Error('message rejected')));
    const offError = on('Error', ({ error }) => finish(new Error(error || 'message rejected')));
    const timer = setTimeout(
      () => finish(new Error('message creation timed out')),
      MESSAGE_CREATED_TIMEOUT_MS,
    );

    send({ SendMessage: { room_name: roomName, content, attachments: [meta] } });
  });
}

function readFileBuffer(file: File): Promise<ArrayBuffer> {
  return new Promise<ArrayBuffer>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('attachment read timed out')),
      FILE_READ_TIMEOUT_MS,
    );
    file.arrayBuffer()
      .then((buf) => {
        clearTimeout(timer);
        resolve(buf);
      })
      .catch((err) => {
        clearTimeout(timer);
        reject(err);
      });
  });
}

function attachmentFilename(file: File): string {
  if (file.name) return file.name;
  if (file.type === 'image/gif') return 'pasted.gif';
  if (file.type === 'image/png') return 'pasted.png';
  if (file.type === 'image/jpeg') return 'pasted.jpg';
  if (file.type === 'image/webp') return 'pasted.webp';
  if (file.type.startsWith('text/')) return 'pasted.txt';
  return 'attachment';
}

// ── Download ──────────────────────────────────────────────────────────────────

const DOWNLOAD_TIMEOUT_MS = 8000;

// Resolve to an object URL for the attachment's bytes. Returns the cached URL
// (including the sender's local preview) immediately when present.
//
// The download is attempted regardless of the snapshot's is_complete flag: a
// receiver's live message arrives before the upload finishes and the server
// never pushes a completion update, so the flag is unreliable. The server only
// streams bytes once the row is complete and otherwise replies with Error; this
// rejects on that Error (or timeout) so the caller can retry with backoff.
export function getAttachmentUrl(att: AttachmentSummary): Promise<string> {
  const cached = urlCache.get(att.attachment_id);
  if (cached) return Promise.resolve(cached);

  return new Promise<string>((resolve, reject) => {
    const chunks = new Map<number, Uint8Array>();
    let settled = false;

    function cleanup() {
      offChunk();
      offEnd();
      offErr();
      clearTimeout(timer);
    }

    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error('download timed out'));
    }, DOWNLOAD_TIMEOUT_MS);

    const offChunk = on('_BinaryChunk', (frame) => {
      if (frame.attachment_id !== att.attachment_id) return;
      chunks.set(frame.seq, frame.data);
    });

    const offEnd = on('AttachmentEnd', ({ attachment_id }) => {
      if (attachment_id !== att.attachment_id || settled) return;
      settled = true;
      cleanup();
      const ordered = [...chunks.keys()].sort((a, b) => a - b).map((k) => chunks.get(k)!);
      const blob = new Blob(ordered as BlobPart[], { type: att.content_type });
      const url = URL.createObjectURL(blob);
      urlCache.set(att.attachment_id, url);
      resolve(url);
    });

    // Error carries no attachment_id; with the single-image-per-message flow used
    // here, an Error during a pending download means this one isn't ready yet.
    const offErr = on('Error', () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error('attachment not ready'));
    });

    send({ DownloadAttachment: { attachment_id: att.attachment_id } });
  });
}
