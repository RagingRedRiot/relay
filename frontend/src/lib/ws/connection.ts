import type { ClientCommand, ConnectionState, ServerEventMap } from './types';

type AnyHandler = (payload: any) => void;
type Listeners = Partial<Record<keyof ServerEventMap, Set<AnyHandler>>>;

const BACKOFF = [1000, 2000, 4000, 8000, 15000, 30000];

export class RelayConnection {
  private url: string;
  private ws: WebSocket | null = null;
  private listeners: Listeners = {};
  private stateListeners = new Set<(s: ConnectionState) => void>();
  private state: ConnectionState = 'disconnected';
  private attempt = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  // Set to true when a Close JSON event is received from the server, or when
  // disconnect() is called manually. Prevents the reconnect loop from firing
  // on those expected closes.
  private expectingClose = false;

  constructor(url: string) {
    this.url = url;
    this.open();
  }

  private open() {
    this.expectingClose = false;
    this.setState('connecting');
    const ws = new WebSocket(this.url);
    ws.binaryType = 'arraybuffer';
    this.ws = ws;

    ws.onopen = () => {
      this.attempt = 0;
      this.setState('connected');
    };

    ws.onmessage = (ev) => {
      if (ev.data instanceof ArrayBuffer) {
        this.handleBinary(ev.data);
      } else {
        this.handleText(ev.data as string);
      }
    };

    ws.onclose = () => {
      this.ws = null;
      if (this.expectingClose) {
        this.setState('disconnected');
        return;
      }
      this.scheduleReconnect();
    };

    // onerror is always followed by onclose; nothing extra to do here.
    ws.onerror = () => {};
  }

  private setState(s: ConnectionState) {
    this.state = s;
    this.stateListeners.forEach((fn) => fn(s));
  }

  private scheduleReconnect() {
    const delay = BACKOFF[Math.min(this.attempt, BACKOFF.length - 1)];
    this.attempt++;
    this.setState('connecting');
    this.timer = setTimeout(() => this.open(), delay);
  }

  private handleText(raw: string) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      console.error('relay: unparseable server message', raw);
      return;
    }

    if (typeof parsed === 'string') {
      // Unit variant: e.g. "AuthOk", "Success"
      const name = parsed as keyof ServerEventMap;
      if (name === 'Close' as any) this.expectingClose = true; // shouldn't happen (Close has payload), but guard anyway
      this.emit(name, undefined);
    } else if (parsed !== null && typeof parsed === 'object') {
      const keys = Object.keys(parsed as object);
      if (keys.length === 1) {
        const name = keys[0] as keyof ServerEventMap;
        const payload = (parsed as Record<string, unknown>)[keys[0]];
        // Server-initiated close: don't reconnect when the socket closes.
        if (name === 'Close') this.expectingClose = true;
        this.emit(name, payload);
      }
    }
  }

  private handleBinary(buf: ArrayBuffer) {
    // Frame: [attachment_id 16B][seq u32 BE 4B][payload...]
    // Server requires payload length > 0, so minimum valid frame is 21 bytes.
    if (buf.byteLength <= 20) return;
    const bytes = new Uint8Array(buf);
    const attachment_id = bytesToUuid(bytes.subarray(0, 16));
    const seq = new DataView(buf).getUint32(16, false);
    const data = bytes.slice(20);
    // Typed construction keeps the payload shape checked before the loose emit.
    const chunk: ServerEventMap['_BinaryChunk'] = { attachment_id, seq, data };
    this.emit('_BinaryChunk', chunk);
  }

  // Deserialized server data is untyped at runtime: handleText derives the event
  // name dynamically, so the payload is necessarily `unknown` here. Subscribers
  // get the precise payload type through the generic `on()`; stored handlers are
  // `AnyHandler`, so dispatching `unknown` is sound.
  private emit(event: keyof ServerEventMap, payload: unknown) {
    this.listeners[event]?.forEach((fn) => fn(payload));
  }

  on<K extends keyof ServerEventMap>(
    event: K,
    handler: (payload: ServerEventMap[K]) => void,
  ): () => void {
    if (!this.listeners[event]) this.listeners[event] = new Set();
    (this.listeners[event] as Set<AnyHandler>).add(handler as AnyHandler);
    return () => (this.listeners[event] as Set<AnyHandler> | undefined)?.delete(handler as AnyHandler);
  }

  onStateChange(fn: (s: ConnectionState) => void): () => void {
    this.stateListeners.add(fn);
    fn(this.state);
    return () => this.stateListeners.delete(fn);
  }

  send(cmd: ClientCommand) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(cmd));
    }
  }

  // Send one attachment chunk. Frame: [uuid 16B][seq u32 BE 4B][payload].
  sendChunk(attachmentId: string, seq: number, data: Uint8Array) {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    const buf = new ArrayBuffer(20 + data.byteLength);
    const view = new DataView(buf);
    const bytes = new Uint8Array(buf);
    bytes.set(uuidToBytes(attachmentId), 0);
    view.setUint32(16, seq, false);
    bytes.set(data, 20);
    this.ws.send(buf);
  }

  disconnect() {
    this.expectingClose = true;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.ws?.close();
  }
}

function bytesToUuid(bytes: Uint8Array): string {
  const h = Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, '');
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}
