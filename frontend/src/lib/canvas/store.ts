import { writable } from 'svelte/store';
import type { PaneState, PaneType } from './types';

const MIN_WIDTH = 300;
const MIN_HEIGHT = 200;
const DEFAULT_WIDTH = 500;
const DEFAULT_HEIGHT = 440;
const CASCADE_STEP = 26;
const CASCADE_LIMIT = 12;

const { subscribe, update } = writable<PaneState[]>([]);

let zTop = 100;
let cascadeCount = 0;

function bumpZ(ps: PaneState[], id: string): PaneState[] {
  const z = ++zTop;
  return ps.map((p) => (p.id === id ? { ...p, z } : p));
}

export const panes = { subscribe };

export const imageViewer = writable<{ url: string; filename: string } | null>(null);

export function openImageViewer(url: string, filename = 'attachment') {
  imageViewer.set({ url, filename });
}

export function closeImageViewer() {
  imageViewer.set(null);
}

export function openPane(
  id: string,
  title: string,
  type: PaneType,
  dims?: { width?: number; height?: number; readOnly?: boolean; unread?: number },
) {
  update((ps) => {
    if (ps.find((p) => p.id === id)) {
      return bumpZ(ps, id).map((p) =>
        p.id === id
          ? { ...p, readOnly: dims?.readOnly ?? p.readOnly, unread: dims?.unread ?? p.unread }
          : p,
      );
    }
    const offset = (cascadeCount % CASCADE_LIMIT) * CASCADE_STEP;
    cascadeCount++;
    return [
      ...ps,
      {
        id,
        title,
        type,
        x: 64 + offset,
        y: 48 + offset,
        width: dims?.width ?? DEFAULT_WIDTH,
        height: dims?.height ?? DEFAULT_HEIGHT,
        z: ++zTop,
        readOnly: dims?.readOnly ?? false,
        unread: dims?.unread ?? 0,
      },
    ];
  });
}

export function closePane(id: string) {
  update((ps) => ps.filter((p) => p.id !== id));
}

// Open the pane if absent; if it's already open, close it (toggle). Used by
// single-button launchers like the hamburger so a second click dismisses.
export function togglePane(
  id: string,
  title: string,
  type: PaneType,
  dims?: { width?: number; height?: number; readOnly?: boolean; unread?: number },
) {
  let exists = false;
  update((ps) => {
    exists = ps.some((p) => p.id === id);
    return ps;
  });
  if (exists) closePane(id);
  else openPane(id, title, type, dims);
}

// Retarget a room's open panes after a server-side rename. The room pane (id ==
// room name) and its info pane (id == `roominfo:<name>`) are keyed off the name,
// so they go stale once the room is renamed. Rewriting id + title here makes the
// keyed {#each} remount them under the new name, which also keeps reopen-dedup
// correct (openPane(newName) now finds the existing pane). Other panes that
// reference newName are left as-is to avoid id collisions; a name is unique
// server-side so a successful rename means no other room owns it.
export function renameRoomPanes(oldName: string, newName: string) {
  update((ps) => {
    const oldRoomInfoId = `roominfo:${oldName}`;
    const newRoomInfoId = `roominfo:${newName}`;
    // Guard against colliding with an already-open pane under the new ids.
    const collides = ps.some(
      (p) => p.id === newName || p.id === newRoomInfoId,
    );
    if (collides) return ps;
    return ps.map((p) => {
      if (p.type === 'room' && p.id === oldName) {
        return { ...p, id: newName, title: `#${newName}` };
      }
      if (p.type === 'roominfo' && p.id === oldRoomInfoId) {
        return { ...p, id: newRoomInfoId, title: `#${newName} · info` };
      }
      return p;
    });
  });
}

export function focusPane(id: string) {
  update((ps) => bumpZ(ps, id));
}

export function movePane(id: string, x: number, y: number) {
  update((ps) => ps.map((p) => (p.id === id ? { ...p, x, y } : p)));
}

export function resizePane(id: string, w: number, h: number) {
  update((ps) =>
    ps.map((p) =>
      p.id === id
        ? { ...p, width: Math.max(MIN_WIDTH, w), height: Math.max(MIN_HEIGHT, h) }
        : p,
    ),
  );
}
