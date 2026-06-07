import { writable } from 'svelte/store';

const THEME_KEY = 'relay_theme';
const FONT_SIZE_KEY = 'relay_font_size';
export const MIN_FONT_SIZE = 13;
export const MAX_FONT_SIZE = 18;
export const DEFAULT_FONT_SIZE = 15;

function loadPrefers(): boolean {
  try {
    const stored = localStorage.getItem(THEME_KEY);
    if (stored === 'dark') return true;
    if (stored === 'light') return false;
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  } catch {
    return true;
  }
}

function clampFontSize(size: number): number {
  return Math.min(MAX_FONT_SIZE, Math.max(MIN_FONT_SIZE, Math.round(size)));
}

function loadFontSize(): number {
  try {
    const stored = Number(localStorage.getItem(FONT_SIZE_KEY));
    return Number.isFinite(stored) ? clampFontSize(stored) : DEFAULT_FONT_SIZE;
  } catch {
    return DEFAULT_FONT_SIZE;
  }
}

function applyFontSize(size: number): void {
  if (typeof document === 'undefined') return;
  document.documentElement.style.fontSize = `${clampFontSize(size)}px`;
}

export const isDark = writable<boolean>(
  typeof window !== 'undefined' ? loadPrefers() : true,
);

export const fontSize = writable<number>(
  typeof window !== 'undefined' ? loadFontSize() : DEFAULT_FONT_SIZE,
);

export function toggleTheme(): void {
  isDark.update(d => {
    const next = !d;
    if (next) document.documentElement.classList.add('dark');
    else document.documentElement.classList.remove('dark');
    try { localStorage.setItem(THEME_KEY, next ? 'dark' : 'light'); } catch {}
    return next;
  });
}

export function setFontSize(size: number): void {
  const next = clampFontSize(size);
  applyFontSize(next);
  try { localStorage.setItem(FONT_SIZE_KEY, String(next)); } catch {}
  fontSize.set(next);
}

if (typeof window !== 'undefined') {
  applyFontSize(loadFontSize());
}
