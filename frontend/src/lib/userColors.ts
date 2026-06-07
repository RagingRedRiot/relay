import { writable } from 'svelte/store';

const KEY = 'relay_user_colors';

function load(): Record<string, string> {
  try {
    return JSON.parse(localStorage.getItem(KEY) ?? '{}');
  } catch {
    return {};
  }
}

const { subscribe, update } = writable<Record<string, string>>(load());

export const userColors = {
  subscribe,
  set(username: string, color: string) {
    update(m => {
      const next = { ...m, [username]: color };
      localStorage.setItem(KEY, JSON.stringify(next));
      return next;
    });
  },
  clear(username: string) {
    update(m => {
      const { [username]: _removed, ...rest } = m;
      localStorage.setItem(KEY, JSON.stringify(rest));
      return rest;
    });
  },
};
