// User-directory (GetUsers) helpers.
//
// The directory is paged: each request returns at most `limit` entries, ordered
// by username, plus `hasMore`. Continue by passing the last entry's `username` as
// the next `after` cursor. An optional `startsWith` prefix filters by username
// (case-insensitive). Open to any authenticated user; `is_admin` is only present
// on entries when the caller is themselves an admin.

import { send, on } from './index';
import type { UserDirectoryEntry } from './types';

export type { UserDirectoryEntry };

export interface UserPage {
  users: UserDirectoryEntry[];
  hasMore: boolean;
}

export interface GetUsersOpts {
  startsWith?: string;
  after?: string;
  limit?: number;
}

const REQUEST_TIMEOUT_MS = 8000;

// Fetch one page of the directory. Resolves on the next `Users` reply. Replies are
// serialized over the socket, so for sequential, awaited paging (the admin
// directory) each call maps to its own response. For rapid typeahead use, debounce
// and guard with a sequence number so only the latest query's result is applied —
// see the invite search in RoomInfoPanel.
export function getUsers(opts: GetUsersOpts = {}): Promise<UserPage> {
  const startsWith = opts.startsWith?.trim() || undefined;
  return new Promise<UserPage>((resolve, reject) => {
    let settled = false;
    const finish = (fn: () => void) => {
      if (settled) return;
      settled = true;
      offUsers();
      offFailed();
      clearTimeout(timer);
      fn();
    };

    const offUsers = on('Users', ({ users, has_more }) =>
      finish(() => resolve({ users, hasMore: has_more })),
    );
    // GetUsers only fails on an internal error; surface it so callers don't hang.
    const offFailed = on('Failed', () => finish(() => reject(new Error('user lookup failed'))));
    const timer = setTimeout(
      () => finish(() => reject(new Error('user lookup timed out'))),
      REQUEST_TIMEOUT_MS,
    );

    send({ GetUsers: { starts_with: startsWith, after: opts.after, limit: opts.limit } });
  });
}
