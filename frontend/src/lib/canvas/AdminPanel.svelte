<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { send, on } from '../ws';
  import { getUsers, type UserDirectoryEntry } from '../ws/users';

  // ── Directory (browse all users) ─────────────────────────────────────────────

  const DIR_PAGE = 25;

  let dirUsers: UserDirectoryEntry[] = [];
  let dirHasMore = false;
  let dirCursor: string | undefined;
  let dirPrefix = '';
  let dirLoading = false;
  let dirErr = '';
  let dirDebounce: ReturnType<typeof setTimeout> | null = null;

  // Load a page. `reset` starts a fresh listing (new prefix or first load);
  // otherwise it appends the next page from the keyset cursor.
  async function loadDirectory(reset: boolean) {
    if (dirLoading) return;
    dirLoading = true;
    dirErr = '';
    try {
      const page = await getUsers({
        startsWith: dirPrefix.trim() || undefined,
        after: reset ? undefined : dirCursor,
        limit: DIR_PAGE,
      });
      dirUsers = reset ? page.users : [...dirUsers, ...page.users];
      dirHasMore = page.hasMore;
      dirCursor = dirUsers.at(-1)?.username;
    } catch {
      dirErr = 'Could not load users.';
    } finally {
      dirLoading = false;
    }
  }

  function onDirInput() {
    if (dirDebounce) clearTimeout(dirDebounce);
    dirDebounce = setTimeout(() => loadDirectory(true), 200);
  }

  // Selecting a directory row loads it into the lookup/edit form below.
  function selectUser(username: string) {
    query = username;
    lookup();
  }

  function entryName(u: UserDirectoryEntry): string {
    return u.alias?.trim() || [u.first_name, u.last_name].filter(Boolean).join(' ').trim() || u.username;
  }

  // ── User lookup state ───────────────────────────────────────────────────────

  let query = '';
  let lookupState: 'idle' | 'pending' | 'found' | 'notfound' = 'idle';
  let expectedUser = '';

  // Editable fields for the looked-up user.
  let username = '';
  let firstName = '';
  let lastName = '';
  let alias = '';
  let createdAt = '';

  let actionMsg = '';
  let actionErr = '';

  // Reset password.
  let resetPw = '';

  // Destructive-action confirm gates.
  let confirmDelete = false;
  let confirmRestart = false;
  let confirmShutdown = false;
  let serverMsg = '';

  const cleanups: Array<() => void> = [];

  function oneShot(handlers: Array<[keyof import('../ws').ServerEventMap, () => void]>): () => void {
    const unsubs = handlers.map(([ev, fn]) => on(ev as any, () => { done(); fn(); }));
    function done() { unsubs.forEach((u) => u()); }
    return done;
  }

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  onMount(() => {
    cleanups.push(
      on('UserInfo', (info) => {
        if (info.username.toLowerCase() !== expectedUser) return;
        username = info.username;
        firstName = info.first_name ?? '';
        lastName = info.last_name ?? '';
        alias = info.alias ?? '';
        createdAt = info.created_at;
        lookupState = 'found';
      }),
      on('NoUserExists', () => {
        if (lookupState === 'pending') lookupState = 'notfound';
      }),
    );
    loadDirectory(true);
  });

  onDestroy(() => cleanups.forEach((u) => u()));

  // ── Lookup ──────────────────────────────────────────────────────────────────

  function lookup() {
    const q = query.trim();
    if (!q) return;
    actionMsg = ''; actionErr = ''; resetPw = '';
    confirmDelete = false;
    lookupState = 'pending';
    expectedUser = q.toLowerCase();
    send({ GetUserByUsername: { username: q } });
  }

  // ── Per-user actions ─────────────────────────────────────────────────────────

  function withFeedback(ok: string) {
    actionMsg = ''; actionErr = '';
    return oneShot([
      ['Success', () => { actionMsg = ok; }],
      ['NoChange', () => { actionMsg = 'No change.'; }],
      ['NoUserExists', () => { actionErr = 'No such user.'; }],
      ['Failed', () => { actionErr = 'Action failed.'; }],
      ['NoAuth', () => { actionErr = 'Not permitted.'; }],
    ]);
  }

  function saveEdits() {
    if (!username) return;
    withFeedback('Profile updated.');
    send({
      EditUser: {
        target_username: expectedUser,
        username: username.trim() || undefined,
        first_name: firstName.trim() || undefined,
        last_name: lastName.trim() || undefined,
        alias: alias.trim() || undefined,
      },
    });
    // Track the possibly-renamed user for subsequent UserInfo.
    expectedUser = (username.trim() || expectedUser).toLowerCase();
  }

  function promote() { withFeedback('Promoted to admin.'); send({ Promote: { target_username: expectedUser } }); }
  function demote() { withFeedback('Demoted.'); send({ Demote: { target_username: expectedUser } }); }

  function resetPassword() {
    if (!resetPw) return;
    withFeedback('Password reset.');
    send({ ResetPassword: { target_username: expectedUser, new_password: resetPw } });
    resetPw = '';
  }

  function deleteUser() {
    oneShot([
      ['Success', () => { actionMsg = `Deleted ${username}.`; lookupState = 'idle'; query = ''; }],
      ['NoUserExists', () => { actionErr = 'No such user.'; }],
      ['Failed', () => { actionErr = 'Delete failed.'; }],
      ['NoAuth', () => { actionErr = 'Not permitted.'; }],
    ]);
    send({ DeleteUser: { target_username: expectedUser } });
    confirmDelete = false;
  }

  // ── Server control ────────────────────────────────────────────────────────────

  function restartServer() {
    serverMsg = 'Restart requested. The connection will drop and reconnect.';
    send('RestartServer');
    confirmRestart = false;
  }

  function shutdownServer() {
    serverMsg = 'Shutdown requested. The server is going down.';
    send('ShutdownServer');
    confirmShutdown = false;
  }

  // ── Shared styles ───────────────────────────────────────────────────────────

  const sectionHeader = 'mb-3 text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const labelClass = 'mb-1.5 block text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const inputClass =
    'w-full rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-2 text-sm text-slate-800 ' +
    'outline-none transition placeholder:text-slate-400 focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 ' +
    'dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 dark:placeholder:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950';
  const btnClass =
    'rounded-md px-2.5 py-1.5 text-xs font-medium text-slate-600 transition hover:bg-slate-100 hover:text-slate-950 ' +
    'disabled:opacity-40 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white';
  const dangerBtn = 'rounded-md px-2.5 py-1.5 text-xs font-medium text-red-500 transition hover:bg-red-100/65 hover:text-red-700 dark:text-red-400 dark:hover:bg-red-950/40 dark:hover:text-red-300';
</script>

<div class="flex h-full flex-col gap-7 overflow-y-auto px-4 py-4 text-sm">

  <!-- Directory -->
  <section>
    <p class={sectionHeader}>directory</p>
    <input
      class="{inputClass} mb-3"
      placeholder="filter by username prefix"
      bind:value={dirPrefix}
      on:input={onDirInput}
      spellcheck="false"
      autocomplete="off"
    />
    {#if dirErr}
      <p class="text-xs text-red-500 dark:text-red-400">{dirErr}</p>
    {:else if dirUsers.length === 0 && !dirLoading}
      <p class="text-sm text-slate-500 dark:text-slate-400">no users</p>
    {:else}
      <div class="flex flex-col">
        {#each dirUsers as u (u.username)}
          <button
            class="flex items-center gap-2 rounded-md px-2 py-1.5 text-left transition hover:bg-slate-100 dark:hover:bg-slate-800/70"
            on:click={() => selectUser(u.username)}
            title="Manage {u.username}"
          >
            <span class="flex-1 truncate text-sm text-slate-700 dark:text-slate-200">
              {entryName(u)}
              {#if entryName(u) !== u.username}
                <span class="text-xs text-slate-400 dark:text-slate-500">@{u.username}</span>
              {/if}
            </span>
            {#if u.is_admin}
              <span class="shrink-0 rounded-md border border-sky-300/70 bg-sky-100/60 px-1.5 py-0.5 text-[0.62rem] uppercase tracking-wider text-sky-700 dark:border-sky-800 dark:bg-sky-950/40 dark:text-sky-300">
                admin
              </span>
            {/if}
          </button>
        {/each}
      </div>
      {#if dirHasMore}
        <button class="{btnClass} mt-2" on:click={() => loadDirectory(false)} disabled={dirLoading}>
          {dirLoading ? 'loading' : 'load more'}
        </button>
      {/if}
    {/if}
  </section>

  <!-- User management -->
  <section class="border-t border-slate-200/70 pt-5 dark:border-slate-800/80">
    <p class={sectionHeader}>user management</p>
    <form on:submit|preventDefault={lookup} class="flex items-center gap-2 mb-4">
      <input
        class={inputClass}
        placeholder="look up username"
        bind:value={query}
        spellcheck="false"
        autocomplete="off"
      />
      <button class={btnClass} type="submit" disabled={!query.trim()}>find</button>
    </form>

    {#if lookupState === 'pending'}
      <p class="text-sm text-slate-500 dark:text-slate-400">searching</p>
    {:else if lookupState === 'notfound'}
      <p class="text-sm text-slate-500 dark:text-slate-400">no such user</p>
    {:else if lookupState === 'found'}
      <div class="flex flex-col gap-4">
        <div>
          <label class={labelClass} for="a-username">username</label>
          <input id="a-username" class={inputClass} bind:value={username} spellcheck="false" autocomplete="off" />
        </div>
        <div class="flex gap-4">
          <div class="flex-1">
            <label class={labelClass} for="a-first">first name</label>
            <input id="a-first" class={inputClass} bind:value={firstName} spellcheck="false" autocomplete="off" />
          </div>
          <div class="flex-1">
            <label class={labelClass} for="a-last">last name</label>
            <input id="a-last" class={inputClass} bind:value={lastName} spellcheck="false" autocomplete="off" />
          </div>
        </div>
        <div>
          <label class={labelClass} for="a-alias">alias</label>
          <input id="a-alias" class={inputClass} bind:value={alias} spellcheck="false" autocomplete="off" />
        </div>

        <div class="flex flex-wrap items-center gap-x-4 gap-y-2">
          <button class={btnClass} on:click={saveEdits}>save edits</button>
          <button class={btnClass} on:click={promote}>promote</button>
          <button class={btnClass} on:click={demote}>demote</button>
        </div>

        <!-- Reset password -->
        <div class="flex items-end gap-2">
          <div class="flex-1">
            <label class={labelClass} for="a-resetpw">reset password</label>
            <input id="a-resetpw" type="text" class={inputClass} bind:value={resetPw} placeholder="new password" autocomplete="off" />
          </div>
          <button class={btnClass} on:click={resetPassword} disabled={!resetPw}>reset</button>
        </div>

        <!-- Delete (confirm-gated) -->
        <div class="flex items-center gap-3 pt-1">
          {#if confirmDelete}
            <span class="text-sm text-slate-600 dark:text-slate-400">delete {username}?</span>
            <button class={dangerBtn} on:click={deleteUser}>confirm delete</button>
            <button class={btnClass} on:click={() => (confirmDelete = false)}>cancel</button>
          {:else}
            <button class={dangerBtn} on:click={() => (confirmDelete = true)}>delete user</button>
          {/if}
        </div>

        {#if actionMsg}<p class="text-xs text-emerald-600 dark:text-emerald-400">{actionMsg}</p>{/if}
        {#if actionErr}<p class="text-xs text-red-500 dark:text-red-400">{actionErr}</p>{/if}
      </div>
    {/if}
  </section>

  <!-- Server control -->
  <section class="border-t border-slate-200/70 pt-5 dark:border-slate-800/80">
    <p class={sectionHeader}>server control</p>
    <div class="flex flex-col gap-3">
      <div class="flex items-center gap-3">
        {#if confirmRestart}
          <span class="text-sm text-slate-600 dark:text-slate-400">restart the server?</span>
          <button class={dangerBtn} on:click={restartServer}>confirm restart</button>
          <button class={btnClass} on:click={() => (confirmRestart = false)}>cancel</button>
        {:else}
          <button class={dangerBtn} on:click={() => { confirmRestart = true; confirmShutdown = false; }}>restart server</button>
        {/if}
      </div>
      <div class="flex items-center gap-3">
        {#if confirmShutdown}
          <span class="text-sm text-slate-600 dark:text-slate-400">shut the server down?</span>
          <button class={dangerBtn} on:click={shutdownServer}>confirm shutdown</button>
          <button class={btnClass} on:click={() => (confirmShutdown = false)}>cancel</button>
        {:else}
          <button class={dangerBtn} on:click={() => { confirmShutdown = true; confirmRestart = false; }}>shutdown server</button>
        {/if}
      </div>
      {#if serverMsg}<p class="text-xs text-slate-500 dark:text-slate-400">{serverMsg}</p>{/if}
    </div>
  </section>

</div>
