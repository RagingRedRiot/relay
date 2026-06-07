<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { send, on, currentUsername, isAdmin, unreadRooms } from '../ws';
  import type { RoomUnread, JoinRequestInfo, DiscoverableRoom } from '../ws';
  import { panes, openPane, togglePane } from './store';

  // ── Server-sourced state ──────────────────────────────────────────────────

  let joinedRooms: RoomUnread[] = [];
  let myInvites: string[] = [];
  let myRequests: string[] = [];
  let incomingRequests: JoinRequestInfo[] = [];
  let discoverableRooms: DiscoverableRoom[] = [];
  // Admin-only: every room on the server (for moderation).
  let allRooms: DiscoverableRoom[] = [];
  let showAllRooms = false;

  // ── Search ────────────────────────────────────────────────────────────────

  let searchQuery = '';
  let searchState: 'idle' | 'pending' | 'found' | 'notfound' = 'idle';
  let searchResult: { room_name: string; is_public: boolean } | null = null;
  let searchTimer: ReturnType<typeof setTimeout>;

  // ── Create room form ─────────────────────────────────────────────────────

  let showCreate = false;
  let newName = '';
  let newPublic = false;
  let newDiscoverable = false;
  let createError = '';
  let creating = false;

  // ── Pending section expand state ─────────────────────────────────────────

  let showInvites = false;
  let showRequests = false;
  let showIncoming = false;

  // ── Lifecycle cleanup ─────────────────────────────────────────────────────

  const cleanups: Array<() => void> = [];

  function refresh() {
    send('GetUnreadSummary');
    send('GetMyInvites');
    send('GetMyJoinRequests');
    send('GetIncomingJoinRequests');
    send('ListDiscoverableRooms');
    if ($isAdmin) send('ListAllRooms');
  }

  onMount(() => {
    cleanups.push(
      on('MyInvites', ({ rooms }) => { myInvites = rooms; }),
      on('MyJoinRequests', ({ rooms }) => { myRequests = rooms; }),
      on('IncomingJoinRequests', ({ requests }) => { incomingRequests = requests; }),
      on('DiscoverableRooms', ({ rooms }) => { discoverableRooms = rooms; }),
      on('AllRooms', ({ rooms }) => { allRooms = rooms; }),
      on('RoomInfo', (info) => {
        if (searchState === 'pending') {
          searchResult = { room_name: info.room_name, is_public: info.is_public };
          searchState = 'found';
        }
      }),
      on('NoRoomExists', () => {
        if (searchState === 'pending') { searchResult = null; searchState = 'notfound'; }
      }),
      on('Failed', () => {
        if (searchState === 'pending') { searchResult = null; searchState = 'notfound'; }
      }),
    );
    refresh();
  });

  onDestroy(() => cleanups.forEach((u) => u()));

  // ── Derived ───────────────────────────────────────────────────────────────

  $: joinedRooms = $unreadRooms;
  $: openRoomIds = new Set($panes.filter((p) => p.type === 'room').map((p) => p.id));
  $: joinedNames = new Set(joinedRooms.map((r) => r.room_name));

  // Refresh when the Directory pane regains focus (rises to the topmost z), so
  // the room list self-heals after changes made from other panes (e.g. a room
  // rename or leave). The pane is created focused, so start assuming it's on top
  // to avoid a redundant refresh right after onMount's initial load.
  let wasTop = true;
  $: detectRefocus($panes);
  function detectRefocus(ps: typeof $panes) {
    const dir = ps.find((p) => p.id === '__directory__');
    const isTop = !!dir && ps.every((p) => p.z <= dir.z);
    if (isTop && !wasTop) refresh();
    wasTop = isTop;
  }

  // ── Actions ───────────────────────────────────────────────────────────────

  function unreadForRoom(roomName: string): number {
    return joinedRooms.find((room) => room.room_name === roomName)?.unread ?? 0;
  }

  function openRoom(roomName: string) {
    openPane(roomName, `#${roomName}`, 'room', { unread: unreadForRoom(roomName) });
  }

  // Admin moderation open: rooms the admin isn't a member of open read-only
  // (history readable, but posting is member-gated server-side).
  function openRoomModerated(roomName: string) {
    openPane(roomName, `#${roomName}`, 'room', {
      readOnly: !joinedNames.has(roomName),
      unread: unreadForRoom(roomName),
    });
  }

  function openProfile() {
    togglePane('__profile__', 'profile', 'profile', { width: 360, height: 460 });
  }

  function openAdmin() {
    togglePane('__admin__', 'admin', 'admin', { width: 380, height: 560 });
  }

  function onSearchInput() {
    clearTimeout(searchTimer);
    const q = searchQuery.trim();
    if (!q) { searchState = 'idle'; searchResult = null; return; }
    searchState = 'pending';
    searchResult = null;
    searchTimer = setTimeout(() => send({ GetRoom: { room_name: q } }), 350);
  }

  function clearSearch() {
    clearTimeout(searchTimer);
    searchQuery = '';
    searchState = 'idle';
    searchResult = null;
  }

  function oneShot(handlers: Array<[keyof import('../ws').ServerEventMap, () => void]>): () => void {
    const unsubs = handlers.map(([ev, fn]) =>
      on(ev as any, () => { done(); fn(); }),
    );
    function done() { unsubs.forEach((u) => u()); }
    return done;
  }

  function joinRoom(roomName: string) {
    oneShot([
      ['Success', () => { refresh(); clearSearch(); }],
      ['JoinRequested', () => { refresh(); clearSearch(); }],
      ['NoChange', () => { refresh(); clearSearch(); }],
      ['Failed', () => {}],
    ]);
    send({ JoinRoom: { room_name: roomName } });
  }

  function acceptInvite(roomName: string) {
    oneShot([
      ['Success', () => refresh()],
      ['NoChange', () => refresh()],
      ['Failed', () => {}],
    ]);
    send({ AcceptInvite: { room_name: roomName } });
  }

  function declineInvite(roomName: string) {
    oneShot([
      ['Success', () => refresh()],
      ['NoChange', () => refresh()],
      ['Failed', () => {}],
    ]);
    send({ DeclineInvite: { room_name: roomName } });
  }

  function cancelRequest(roomName: string) {
    oneShot([
      ['Success', () => refresh()],
      ['NoChange', () => refresh()],
      ['Failed', () => {}],
    ]);
    send({ CancelJoinRequest: { room_name: roomName } });
  }

  function approveRequest(roomName: string, username: string) {
    oneShot([
      ['Success', () => refresh()],
      ['NoChange', () => refresh()],
      ['Failed', () => {}],
    ]);
    send({ ApproveJoinRequest: { room_name: roomName, requester_username: username } });
  }

  function rejectRequest(roomName: string, username: string) {
    oneShot([
      ['Success', () => refresh()],
      ['NoChange', () => refresh()],
      ['Failed', () => {}],
    ]);
    send({ RejectJoinRequest: { room_name: roomName, requester_username: username } });
  }

  function submitCreate() {
    const name = newName.trim();
    if (!name) return;
    creating = true;
    createError = '';
    oneShot([
      ['Success', () => {
        creating = false;
        showCreate = false;
        newName = '';
        newPublic = false;
        newDiscoverable = false;
        refresh();
      }],
      ['Failed', () => {
        creating = false;
        createError = 'Name already taken or invalid.';
      }],
    ]);
    send({ NewRoom: { room_name: name, is_public: newPublic, is_discoverable: newDiscoverable } });
  }

  // ── Shared styles ─────────────────────────────────────────────────────────

  const sectionHeader =
    'mb-2 text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const rowBtn =
    '-mx-1.5 flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-left ' +
    'transition hover:bg-sky-100/60 hover:text-sky-800 dark:hover:bg-sky-950/35 dark:hover:text-sky-100';
  const rowBase = 'flex items-center gap-2 py-1.5';
  const rowName =
    'flex-1 truncate text-sm text-slate-700 dark:text-slate-200';
  const rowMeta =
    'shrink-0 text-xs text-slate-400 dark:text-slate-500';
  const actionBtn =
    'shrink-0 rounded-md px-1.5 py-1 text-xs font-medium text-slate-500 ' +
    'transition hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-white';
  const inputClass =
    'w-full rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-2 text-sm ' +
    'text-slate-800 outline-none transition placeholder:text-slate-400 ' +
    'focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 ' +
    'dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 ' +
    'dark:placeholder:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950';
</script>

<div class="flex h-full flex-col text-sm">

  <!-- Search -->
  <div class="border-b border-slate-300/65 bg-slate-200/35 px-4 py-3 dark:border-slate-800/80 dark:bg-slate-900/35">
    <input
      class={inputClass}
      placeholder="search rooms by name"
      bind:value={searchQuery}
      on:input={onSearchInput}
      autocomplete="off"
      spellcheck="false"
    />
  </div>

  <!-- Scrollable body -->
  <div class="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 py-4">

    <!-- Search result -->
    {#if searchState !== 'idle'}
      <section>
        <p class={sectionHeader}>result</p>
        {#if searchState === 'pending'}
          <p class="text-sm text-slate-500 dark:text-slate-400">searching</p>
        {:else if searchState === 'notfound'}
          <p class="text-sm text-slate-500 dark:text-slate-400">no room found</p>
        {:else if searchResult}
          {@const inJoined = joinedNames.has(searchResult.room_name)}
          <button
            class={rowBtn}
            on:click={() => inJoined
              ? openRoom(searchResult!.room_name)
              : joinRoom(searchResult!.room_name)}
          >
            <span class={rowName}>#{searchResult.room_name}</span>
            <span class={rowMeta}>{searchResult.is_public ? 'open' : 'invite-only'}</span>
            <span class={rowMeta}>{inJoined ? '↗' : searchResult.is_public ? 'join' : 'request'}</span>
          </button>
        {/if}
      </section>
    {/if}

    <!-- My rooms -->
    {#if joinedRooms.length > 0}
      <section>
        <p class={sectionHeader}>my rooms</p>
        {#each joinedRooms as room (room.room_name)}
          <button class={rowBtn} on:click={() => openRoom(room.room_name)}>
            <span class={rowName}>#{room.room_name}</span>
            {#if room.unread > 0}
              <span class="shrink-0 rounded-full bg-sky-100 px-2 py-0.5 text-xs font-semibold text-sky-700 dark:bg-sky-950 dark:text-sky-300">{room.unread}</span>
            {/if}
          </button>
        {/each}
      </section>
    {/if}

    <!-- Pending invites -->
    {#if myInvites.length > 0}
      <section>
        <button
          class="flex items-center gap-2 w-full text-left mb-1"
          on:click={() => (showInvites = !showInvites)}
        >
          <p class={sectionHeader + ' flex-1 mb-0'}>invites</p>
          <span class={rowMeta}>{myInvites.length}</span>
          <span class={rowMeta}>{showInvites ? '▴' : '▾'}</span>
        </button>
        {#if showInvites}
          {#each myInvites as room}
            <div class={rowBase}>
              <span class={rowName}>#{room}</span>
              <button class={actionBtn} on:click={() => acceptInvite(room)}>accept</button>
              <button class={actionBtn} on:click={() => declineInvite(room)}>decline</button>
            </div>
          {/each}
        {/if}
      </section>
    {/if}

    <!-- My pending join requests -->
    {#if myRequests.length > 0}
      <section>
        <button
          class="flex items-center gap-2 w-full text-left mb-1"
          on:click={() => (showRequests = !showRequests)}
        >
          <p class={sectionHeader + ' flex-1 mb-0'}>your requests</p>
          <span class={rowMeta}>{myRequests.length}</span>
          <span class={rowMeta}>{showRequests ? '▴' : '▾'}</span>
        </button>
        {#if showRequests}
          {#each myRequests as room}
            <div class={rowBase}>
              <span class={rowName}>#{room}</span>
              <span class={rowMeta}>pending</span>
              <button class={actionBtn} on:click={() => cancelRequest(room)}>cancel</button>
            </div>
          {/each}
        {/if}
      </section>
    {/if}

    <!-- Incoming join requests (room owners) -->
    {#if incomingRequests.length > 0}
      <section>
        <button
          class="flex items-center gap-2 w-full text-left mb-1"
          on:click={() => (showIncoming = !showIncoming)}
        >
          <p class={sectionHeader + ' flex-1 mb-0'}>approval needed</p>
          <span class={rowMeta}>{incomingRequests.length}</span>
          <span class={rowMeta}>{showIncoming ? '▴' : '▾'}</span>
        </button>
        {#if showIncoming}
          {#each incomingRequests as req}
            <div class={rowBase}>
              <span class={rowName}>
                <span class="text-neutral-600 dark:text-neutral-400">{req.username}</span>
                <span class="text-neutral-300 dark:text-neutral-600"> → </span>
                <span>#{req.room_name}</span>
              </span>
              <button class={actionBtn} on:click={() => approveRequest(req.room_name, req.username)}>
                approve
              </button>
              <button class={actionBtn} on:click={() => rejectRequest(req.room_name, req.username)}>
                reject
              </button>
            </div>
          {/each}
        {/if}
      </section>
    {/if}

    <!-- Discoverable rooms — shown when search is idle -->
    {#if searchState === 'idle' && discoverableRooms.length > 0}
      {@const unjoined = discoverableRooms.filter(r => !joinedNames.has(r.room_name))}
      {#if unjoined.length > 0}
        <section>
          <p class={sectionHeader}>discover</p>
          {#each unjoined as room (room.room_name)}
            <button class={rowBtn} on:click={() => joinRoom(room.room_name)}>
              <span class={rowName}>#{room.room_name}</span>
              <span class={rowMeta}>{room.member_count}</span>
              <span class={rowMeta}>{room.is_public ? 'join' : 'request'}</span>
            </button>
          {/each}
        </section>
      {/if}
    {:else if searchState === 'idle' && discoverableRooms.length === 0}
      <p class="text-sm leading-relaxed text-slate-500 dark:text-slate-400">
        search by name to find rooms
      </p>
    {/if}

    <!-- All rooms (admin moderation) — every room, including private ones -->
    {#if $isAdmin && searchState === 'idle' && allRooms.length > 0}
      <section>
        <button
          class="flex items-center gap-2 w-full text-left mb-1"
          on:click={() => (showAllRooms = !showAllRooms)}
        >
          <p class={sectionHeader + ' flex-1 mb-0'}>all rooms · admin</p>
          <span class={rowMeta}>{allRooms.length}</span>
          <span class={rowMeta}>{showAllRooms ? '▴' : '▾'}</span>
        </button>
        {#if showAllRooms}
          {#each allRooms as room (room.room_name)}
            <button class={rowBtn} on:click={() => openRoomModerated(room.room_name)}>
              <span class={rowName}>#{room.room_name}</span>
              {#if joinedNames.has(room.room_name)}
                <span class={rowMeta}>member</span>
              {/if}
              <span class={rowMeta}>{room.member_count}</span>
              <span class={rowMeta}>{room.is_public ? 'public' : 'private'}</span>
            </button>
          {/each}
        {/if}
      </section>
    {/if}

  </div>

  <!-- Bottom actions -->
  <div class="flex flex-col gap-3 border-t border-slate-300/65 bg-slate-200/35 px-4 py-3 dark:border-slate-800/80 dark:bg-slate-900/35">

    <!-- Create room -->
    {#if showCreate}
      <form on:submit|preventDefault={submitCreate} class="flex flex-col gap-3">
        <input
          class={inputClass}
          placeholder="room name"
          bind:value={newName}
          autocomplete="off"
          spellcheck="false"
          disabled={creating}
        />
        <div class="flex gap-4">
          <label class="flex items-center gap-1.5 cursor-pointer">
            <input
              type="checkbox"
              bind:checked={newPublic}
              class="accent-neutral-500"
            />
            <span class="text-xs text-slate-600 dark:text-slate-400">public</span>
          </label>
          <label class="flex items-center gap-1.5 cursor-pointer">
            <input
              type="checkbox"
              bind:checked={newDiscoverable}
              class="accent-neutral-500"
            />
            <span class="text-xs text-slate-600 dark:text-slate-400">discoverable</span>
          </label>
        </div>
        {#if createError}
          <p class="text-sm text-red-500 dark:text-red-400">{createError}</p>
        {/if}
        <div class="flex items-center justify-between">
          <button
            type="button"
            class="rounded-md px-2 py-1 text-xs font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-white"
            on:click={() => { showCreate = false; createError = ''; newName = ''; }}
          >
            cancel
          </button>
          <button
            type="submit"
            disabled={creating || !newName.trim()}
            class="rounded-md bg-slate-900 px-3 py-1.5 text-xs font-medium text-white transition hover:bg-sky-700 disabled:opacity-40 dark:bg-sky-500 dark:text-slate-950 dark:hover:bg-sky-400"
          >
            {creating ? 'creating' : 'create'}
          </button>
        </div>
      </form>
    {:else}
      <button
        class="rounded-md px-2 py-1.5 text-left text-sm font-medium text-slate-600 transition hover:bg-slate-100 hover:text-sky-700 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-sky-200"
        on:click={() => (showCreate = true)}
      >
        + new room
      </button>
    {/if}

    <!-- Profile + admin -->
    <div class="flex items-center justify-between">
      <button
        class="truncate rounded-md px-2 py-1 text-left text-xs font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-white"
        on:click={openProfile}
      >
        {$currentUsername} · profile
      </button>
      {#if $isAdmin}
        <button
          class="rounded-md px-2 py-1 text-xs font-medium text-slate-500 transition hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-white"
          on:click={openAdmin}
        >
          admin
        </button>
      {/if}
    </div>

  </div>
</div>
