<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { send, on, currentUsername, isAdmin } from '../ws';
  import type { RoomMember } from '../ws';
  import { getUsers, type UserDirectoryEntry } from '../ws/users';
  import { closePane, renameRoomPanes } from './store';

  export let roomName: string;

  // ── State ─────────────────────────────────────────────────────────────────

  let members: RoomMember[] = [];
  let isPublic = false;
  let isDiscoverable = false;
  let loaded = false;

  let inviteName = '';
  let ownerName = '';
  let newRoomName = '';

  let msg = '';
  let err = '';

  let confirmLeave = false;

  const cleanups: Array<() => void> = [];

  function oneShot(handlers: Array<[keyof import('../ws').ServerEventMap, () => void]>): () => void {
    const unsubs = handlers.map(([ev, fn]) => on(ev as any, () => { done(); fn(); }));
    function done() { unsubs.forEach((u) => u()); }
    return done;
  }

  // ── Derived ───────────────────────────────────────────────────────────────

  $: amOwner = members.some((m) => m.username === $currentUsername && m.is_owner);
  $: canManage = amOwner || $isAdmin;

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  function refresh() {
    send({ GetRoomMembership: { room_name: roomName } });
    send({ GetRoom: { room_name: roomName } });
  }

  onMount(() => {
    cleanups.push(
      on('RoomMembers', ({ members: m }) => { members = m; loaded = true; }),
      on('RoomInfo', (info) => {
        if (info.room_name.toLowerCase() !== roomName.toLowerCase()) return;
        isPublic = info.is_public;
        isDiscoverable = info.is_discoverable;
      }),
    );
    refresh();
  });

  onDestroy(() => cleanups.forEach((u) => u()));

  // ── Actions ───────────────────────────────────────────────────────────────

  function feedback(ok: string, after?: () => void) {
    msg = ''; err = '';
    return oneShot([
      ['Success', () => { msg = ok; after?.(); refresh(); }],
      ['JoinRequested', () => { msg = ok; refresh(); }],
      ['NoChange', () => { msg = 'No change.'; }],
      ['NoUserExists', () => { err = 'No such user.'; }],
      ['NoRoomExists', () => { err = 'Room not found.'; }],
      ['Failed', () => { err = 'Action failed.'; }],
      ['NoAuth', () => { err = 'Not permitted.'; }],
    ]);
  }

  function invite() {
    const name = inviteName.trim();
    if (!name) return;
    feedback(`Invited ${name}.`, () => {
      inviteName = '';
      inviteSuggestions = [];
      showSuggestions = false;
    });
    send({ InviteToRoom: { room_name: roomName, invitee_username: name } });
  }

  // ── Invite typeahead (find a user without knowing the exact name) ─────────────

  let inviteSuggestions: UserDirectoryEntry[] = [];
  let inviteSearching = false;
  let showSuggestions = false;
  let inviteDebounce: ReturnType<typeof setTimeout> | null = null;
  // Guards against an out-of-order response from a superseded keystroke applying
  // stale suggestions (the socket has no per-request correlation).
  let inviteSeq = 0;

  function onInviteInput() {
    showSuggestions = true;
    if (inviteDebounce) clearTimeout(inviteDebounce);
    const q = inviteName.trim();
    if (!q) {
      inviteSuggestions = [];
      inviteSearching = false;
      return;
    }
    inviteSearching = true;
    inviteDebounce = setTimeout(() => searchInvite(q), 200);
  }

  async function searchInvite(prefix: string) {
    const mine = ++inviteSeq;
    try {
      const page = await getUsers({ startsWith: prefix, limit: 6 });
      if (mine !== inviteSeq) return; // a newer keystroke superseded this search
      // Don't suggest people already in the room -- they can't be invited.
      const present = new Set(members.map((m) => m.username.toLowerCase()));
      inviteSuggestions = page.users.filter((u) => !present.has(u.username.toLowerCase()));
    } catch {
      if (mine === inviteSeq) inviteSuggestions = [];
    } finally {
      if (mine === inviteSeq) inviteSearching = false;
    }
  }

  function pickSuggestion(username: string) {
    inviteName = username;
    inviteSuggestions = [];
    showSuggestions = false;
  }

  // Delay hiding so a click on a suggestion (which blurs the input first) still
  // registers before the dropdown is removed.
  function onInviteBlur() {
    setTimeout(() => (showSuggestions = false), 150);
  }

  function suggestionName(u: UserDirectoryEntry): string {
    return u.alias?.trim() || [u.first_name, u.last_name].filter(Boolean).join(' ').trim() || u.username;
  }

  function addOwner() {
    const name = ownerName.trim();
    if (!name) return;
    feedback(`${name} is now an owner.`, () => (ownerName = ''));
    send({ AddRoomOwner: { room_name: roomName, new_owner_username: name } });
  }

  function rename() {
    const next = newRoomName.trim();
    if (!next) return;
    msg = ''; err = '';
    // On success, retarget the open room + info panes to the new name. This pane
    // is keyed by the old name, so it remounts under the new one (a fresh fetch);
    // don't refresh() here, since that would query the now-stale old name.
    oneShot([
      ['Success', () => renameRoomPanes(roomName, next)],
      ['NoChange', () => { msg = 'No change.'; }],
      ['NoRoomExists', () => { err = 'Room not found.'; }],
      ['Failed', () => { err = 'Rename failed (name may be taken).'; }],
      ['NoAuth', () => { err = 'Not permitted.'; }],
    ]);
    send({ SetRoomName: { current_name: roomName, new_name: next } });
  }

  function leave() {
    oneShot([
      ['Success', () => { closePane(roomName); closePane(`roominfo:${roomName}`); }],
      ['NoChange', () => { closePane(roomName); closePane(`roominfo:${roomName}`); }],
      ['Failed', () => { err = 'Could not leave.'; }],
    ]);
    send({ LeaveRoom: { room_name: roomName } });
    confirmLeave = false;
  }

  // Which member the manager is confirming removal of (by username), if any.
  let confirmRemove: string | null = null;

  function removeMember(username: string) {
    feedback(`Removed ${username}.`, () => (confirmRemove = null));
    send({ RemoveRoomMember: { room_name: roomName, member_username: username } });
  }

  function displayName(m: RoomMember): string {
    return m.alias?.trim() || m.username;
  }

  // ── Shared styles ───────────────────────────────────────────────────────────

  const sectionHeader = 'mb-2 text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const labelClass = 'mb-1.5 block text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const inputClass =
    'w-full rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-2 text-sm text-slate-800 ' +
    'outline-none transition placeholder:text-slate-400 focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 ' +
    'dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 dark:placeholder:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950';
  const btnClass =
    'shrink-0 rounded-md px-2.5 py-1.5 text-xs font-medium text-slate-600 transition hover:bg-slate-100 hover:text-slate-950 ' +
    'disabled:opacity-40 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white';
  const dangerBtn = 'rounded-md px-2.5 py-1.5 text-xs font-medium text-red-500 transition hover:bg-red-100/65 hover:text-red-700 dark:text-red-400 dark:hover:bg-red-950/40 dark:hover:text-red-300';
</script>

<div class="flex h-full flex-col gap-6 overflow-y-auto px-4 py-4 text-sm">

  <!-- Header -->
  <div>
    <p class="text-base font-semibold text-slate-800 dark:text-slate-100">#{roomName}</p>
    <p class="mt-0.5 text-xs text-slate-500 dark:text-slate-400">
      {isPublic ? 'public' : 'private'}{isDiscoverable ? ' · discoverable' : ''}
    </p>
  </div>

  <!-- Members -->
  <section>
    <p class={sectionHeader}>members</p>
    {#if !loaded}
      <p class="text-sm text-slate-500 dark:text-slate-400">loading</p>
    {:else if members.length === 0}
      <p class="text-sm text-slate-500 dark:text-slate-400">no members</p>
    {:else}
      <div class="flex flex-col">
        {#each members as m (m.username)}
          <div class="flex items-center gap-2 py-1">
            <span class="flex-1 truncate text-sm text-slate-700 dark:text-slate-200">
              {displayName(m)}
              {#if m.alias?.trim()}
                <span class="text-xs text-slate-400 dark:text-slate-500">@{m.username}</span>
              {/if}
            </span>
            {#if m.is_owner}
              <span class="shrink-0 rounded-md border border-slate-200 px-1.5 py-0.5 text-[0.62rem] uppercase tracking-wider text-slate-500 dark:border-slate-700 dark:text-slate-400">
                owner
              </span>
            {/if}
            {#if canManage && m.username !== $currentUsername}
              {#if confirmRemove === m.username}
                <button class={dangerBtn} on:click={() => removeMember(m.username)}>confirm</button>
                <button class={btnClass} on:click={() => (confirmRemove = null)}>cancel</button>
              {:else}
                <button
                  class="shrink-0 rounded-md px-1.5 py-1 text-xs text-slate-400 transition hover:bg-red-50 hover:text-red-500 dark:text-slate-500 dark:hover:bg-red-950/40 dark:hover:text-red-400"
                  on:click={() => (confirmRemove = m.username)}
                  title="Remove from room"
                >remove</button>
              {/if}
            {/if}
          </div>
        {/each}
      </div>
    {/if}
  </section>

  <!-- Owner/admin management -->
  {#if canManage}
    <section class="flex flex-col gap-4 border-t border-slate-200/70 pt-5 dark:border-slate-800/80">
      <p class={sectionHeader}>manage</p>

      <div class="flex items-end gap-2">
        <div class="relative flex-1">
          <label class={labelClass} for="r-invite">invite user</label>
          <input
            id="r-invite"
            class={inputClass}
            bind:value={inviteName}
            on:input={onInviteInput}
            on:focus={onInviteInput}
            on:blur={onInviteBlur}
            placeholder="search by username"
            spellcheck="false"
            autocomplete="off"
          />
          {#if showSuggestions && inviteName.trim() && (inviteSuggestions.length > 0 || inviteSearching)}
            <div class="absolute z-10 mt-1 max-h-56 w-full overflow-y-auto rounded-md border border-slate-300/80 bg-white shadow-lg dark:border-slate-700 dark:bg-slate-900">
              {#if inviteSuggestions.length === 0 && inviteSearching}
                <p class="px-3 py-2 text-xs text-slate-400 dark:text-slate-500">searching</p>
              {:else if inviteSuggestions.length === 0}
                <p class="px-3 py-2 text-xs text-slate-400 dark:text-slate-500">no matches</p>
              {:else}
                {#each inviteSuggestions as u (u.username)}
                  <button
                    type="button"
                    class="block w-full truncate px-3 py-1.5 text-left text-sm text-slate-700 transition hover:bg-sky-100/60 dark:text-slate-200 dark:hover:bg-sky-950/40"
                    on:click={() => pickSuggestion(u.username)}
                  >
                    {suggestionName(u)}
                    {#if suggestionName(u) !== u.username}
                      <span class="text-xs text-slate-400 dark:text-slate-500">@{u.username}</span>
                    {/if}
                  </button>
                {/each}
              {/if}
            </div>
          {/if}
        </div>
        <button class={btnClass} on:click={invite} disabled={!inviteName.trim()}>invite</button>
      </div>

      <div class="flex items-end gap-2">
        <div class="flex-1">
          <label class={labelClass} for="r-owner">add owner</label>
          <input id="r-owner" class={inputClass} bind:value={ownerName} placeholder="username (must be a member)" spellcheck="false" autocomplete="off" />
        </div>
        <button class={btnClass} on:click={addOwner} disabled={!ownerName.trim()}>add</button>
      </div>

      <div class="flex items-end gap-2">
        <div class="flex-1">
          <label class={labelClass} for="r-rename">rename room</label>
          <input id="r-rename" class={inputClass} bind:value={newRoomName} placeholder="new name" spellcheck="false" autocomplete="off" />
        </div>
        <button class={btnClass} on:click={rename} disabled={!newRoomName.trim()}>rename</button>
      </div>
    </section>
  {/if}

  <!-- Leave -->
  <section class="border-t border-slate-200/70 pt-5 dark:border-slate-800/80">
    <div class="flex items-center gap-3">
      {#if confirmLeave}
        <span class="text-sm text-slate-600 dark:text-slate-400">leave #{roomName}?</span>
        <button class={dangerBtn} on:click={leave}>confirm leave</button>
        <button class={btnClass} on:click={() => (confirmLeave = false)}>cancel</button>
      {:else}
        <button class={dangerBtn} on:click={() => (confirmLeave = true)}>leave room</button>
      {/if}
    </div>
  </section>

  {#if msg}<p class="text-xs text-emerald-600 dark:text-emerald-400">{msg}</p>{/if}
  {#if err}<p class="text-xs text-red-500 dark:text-red-400">{err}</p>{/if}

</div>
