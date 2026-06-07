<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { get } from 'svelte/store';
  import { send, on, currentUsername, setCurrentUser } from '../ws';

  // ── State ─────────────────────────────────────────────────────────────────

  let firstName = '';
  let lastName = '';
  let alias = '';
  let username = '';
  let loaded = false;
  // The username we asked for; UserInfo for any other user (e.g. an admin
  // lookup in another open pane) is ignored so it can't clobber our fields.
  let expectedUser = '';

  // Profile save feedback.
  let savingProfile = false;
  let profileMsg = '';
  let profileErr = '';

  // Password change.
  let currentPassword = '';
  let newPassword = '';
  let confirmPassword = '';
  let savingPw = false;
  let pwMsg = '';
  let pwErr = '';

  const cleanups: Array<() => void> = [];

  // ── oneShot (matches Directory's pattern) ───────────────────────────────────

  function oneShot(handlers: Array<[keyof import('../ws').ServerEventMap, () => void]>): () => void {
    const unsubs = handlers.map(([ev, fn]) =>
      on(ev as any, () => { done(); fn(); }),
    );
    function done() { unsubs.forEach((u) => u()); }
    return done;
  }

  // ── Lifecycle ───────────────────────────────────────────────────────────────

  function fetchUser(name: string) {
    expectedUser = name.toLowerCase();
    send({ GetUserByUsername: { username: name } });
  }

  onMount(() => {
    cleanups.push(
      on('UserInfo', (info) => {
        if (info.username.toLowerCase() !== expectedUser) return;
        firstName = info.first_name ?? '';
        lastName = info.last_name ?? '';
        alias = info.alias ?? '';
        username = info.username;
        setCurrentUser(info.username);
        loaded = true;
      }),
    );
    const me = get(currentUsername);
    if (me) fetchUser(me);
  });

  onDestroy(() => cleanups.forEach((u) => u()));

  // ── Actions ───────────────────────────────────────────────────────────────

  function saveProfile() {
    const me = get(currentUsername);
    if (!me) return;
    savingProfile = true;
    profileMsg = '';
    profileErr = '';
    oneShot([
      ['Success', () => {
        savingProfile = false;
        profileMsg = 'Saved.';
        // Username may have changed; re-fetch to stay in sync.
        fetchUser(username.trim() || me);
      }],
      ['NoChange', () => { savingProfile = false; profileMsg = 'No changes.'; }],
      ['Failed', () => { savingProfile = false; profileErr = 'Could not save (name may be taken).'; }],
    ]);
    send({
      EditUser: {
        target_username: me,
        username: username.trim() || undefined,
        first_name: firstName.trim() || undefined,
        last_name: lastName.trim() || undefined,
        alias: alias.trim() || undefined,
      },
    });
  }

  function changePassword() {
    pwMsg = '';
    pwErr = '';
    if (!currentPassword || !newPassword) return;
    if (newPassword !== confirmPassword) { pwErr = 'New passwords do not match.'; return; }
    savingPw = true;
    oneShot([
      ['Success', () => {
        savingPw = false;
        pwMsg = 'Password updated.';
        currentPassword = ''; newPassword = ''; confirmPassword = '';
      }],
      ['NoChange', () => { savingPw = false; pwMsg = 'Password unchanged.'; }],
      ['Failed', () => { savingPw = false; pwErr = 'Update failed (check current password).'; }],
      ['NoAuth', () => { savingPw = false; pwErr = 'Current password is incorrect.'; }],
    ]);
    send({ UpdatePassword: { current_password: currentPassword, new_password: newPassword } });
  }

  // ── Shared styles ───────────────────────────────────────────────────────────

  const sectionHeader =
    'mb-3 text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const labelClass =
    'mb-1.5 block text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
  const inputClass =
    'w-full rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-2 text-sm ' +
    'text-slate-800 outline-none transition placeholder:text-slate-400 ' +
    'focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 ' +
    'dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 ' +
    'dark:placeholder:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950';
  const btnClass =
    'rounded-md px-2.5 py-1.5 text-xs font-medium text-slate-600 transition hover:bg-slate-100 hover:text-slate-950 ' +
    'disabled:opacity-40 dark:text-slate-300 dark:hover:bg-slate-800 dark:hover:text-white';
</script>

<div class="flex h-full flex-col gap-7 overflow-y-auto px-4 py-4 text-sm">

  <!-- Profile -->
  <section>
    <p class={sectionHeader}>profile</p>
    {#if !loaded}
      <p class="text-sm text-slate-500 dark:text-slate-400">loading</p>
    {:else}
      <div class="flex flex-col gap-4">
        <div>
          <label class={labelClass} for="p-username">username</label>
          <input id="p-username" class={inputClass} bind:value={username} spellcheck="false" autocomplete="off" />
        </div>
        <div class="flex gap-4">
          <div class="flex-1">
            <label class={labelClass} for="p-first">first name</label>
            <input id="p-first" class={inputClass} bind:value={firstName} spellcheck="false" autocomplete="off" />
          </div>
          <div class="flex-1">
            <label class={labelClass} for="p-last">last name</label>
            <input id="p-last" class={inputClass} bind:value={lastName} spellcheck="false" autocomplete="off" />
          </div>
        </div>
        <div>
          <label class={labelClass} for="p-alias">alias</label>
          <input id="p-alias" class={inputClass} bind:value={alias} spellcheck="false" autocomplete="off" />
        </div>
        <div class="flex items-center gap-3">
          <button class={btnClass} on:click={saveProfile} disabled={savingProfile}>
            {savingProfile ? 'saving' : 'save profile'}
          </button>
          {#if profileMsg}<span class="text-xs text-emerald-600 dark:text-emerald-400">{profileMsg}</span>{/if}
          {#if profileErr}<span class="text-xs text-red-500 dark:text-red-400">{profileErr}</span>{/if}
        </div>
      </div>
    {/if}
  </section>

  <!-- Password -->
  <section class="border-t border-slate-200/70 pt-5 dark:border-slate-800/80">
    <p class={sectionHeader}>change password</p>
    <div class="flex flex-col gap-4">
      <div>
        <label class={labelClass} for="p-curpw">current password</label>
        <input id="p-curpw" type="password" class={inputClass} bind:value={currentPassword} autocomplete="current-password" />
      </div>
      <div>
        <label class={labelClass} for="p-newpw">new password</label>
        <input id="p-newpw" type="password" class={inputClass} bind:value={newPassword} autocomplete="new-password" />
      </div>
      <div>
        <label class={labelClass} for="p-confpw">confirm new password</label>
        <input id="p-confpw" type="password" class={inputClass} bind:value={confirmPassword} autocomplete="new-password" />
      </div>
      <div class="flex items-center gap-3">
        <button class={btnClass} on:click={changePassword} disabled={savingPw || !currentPassword || !newPassword}>
          {savingPw ? 'updating' : 'update password'}
        </button>
        {#if pwMsg}<span class="text-xs text-emerald-600 dark:text-emerald-400">{pwMsg}</span>{/if}
        {#if pwErr}<span class="text-xs text-red-500 dark:text-red-400">{pwErr}</span>{/if}
      </div>
    </div>
  </section>

</div>
