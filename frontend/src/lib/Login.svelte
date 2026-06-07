<script lang="ts">
  import { onDestroy } from 'svelte';
  import { connect, send, on, setCurrentUser, connectionState, signupsOpen } from './ws';
  import { fontSize, isDark, MAX_FONT_SIZE, MIN_FONT_SIZE, setFontSize, toggleTheme } from './theme';

  type Mode = 'login' | 'register';

  let mode: Mode = 'login';
  let notice = '';

  let username = '';
  let password = '';
  let confirmPassword = '';
  let firstName = '';
  let lastName = '';
  let alias = '';

  let error = '';
  let loading = false;
  let cleanups: Array<() => void> = [];

  function cleanup() {
    cleanups.forEach((u) => u());
    cleanups = [];
  }

  function switchMode(m: Mode) {
    cleanup();
    error = '';
    loading = false;
    notice = '';
    mode = m;
    password = '';
    confirmPassword = '';
    firstName = '';
    lastName = '';
    alias = '';
  }

  function handleLogin() {
    if (!username || !password) return;
    loading = true;
    error = '';
    cleanup();

    cleanups.push(
      on('AuthOk', () => {
        cleanup();
        setCurrentUser(username.trim());
      }),
    );

    cleanups.push(
      on('NoAuth', () => {
        cleanup();
        error = 'Invalid username or password.';
        loading = false;
      }),
    );

    send({ Auth: { username, password } });
  }

  function handleRegister() {
    if (!username || !password) return;
    if (password !== confirmPassword) {
      error = 'Passwords do not match.';
      return;
    }
    loading = true;
    error = '';
    cleanup();

    cleanups.push(
      on('UserCreated', () => {
        cleanup();
        connect();
        mode = 'login';
        notice = 'Account created. Log in below.';
        loading = false;
        password = '';
        confirmPassword = '';
      }),
    );

    cleanups.push(
      on('Failed', () => {
        cleanup();
        error = 'Registration failed. Username may already be taken.';
        loading = false;
      }),
    );

    cleanups.push(
      on('NoAuth', () => {
        cleanup();
        error = 'Registrations are not open.';
        loading = false;
      }),
    );

    send({
      NewUser: {
        username,
        password,
        ...(firstName ? { first_name: firstName } : {}),
        ...(lastName ? { last_name: lastName } : {}),
        ...(alias ? { alias } : {}),
      },
    });
  }

  function onSubmit(e: SubmitEvent) {
    e.preventDefault();
    if (mode === 'login') handleLogin();
    else handleRegister();
  }

  onDestroy(() => cleanup());

  $: connected = $connectionState === 'connected';
  $: submitDisabled = !connected || loading || !username || !password;

  const inputClass =
    'w-full rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-2 text-sm ' +
    'text-slate-800 outline-none transition placeholder:text-slate-400 ' +
    'focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 ' +
    'disabled:opacity-60 dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-100 ' +
    'dark:placeholder:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950';

  const labelClass =
    'mb-1.5 block text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400';
</script>

<div class="min-h-screen bg-[radial-gradient(circle_at_18%_14%,rgba(14,165,233,0.10),transparent_30%),linear-gradient(135deg,#d9e1ea_0%,#edf1f5_48%,#dde8e3_100%)] px-4 text-slate-800 dark:bg-[radial-gradient(circle_at_20%_16%,rgba(14,165,233,0.15),transparent_31%),linear-gradient(135deg,#0c1117_0%,#101820_50%,#111513_100%)] dark:text-slate-100">
  <div class="fixed right-4 top-4 z-10 flex items-center gap-2">
    <div class="flex h-9 items-center gap-2 rounded-md border border-slate-300/75 bg-slate-100/82 px-2.5 text-slate-600 shadow-sm shadow-slate-400/15 backdrop-blur dark:border-slate-700/70 dark:bg-slate-900/78 dark:text-slate-300 dark:shadow-black/30">
      <span class="text-xs font-semibold" aria-hidden="true">A</span>
      <input
        class="h-1.5 w-24 accent-sky-600"
        type="range"
        min={MIN_FONT_SIZE}
        max={MAX_FONT_SIZE}
        value={$fontSize}
        on:input={(e) => setFontSize(Number(e.currentTarget.value))}
        title="Font size"
        aria-label="Font size"
      />
      <span class="w-5 text-right text-xs tabular-nums">{$fontSize}</span>
    </div>

    <button
      class="grid h-9 w-9 place-items-center rounded-md border border-slate-300/75 bg-slate-100/82 text-base text-slate-600 shadow-sm shadow-slate-400/15 backdrop-blur transition hover:border-sky-400 hover:text-sky-700 dark:border-slate-700/70 dark:bg-slate-900/78 dark:text-slate-300 dark:shadow-black/30 dark:hover:border-sky-500/70 dark:hover:text-sky-200"
      on:click={toggleTheme}
      title={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      aria-label={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
    >
      {$isDark ? '☀' : '☾'}
    </button>
  </div>

  <div class="mx-auto flex min-h-screen w-full max-w-sm flex-col justify-center pb-16 pt-24">
    <div class="mb-8">
      <p class="text-3xl font-semibold tracking-normal text-slate-950 dark:text-white">relay</p>
      <p class="mt-2 text-sm text-slate-500 dark:text-slate-400">
        {mode === 'login' ? 'Sign in to your rooms.' : 'Create an account to start messaging.'}
      </p>
    </div>

    {#if notice}
      <p class="mb-5 rounded-md border border-emerald-300/70 bg-emerald-100/65 px-3 py-2 text-sm text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-300">{notice}</p>
    {/if}

    <form on:submit={onSubmit} class="flex flex-col gap-4 rounded-lg border border-slate-300/70 bg-slate-100/86 p-5 shadow-2xl shadow-slate-500/18 backdrop-blur-xl dark:border-slate-700/70 dark:bg-slate-950/78 dark:shadow-black/60">
      <div>
        <label class={labelClass} for="username">username</label>
        <input
          id="username"
          type="text"
          bind:value={username}
          autocomplete={mode === 'login' ? 'username' : 'off'}
          class={inputClass}
          disabled={loading}
        />
      </div>

      <div>
        <label class={labelClass} for="password">password</label>
        <input
          id="password"
          type="password"
          bind:value={password}
          autocomplete={mode === 'login' ? 'current-password' : 'new-password'}
          class={inputClass}
          disabled={loading}
        />
      </div>

      {#if mode === 'register'}
        <div>
          <label class={labelClass} for="confirm">confirm password</label>
          <input
            id="confirm"
            type="password"
            bind:value={confirmPassword}
            autocomplete="new-password"
            class={inputClass}
            disabled={loading}
          />
        </div>

        <div class="mt-1 flex flex-col gap-4 border-t border-slate-200 pt-4 dark:border-slate-800">
          <div>
            <label class={labelClass} for="firstName">
              first name <span class="normal-case text-slate-400 dark:text-slate-500">(optional)</span>
            </label>
            <input
              id="firstName"
              type="text"
              bind:value={firstName}
              autocomplete="given-name"
              class={inputClass}
              disabled={loading}
            />
          </div>

          <div>
            <label class={labelClass} for="lastName">
              last name <span class="normal-case text-slate-400 dark:text-slate-500">(optional)</span>
            </label>
            <input
              id="lastName"
              type="text"
              bind:value={lastName}
              autocomplete="family-name"
              class={inputClass}
              disabled={loading}
            />
          </div>

          <div>
            <label class={labelClass} for="alias">
              alias <span class="normal-case text-slate-400 dark:text-slate-500">(optional)</span>
            </label>
            <input
              id="alias"
              type="text"
              bind:value={alias}
              autocomplete="nickname"
              class={inputClass}
              disabled={loading}
            />
          </div>
        </div>
      {/if}

      {#if error}
        <p class="-mt-1 rounded-md border border-red-300/70 bg-red-100/60 px-3 py-2 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300">{error}</p>
      {/if}

      <div class="flex items-center justify-between mt-2">
        {#if mode === 'login'}
          {#if $signupsOpen}
            <button
              type="button"
              class="text-sm text-slate-500 transition-colors hover:text-slate-900 dark:text-slate-400 dark:hover:text-white"
              on:click={() => switchMode('register')}
            >
              register
            </button>
          {:else}
            <!-- signups closed: no register affordance; spacer keeps submit right-aligned -->
            <span></span>
          {/if}
        {:else}
          <button
            type="button"
            class="text-sm text-slate-500 transition-colors hover:text-slate-900 dark:text-slate-400 dark:hover:text-white"
            on:click={() => switchMode('login')}
          >
            back
          </button>
        {/if}

        <button
          type="submit"
          disabled={submitDisabled}
          class="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white shadow-sm transition hover:bg-sky-700 disabled:opacity-40 dark:bg-sky-500 dark:text-slate-950 dark:hover:bg-sky-400"
        >
          {#if !connected}
            connecting
          {:else if loading}
            {mode === 'login' ? 'logging in' : 'creating'}
          {:else}
            {mode === 'login' ? 'log in' : 'create account'}
          {/if}
        </button>
      </div>
    </form>

    {#if $connectionState === 'connecting'}
      <p class="mt-6 text-xs text-slate-500 dark:text-slate-400">connecting...</p>
    {/if}
  </div>
</div>
