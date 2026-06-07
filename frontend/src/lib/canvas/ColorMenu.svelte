<script lang="ts">
  import { createEventDispatcher } from 'svelte';

  export let username: string;
  export let x: number;
  export let y: number;

  const dispatch = createEventDispatcher<{ pick: { color: string | null } }>();

  const PALETTE = [
    '#e06c75', '#e5c07b', '#98c379', '#56b6c2',
    '#61afef', '#c678dd', '#d19a66', '#abb2bf',
  ];
</script>

<!-- svelte-ignore a11y-no-static-element-interactions -->
<!-- svelte-ignore a11y-click-events-have-key-events -->
<div
  class="fixed z-[99997] rounded-lg border border-slate-300/75 bg-slate-100/92 p-3 shadow-xl shadow-slate-500/20 backdrop-blur dark:border-slate-700/70 dark:bg-slate-950/92 dark:shadow-black/60"
  style="left:{x}px; top:{y}px"
  on:click|stopPropagation
>
  <p class="mb-2 text-[0.68rem] font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400">{username}</p>
  <div class="flex gap-1.5 mb-2">
    {#each PALETTE as color}
      <button
        class="h-5 w-5 rounded-md border border-white/70 shadow-sm transition-transform hover:scale-110 dark:border-slate-900/70"
        style:background={color}
        on:click={() => dispatch('pick', { color })}
        aria-label={color}
      ></button>
    {/each}
  </div>
  <button
    class="rounded-md px-1.5 py-1 text-xs text-slate-500 transition hover:bg-slate-300/45 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-white"
    on:click={() => dispatch('pick', { color: null })}
  >
    clear color
  </button>
</div>
