<script lang="ts">
  import { onMount, onDestroy, createEventDispatcher } from 'svelte';
  import { getAttachmentUrl, uploadProgress } from '../ws/attachments';
  import type { AttachmentSummary } from '../ws';

  export let att: AttachmentSummary;

  const dispatch = createEventDispatcher<{
    open: { url: string; filename: string };
    loaded: void;
  }>();

  // Sender-side upload state for this attachment (undefined for receivers).
  $: upload = $uploadProgress[att.attachment_id]?.status;
  $: uploadReason = $uploadProgress[att.attachment_id]?.reason;

  let url: string | null = null;
  let status: 'loading' | 'ready' | 'error' = 'loading';

  // Receivers see the message before the upload completes; the server only
  // serves bytes once complete. Retry with backoff until it loads.
  const DELAYS = [1500, 2500, 4000, 6000, 8000];
  let attempt = 0;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let destroyed = false;

  function clearRetryTimer() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
  }

  $: if (upload === 'error') {
    clearRetryTimer();
    url = null;
    status = 'error';
  }

  async function tryLoad() {
    try {
      const u = await getAttachmentUrl(att);
      if (destroyed || upload === 'error') return;
      url = u;
      status = 'ready';
    } catch {
      if (destroyed || upload === 'error') return;
      if (attempt < DELAYS.length) {
        const delay = DELAYS[attempt++];
        timer = setTimeout(tryLoad, delay);
      } else {
        status = 'error';
      }
    }
  }

  onMount(tryLoad);
  onDestroy(() => { destroyed = true; clearRetryTimer(); });
</script>

{#if upload === 'error'}
  <div class="mt-1 flex w-fit max-w-full items-center gap-1.5 rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-xs text-red-600 dark:border-red-900/70 dark:bg-red-950/35 dark:text-red-300">
    <span class="truncate max-w-[140px]">{att.filename}</span>
    <span title={uploadReason ?? 'unsupported file'}>rejected</span>
  </div>
{:else if status === 'ready' && url}
  <div class="relative mt-1 max-w-full sm:max-w-[min(34rem,100%)]">
    <button
      type="button"
      class="group relative block max-w-full overflow-hidden rounded-lg border border-slate-300/75 bg-slate-100/70 shadow-sm shadow-slate-400/15 transition hover:border-sky-300 hover:opacity-95 dark:border-slate-800 dark:bg-slate-900/60 dark:shadow-black/30 dark:hover:border-sky-800"
      on:click={() => dispatch('open', { url: url!, filename: att.filename })}
      title="Open {att.filename}"
    >
      <img
        src={url}
        alt={att.filename}
        class="max-h-[420px] max-w-full object-contain"
        on:load={() => dispatch('loaded')}
        on:error={() => dispatch('loaded')}
      />
      <span class="absolute bottom-2 right-2 rounded-md bg-slate-950/70 px-2 py-1 text-xs font-medium text-white/90 opacity-0 backdrop-blur transition group-hover:opacity-100">
        open
      </span>
    </button>

    {#if upload === 'uploading'}
      <!-- Still chunking / awaiting server confirmation -->
      <div class="absolute inset-x-0 bottom-0 bg-black/55 px-2 py-1 flex items-center gap-2">
        <div class="relative flex-1 h-[3px] overflow-hidden rounded-full bg-white/20">
          <div class="relay-sweep absolute inset-y-0 left-0 w-1/3 rounded-full bg-white/85"></div>
        </div>
        <span class="text-[9px] tracking-wider uppercase text-white/90 shrink-0">uploading</span>
      </div>
    {:else if upload === 'complete'}
      <!-- Server confirmed AttachmentComplete -->
      <div class="absolute inset-x-0 bottom-0 bg-emerald-600/80 px-2 py-1 flex items-center justify-end">
        <span class="text-[9px] tracking-wider uppercase text-white">sent ✓</span>
      </div>
    {/if}
  </div>
{:else if status === 'loading'}
  <div class="mt-1 flex items-center gap-1.5 px-2 py-1 text-xs text-slate-400 dark:text-slate-500">
    <span>loading image</span>
  </div>
{:else}
  <div class="mt-1 px-2 py-1 text-xs text-slate-400 dark:text-slate-500">
    image unavailable
  </div>
{/if}
