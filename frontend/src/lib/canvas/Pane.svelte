<script lang="ts">
  import { closePane, focusPane, movePane, resizePane } from './store';
  import type { PaneState } from './types';

  export let pane: PaneState;

  let dragging = false;
  let resizing = false;

  let originX = 0;
  let originY = 0;
  let resizeBaseW = 0;
  let resizeBaseH = 0;

  function onDragMove(e: MouseEvent) {
    movePane(pane.id, e.clientX - originX, Math.max(0, e.clientY - originY));
  }

  function onResizeMove(e: MouseEvent) {
    resizePane(
      pane.id,
      resizeBaseW + (e.clientX - originX),
      resizeBaseH + (e.clientY - originY),
    );
  }

  function stopAll() {
    if (dragging) {
      window.removeEventListener('mousemove', onDragMove);
      dragging = false;
    }
    if (resizing) {
      window.removeEventListener('mousemove', onResizeMove);
      resizing = false;
    }
    window.removeEventListener('mouseup', stopAll);
  }

  function startDrag(e: MouseEvent) {
    if (e.button !== 0) return;
    e.preventDefault();
    focusPane(pane.id);
    originX = e.clientX - pane.x;
    originY = e.clientY - pane.y;
    dragging = true;
    window.addEventListener('mousemove', onDragMove);
    window.addEventListener('mouseup', stopAll);
  }

  function startResize(e: MouseEvent) {
    if (e.button !== 0) return;
    e.preventDefault();
    e.stopPropagation();
    focusPane(pane.id);
    originX = e.clientX;
    originY = e.clientY;
    resizeBaseW = pane.width;
    resizeBaseH = pane.height;
    resizing = true;
    window.addEventListener('mousemove', onResizeMove);
    window.addEventListener('mouseup', stopAll);
  }
</script>

<!-- svelte-ignore a11y-no-static-element-interactions -->
<div
  class="fixed flex flex-col overflow-hidden rounded-lg
         border border-slate-300/70 bg-slate-100/88 text-slate-800
         shadow-2xl shadow-slate-500/20 backdrop-blur-xl
         dark:border-slate-700/70 dark:bg-slate-950/86 dark:text-slate-200 dark:shadow-black/70"
  class:select-none={dragging || resizing}
  style="left:{pane.x}px; top:{pane.y}px; width:{pane.width}px; height:{pane.height}px; z-index:{pane.z}"
  on:mousedown={() => focusPane(pane.id)}
>
  <!-- Title bar — drag handle -->
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class="flex h-10 shrink-0 items-center justify-between border-b border-slate-200/70
           bg-slate-200/58 px-3 dark:border-slate-800/80 dark:bg-slate-900/82
           cursor-move select-none"
    on:mousedown={startDrag}
  >
    <span class="truncate text-xs font-semibold tracking-wide text-slate-600 dark:text-slate-300">{pane.title}</span>

    <button
      class="-mr-1 ml-3 grid h-6 w-6 shrink-0 place-items-center rounded-md
             text-slate-400 transition-colors hover:bg-slate-200/70 hover:text-slate-700
             dark:text-slate-500 dark:hover:bg-slate-800 dark:hover:text-slate-200"
      on:click|stopPropagation={() => closePane(pane.id)}
      on:mousedown|stopPropagation
      tabindex="-1"
      aria-label="Close pane"
    >
      ✕
    </button>
  </div>

  <!-- Content area -->
  <div
    class="flex-1 min-h-0"
    style:pointer-events={dragging || resizing ? 'none' : 'auto'}
  >
    <slot />
  </div>

  <!-- Resize handle — bottom-right corner -->
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class="absolute bottom-0 right-0 w-5 h-5 cursor-se-resize select-none"
    on:mousedown={startResize}
  >
    <div class="absolute bottom-1.5 right-1.5 h-2 w-2 border-b border-r border-slate-300/80 dark:border-slate-600/80"></div>
  </div>
</div>
