<script lang="ts">
  import { closeImageViewer, closePane, focusPane, imageViewer, openPane, panes, togglePane } from './store';
  import { fontSize, isDark, MAX_FONT_SIZE, MIN_FONT_SIZE, setFontSize, toggleTheme } from '../theme';
  import Pane from './Pane.svelte';
  import Directory from './Directory.svelte';
  import Room from './Room.svelte';
  import ProfilePanel from './ProfilePanel.svelte';
  import AdminPanel from './AdminPanel.svelte';
  import RoomInfoPanel from './RoomInfoPanel.svelte';

  function toggleDirectory() {
    togglePane('__directory__', 'directory', 'directory', { width: 340, height: 540 });
  }

  function openDirectory() {
    openPane('__directory__', 'directory', 'directory', { width: 340, height: 540 });
  }

  $: sortedPanes = [...$panes].sort((a, b) => b.z - a.z);
  $: activePane = sortedPanes[0] ?? null;
  $: roomPanes = $panes
    .filter((p) => p.type === 'room')
    .sort((a, b) => a.z - b.z);
  $: activeRoomIndex = activePane
    ? roomPanes.findIndex((p) => p.id === activePane.id)
    : -1;

  let swipeStartX = 0;
  let swipeStartY = 0;
  let swipedRoomHandle = false;
  let viewportWidth = 1024;

  $: isMobile = viewportWidth < 768;

  function focusRoomAt(index: number) {
    if (roomPanes.length === 0) return;
    const next = (index + roomPanes.length) % roomPanes.length;
    focusPane(roomPanes[next].id);
  }

  function onSwipeStart(e: TouchEvent) {
    const touch = e.touches[0];
    swipedRoomHandle = false;
    swipeStartX = touch.clientX;
    swipeStartY = touch.clientY;
  }

  function onSwipeEnd(e: TouchEvent) {
    const touch = e.changedTouches[0];
    const dx = touch.clientX - swipeStartX;
    const dy = touch.clientY - swipeStartY;
    if (Math.abs(dx) < 44 || Math.abs(dx) < Math.abs(dy) || activeRoomIndex < 0) return;
    swipedRoomHandle = true;
    focusRoomAt(activeRoomIndex + (dx < 0 ? 1 : -1));
  }

  function onSwipeHandleClick() {
    if (swipedRoomHandle) {
      swipedRoomHandle = false;
      return;
    }
    focusRoomAt(activeRoomIndex + 1);
  }

  let viewerUrl = '';
  let imageZoom = 1;
  let imagePanX = 0;
  let imagePanY = 0;
  let draggingImage = false;
  let dragStartX = 0;
  let dragStartY = 0;
  let dragBaseX = 0;
  let dragBaseY = 0;
  let pinchStartDistance = 0;
  let pinchStartZoom = 1;
  let pinchStartCenterX = 0;
  let pinchStartCenterY = 0;
  let pinchBaseX = 0;
  let pinchBaseY = 0;
  let lastTapAt = 0;
  let lastTapX = 0;
  let lastTapY = 0;
  const imagePointers = new Map<number, { x: number; y: number }>();

  $: if (($imageViewer?.url ?? '') !== viewerUrl) {
    viewerUrl = $imageViewer?.url ?? '';
    resetImageZoom();
  }

  function resetImageZoom() {
    imageZoom = 1;
    imagePanX = 0;
    imagePanY = 0;
    draggingImage = false;
    imagePointers.clear();
  }

  function setImageZoom(next: number) {
    imageZoom = Math.min(5, Math.max(1, Number(next.toFixed(2))));
    if (imageZoom === 1) {
      imagePanX = 0;
      imagePanY = 0;
    }
  }

  function zoomImage(delta: number) {
    setImageZoom(imageZoom + delta);
  }

  function onImageWheel(e: WheelEvent) {
    zoomImage(e.deltaY < 0 ? 0.35 : -0.35);
  }

  function pointerPair() {
    return [...imagePointers.values()].slice(0, 2);
  }

  function pointerDistance(a: { x: number; y: number }, b: { x: number; y: number }) {
    return Math.hypot(a.x - b.x, a.y - b.y);
  }

  function pointerCenter(a: { x: number; y: number }, b: { x: number; y: number }) {
    return { x: (a.x + b.x) / 2, y: (a.y + b.y) / 2 };
  }

  function startPinch() {
    const [a, b] = pointerPair();
    if (!a || !b) return;
    const center = pointerCenter(a, b);
    pinchStartDistance = pointerDistance(a, b);
    pinchStartZoom = imageZoom;
    pinchStartCenterX = center.x;
    pinchStartCenterY = center.y;
    pinchBaseX = imagePanX;
    pinchBaseY = imagePanY;
    draggingImage = false;
  }

  function toggleQuickZoom() {
    if (imageZoom > 1) {
      resetImageZoom();
    } else {
      setImageZoom(2.5);
    }
  }

  function maybeDoubleTap(e: PointerEvent): boolean {
    if (e.pointerType === 'mouse') return false;
    const now = Date.now();
    const closeEnough = Math.hypot(e.clientX - lastTapX, e.clientY - lastTapY) < 36;
    const isDoubleTap = now - lastTapAt < 320 && closeEnough;
    lastTapAt = now;
    lastTapX = e.clientX;
    lastTapY = e.clientY;
    if (!isDoubleTap) return false;
    toggleQuickZoom();
    return true;
  }

  function startImageDrag(e: PointerEvent) {
    if (e.pointerType === 'mouse' && e.button !== 0) return;
    e.preventDefault();
    (e.currentTarget as HTMLElement).setPointerCapture?.(e.pointerId);
    if (imagePointers.size === 0 && maybeDoubleTap(e)) return;
    imagePointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (imagePointers.size >= 2) {
      startPinch();
      return;
    }
    if (imageZoom <= 1) return;
    draggingImage = true;
    dragStartX = e.clientX;
    dragStartY = e.clientY;
    dragBaseX = imagePanX;
    dragBaseY = imagePanY;
  }

  function onImageDrag(e: PointerEvent) {
    if (!imagePointers.has(e.pointerId)) return;
    e.preventDefault();
    imagePointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
    if (imagePointers.size >= 2) {
      const [a, b] = pointerPair();
      if (!a || !b || pinchStartDistance <= 0) return;
      const center = pointerCenter(a, b);
      setImageZoom(pinchStartZoom * (pointerDistance(a, b) / pinchStartDistance));
      if (imageZoom > 1) {
        imagePanX = pinchBaseX + center.x - pinchStartCenterX;
        imagePanY = pinchBaseY + center.y - pinchStartCenterY;
      }
      return;
    }
    if (!draggingImage) return;
    imagePanX = dragBaseX + e.clientX - dragStartX;
    imagePanY = dragBaseY + e.clientY - dragStartY;
  }

  function stopImageDrag(e?: PointerEvent) {
    if (e) {
      imagePointers.delete(e.pointerId);
      (e.currentTarget as HTMLElement).releasePointerCapture?.(e.pointerId);
    } else {
      imagePointers.clear();
    }
    draggingImage = false;
    if (imagePointers.size === 1 && imageZoom > 1) {
      const remaining = [...imagePointers.values()][0];
      draggingImage = true;
      dragStartX = remaining.x;
      dragStartY = remaining.y;
      dragBaseX = imagePanX;
      dragBaseY = imagePanY;
    }
  }

  function closeViewer() {
    closeImageViewer();
    resetImageZoom();
  }

  const btnClass =
    'grid h-9 w-9 place-items-center rounded-md border border-slate-300/75 bg-slate-100/82 text-slate-600 ' +
    'shadow-sm shadow-slate-300/20 backdrop-blur transition hover:border-sky-300 hover:text-sky-700 ' +
    'dark:border-slate-700/70 dark:bg-slate-900/78 dark:text-slate-300 dark:shadow-black/30 ' +
    'dark:hover:border-sky-500/70 dark:hover:text-sky-200';
</script>

<svelte:window
  bind:innerWidth={viewportWidth}
  on:keydown={(e) => { if (e.key === 'Escape') closeViewer(); }}
/>

<!-- Canvas background -->
<div class="fixed inset-0 overflow-hidden bg-[radial-gradient(circle_at_15%_12%,rgba(14,165,233,0.10),transparent_28%),linear-gradient(135deg,#d9e1ea_0%,#edf1f5_45%,#dde8e3_100%)] dark:bg-[radial-gradient(circle_at_14%_10%,rgba(14,165,233,0.16),transparent_30%),linear-gradient(135deg,#0c1117_0%,#101820_48%,#111513_100%)]"></div>
<div class="fixed inset-0 bg-[linear-gradient(rgba(15,23,42,0.045)_1px,transparent_1px),linear-gradient(90deg,rgba(15,23,42,0.045)_1px,transparent_1px)] bg-[size:36px_36px] opacity-55 dark:bg-[linear-gradient(rgba(226,232,240,0.035)_1px,transparent_1px),linear-gradient(90deg,rgba(226,232,240,0.035)_1px,transparent_1px)]"></div>

<div class="fixed left-4 top-4 z-[99999] hidden items-center gap-2 md:flex">
  <button
    class="{btnClass} flex-col justify-center gap-[4px]"
    on:click={toggleDirectory}
    title="Directory"
    aria-label="Open directory"
  >
    <span class="block h-0.5 w-4 rounded-full bg-current"></span>
    <span class="block h-0.5 w-4 rounded-full bg-current"></span>
    <span class="block h-0.5 w-4 rounded-full bg-current"></span>
  </button>

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
</div>

<button
  class="{btnClass} fixed right-4 top-4 z-[99999] hidden text-base md:grid"
  on:click={toggleTheme}
  title={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
  aria-label={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
>
  {$isDark ? '☀' : '☾'}
</button>

<!-- Panes -->
{#if !isMobile}
  {#each $panes as pane (pane.id)}
    <Pane {pane}>
      {#if pane.type === 'directory'}
        <Directory />
      {:else if pane.type === 'profile'}
        <ProfilePanel />
      {:else if pane.type === 'admin'}
        <AdminPanel />
      {:else if pane.type === 'roominfo'}
        <RoomInfoPanel roomName={pane.id.replace(/^roominfo:/, '')} />
      {:else}
        <Room
          roomName={pane.id}
          readOnly={pane.readOnly ?? false}
          active={true}
          unreadCount={pane.unread ?? 0}
        />
      {/if}
    </Pane>
  {/each}
{/if}

{#if isMobile}
<div class="fixed inset-0 z-[99990] flex flex-col">
  <div class="flex h-14 shrink-0 items-center gap-2 border-b border-slate-300/70 bg-slate-100/92 px-3 shadow-sm shadow-slate-500/10 backdrop-blur dark:border-slate-800 dark:bg-slate-950/92">
    <button
      class="grid h-10 w-10 shrink-0 place-items-center rounded-md border border-slate-300/75 bg-slate-200/55 text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300"
      on:click={openDirectory}
      aria-label="Open directory"
      title="Directory"
    >
      <span class="flex flex-col gap-1">
        <span class="block h-0.5 w-4 rounded-full bg-current"></span>
        <span class="block h-0.5 w-4 rounded-full bg-current"></span>
        <span class="block h-0.5 w-4 rounded-full bg-current"></span>
      </span>
    </button>

    <div class="min-w-0 flex-1">
      <p class="truncate text-sm font-semibold text-slate-800 dark:text-slate-100">
        {activePane?.title ?? 'relay'}
      </p>
      <p class="truncate text-xs text-slate-500 dark:text-slate-400">
        {activePane ? (activePane.type === 'room' ? 'chat' : activePane.type) : 'open the directory to start'}
      </p>
    </div>

    <button
      class="grid h-10 w-10 shrink-0 place-items-center rounded-md border border-slate-300/75 bg-slate-200/55 text-base text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300"
      on:click={toggleTheme}
      aria-label={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
      title={$isDark ? 'Switch to light mode' : 'Switch to dark mode'}
    >
      {$isDark ? '☀' : '☾'}
    </button>

    {#if activePane}
      <button
        class="grid h-10 w-10 shrink-0 place-items-center rounded-md border border-slate-300/75 bg-slate-200/55 text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400"
        on:click={() => closePane(activePane!.id)}
        aria-label="Close current view"
        title="Close"
      >
        ✕
      </button>
    {/if}
  </div>

  <main class="min-h-0 flex-1 overflow-hidden bg-slate-100/88 dark:bg-slate-950/86">
    {#if activePane}
      {#key activePane.id}
        <section class="h-full min-h-0 overflow-hidden">
          {#if activePane.type === 'directory'}
            <Directory />
          {:else if activePane.type === 'profile'}
            <ProfilePanel />
          {:else if activePane.type === 'admin'}
            <AdminPanel />
          {:else if activePane.type === 'roominfo'}
            <RoomInfoPanel roomName={activePane.id.replace(/^roominfo:/, '')} />
          {:else}
            <Room
              roomName={activePane.id}
              readOnly={activePane.readOnly ?? false}
              active={true}
              unreadCount={activePane.unread ?? 0}
            />
          {/if}
        </section>
      {/key}
    {:else}
      <div class="flex h-full flex-col items-center justify-center gap-4 px-6 text-center">
        <p class="text-lg font-semibold text-slate-800 dark:text-slate-100">No view open</p>
        <button
          class="rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white dark:bg-sky-500 dark:text-slate-950"
          on:click={openDirectory}
        >
          Open directory
        </button>
      </div>
    {/if}
  </main>

  <div class="shrink-0 border-t border-slate-300/70 bg-slate-100/94 px-2 pb-[max(0.5rem,env(safe-area-inset-bottom))] pt-2 backdrop-blur dark:border-slate-800 dark:bg-slate-950/94">
    {#if roomPanes.length > 1 && activeRoomIndex >= 0}
      <button
        type="button"
        class="mx-auto mb-2 flex h-8 w-28 touch-pan-y items-center justify-center rounded-full border border-slate-300/70 bg-slate-200/70 text-xs font-medium text-slate-500 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-400"
        on:click={onSwipeHandleClick}
        on:touchstart={onSwipeStart}
        on:touchend={onSwipeEnd}
        title="Swipe to switch rooms"
        aria-label="Swipe or tap to switch rooms"
      >
        swipe rooms
      </button>
    {/if}

    {#if roomPanes.length > 0}
      <div class="flex gap-2 overflow-x-auto px-1 pb-1">
        {#each roomPanes as room}
          <button
            class="min-w-0 shrink-0 rounded-md border px-3 py-2 text-sm font-medium transition
              {activePane?.id === room.id
                ? 'border-sky-400 bg-sky-100/70 text-sky-800 dark:border-sky-700 dark:bg-sky-950/60 dark:text-sky-200'
                : 'border-slate-300/70 bg-slate-200/45 text-slate-600 dark:border-slate-700 dark:bg-slate-900/70 dark:text-slate-300'}"
            on:click={() => focusPane(room.id)}
          >
            <span class="block max-w-32 truncate">#{room.id}</span>
          </button>
        {/each}
      </div>
    {:else}
      <p class="px-2 py-2 text-center text-xs text-slate-500 dark:text-slate-400">Open rooms from the directory.</p>
    {/if}
  </div>
</div>
{/if}

{#if $imageViewer}
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <!-- svelte-ignore a11y-click-events-have-key-events -->
  <div
    class="fixed inset-0 z-[100000] flex cursor-zoom-out items-center justify-center bg-slate-950/92 p-4 backdrop-blur-sm sm:p-8"
    on:click={closeViewer}
  >
    <div class="absolute left-4 right-4 top-4 flex items-center justify-between gap-3 text-white sm:left-6 sm:right-6">
      <p class="truncate text-sm font-medium text-white/80">{$imageViewer.filename}</p>
      <div class="flex shrink-0 items-center gap-2">
        <div class="flex h-9 items-center overflow-hidden rounded-md bg-white/10 text-white/80">
          <button
            class="grid h-9 w-9 place-items-center transition hover:bg-white/12 hover:text-white disabled:opacity-35"
            on:click|stopPropagation={() => zoomImage(-0.5)}
            disabled={imageZoom <= 1}
            aria-label="Zoom out"
            title="Zoom out"
          >
            -
          </button>
          <button
            class="h-9 min-w-14 px-2 text-xs font-semibold tabular-nums transition hover:bg-white/12 hover:text-white"
            on:click|stopPropagation={resetImageZoom}
            aria-label="Reset zoom"
            title="Reset zoom"
          >
            {Math.round(imageZoom * 100)}%
          </button>
          <button
            class="grid h-9 w-9 place-items-center transition hover:bg-white/12 hover:text-white disabled:opacity-35"
            on:click|stopPropagation={() => zoomImage(0.5)}
            disabled={imageZoom >= 5}
            aria-label="Zoom in"
            title="Zoom in"
          >
            +
          </button>
        </div>
        <button
          class="grid h-9 w-9 place-items-center rounded-md bg-white/10 text-lg text-white/80 transition hover:bg-white/18 hover:text-white"
          on:click|stopPropagation={closeViewer}
          aria-label="Close image viewer"
          title="Close"
        >
          ✕
        </button>
      </div>
    </div>
    <!-- svelte-ignore a11y-no-static-element-interactions -->
    <div
      class="max-h-[calc(100vh-7rem)] max-w-[calc(100vw-2rem)] touch-none overflow-hidden rounded-md sm:max-w-[calc(100vw-4rem)]"
      class:cursor-grab={imageZoom > 1 && !draggingImage}
      class:cursor-grabbing={draggingImage}
      on:click|stopPropagation
      on:pointerdown|stopPropagation={startImageDrag}
      on:pointermove|stopPropagation={onImageDrag}
      on:pointerup|stopPropagation={stopImageDrag}
      on:pointercancel|stopPropagation={stopImageDrag}
      on:dblclick|stopPropagation={toggleQuickZoom}
      on:wheel|preventDefault={onImageWheel}
    >
      <img
        src={$imageViewer.url}
        alt={$imageViewer.filename}
        class="max-h-[calc(100vh-7rem)] max-w-[calc(100vw-2rem)] select-none rounded-md object-contain shadow-2xl shadow-black/60 transition-transform duration-100 sm:max-w-[calc(100vw-4rem)]"
        class:cursor-default={imageZoom === 1}
        draggable="false"
        style="transform: translate({imagePanX}px, {imagePanY}px) scale({imageZoom});"
      />
    </div>
    <p class="absolute bottom-4 left-4 right-4 text-center text-xs text-white/55 sm:bottom-6">
      Scroll to zoom. Drag while zoomed to inspect details.
    </p>
  </div>
{/if}
