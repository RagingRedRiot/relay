<script lang="ts">
  import { onMount, onDestroy, tick } from 'svelte';
  import { send, on, currentUsername, isAdmin } from '../ws';
  import type { MessageHistoryItem, AttachmentSummary } from '../ws/types';
  import { userColors } from '../userColors';
  import { openImageViewer, openPane } from './store';
  import { uploadAttachment, uploadProgress, getAttachmentUrl, dropLocalAttachment } from '../ws/attachments';
  import ColorMenu from './ColorMenu.svelte';
  import AttachmentImage from './AttachmentImage.svelte';

  export let roomName: string;
  // True when an admin opens a room they're not a member of (moderation view):
  // history is readable but posting is member-gated server-side, so hide the
  // composer rather than let sends silently fail.
  export let readOnly: boolean = false;
  export let active: boolean = true;
  export let unreadCount: number = 0;

  // ── Message state ─────────────────────────────────────────────────────────

  let messages: MessageHistoryItem[] = [];
  let hasMore = true;
  let loadingMore = false;
  let initialLoaded = false;
  let atBottom = true;
  let stickToBottom = true;
  let initialUnreadCount: number | null = null;
  let pendingInitialScroll = false;

  let savedScrollHeight = 0;
  let savedScrollTop = 0;

  const seen = new Set<string>();
  const removedMessageIds = new Set<string>();
  const rejectedAttachmentIds = new Set<string>();
  let waitingForCreated = false;
  let lastMarkedReadId = '';

  // ── DOM refs ──────────────────────────────────────────────────────────────

  let scrollEl: HTMLDivElement;
  let editor: HTMLDivElement;
  // ── Input ─────────────────────────────────────────────────────────────────

  let input = '';
  let customReaction = '';

  // ── Attachments ─────────────────────────────────────────────────────────────

  let uploading = false;
  let uploadError = '';
  // ── Overlays ──────────────────────────────────────────────────────────────

  let colorMenu: { username: string; x: number; y: number } | null = null;
  let emojiTarget: string | null = null;

  const REACTION_PRESETS = [
    '👍', '👎', '❤️', '🔥', '😂', '👏', '🎉', '🚀',
    '👀', '💯', '✅', '❌', '🙏', '🤝', '🫡', '🧠',
    '🤯', '😮', '😢', '😅', '🥲', '😎', '🤔', '🙌',
    '⭐', '🏆', '💡', '📌', '⚡', '🧵', '🔒', '🛠️',
  ];
  const cleanups: Array<() => void> = [];

  // ── Scroll helpers ────────────────────────────────────────────────────────

  function scrollToBottom() {
    if (scrollEl) scrollEl.scrollTop = scrollEl.scrollHeight;
  }

  function scrollToUnreadBoundary(count: number) {
    if (!scrollEl || count <= 0 || messages.length === 0) {
      scrollToBottom();
      return;
    }

    const firstUnreadIndex = Math.max(0, messages.length - count);
    const messageId = messages[firstUnreadIndex]?.message_id;
    const target = messageId
      ? scrollEl.querySelector<HTMLElement>(`[data-message-id="${messageId}"]`)
      : null;

    if (!target) {
      scrollToBottom();
      return;
    }

    scrollEl.scrollTop = Math.max(0, target.offsetTop - scrollEl.offsetTop - 12);
  }

  async function applyInitialScroll() {
    if (!pendingInitialScroll || initialUnreadCount === null) return;
    pendingInitialScroll = false;
    const count = initialUnreadCount;
    stickToBottom = count <= 0;
    await tick();
    scrollToUnreadBoundary(count);
    checkAtBottom();
    if (active && atBottom) markLatestRead();
  }

  function checkAtBottom() {
    if (!scrollEl) return;
    atBottom = scrollEl.scrollHeight - scrollEl.scrollTop - scrollEl.clientHeight < 60;
  }

  function markLatestRead() {
    const newest = messages.at(-1);
    if (!newest || readOnly || lastMarkedReadId === newest.message_id) return;
    lastMarkedReadId = newest.message_id;
    send({ MarkRead: { room_name: roomName, up_to_message_id: newest.message_id } });
    send('GetUnreadSummary');
  }

  function loadMore() {
    if (loadingMore || !hasMore || !scrollEl) return;
    loadingMore = true;
    savedScrollHeight = scrollEl.scrollHeight;
    savedScrollTop = scrollEl.scrollTop;
    const before = messages[0]?.message_id;
    send({ GetMessages: { room_name: roomName, before, limit: 50 } });
  }

  function onScroll() {
    checkAtBottom();
    stickToBottom = atBottom;
    if (active && atBottom) markLatestRead();
    if (scrollEl.scrollTop < 80 && hasMore && !loadingMore) loadMore();
  }

  async function onAttachmentLoaded() {
    if (!active || !initialLoaded || !stickToBottom || loadingMore) return;
    await tick();
    scrollToBottom();
    markLatestRead();
  }

  function forgetVisibleAttachment(attachmentId: string) {
    dropLocalAttachment(attachmentId);
    downloadErrorIds = new Set([...downloadErrorIds].filter((id) => id !== attachmentId));
    const nextDownloading = new Set(downloadingIds);
    if (nextDownloading.delete(attachmentId)) downloadingIds = nextDownloading;
  }

  function sanitizeMessage(message: MessageHistoryItem): MessageHistoryItem | null {
    if (removedMessageIds.has(message.message_id)) return null;
    const attachments = message.attachments.filter(
      (att) => !rejectedAttachmentIds.has(att.attachment_id),
    );
    if (attachments.length === message.attachments.length) return message;
    if (attachments.length === 0) return null;
    return { ...message, attachments };
  }

  function removeMessageFromView(messageId: string) {
    const target = messages.find((m) => m.message_id === messageId);
    target?.attachments.forEach((att) => forgetVisibleAttachment(att.attachment_id));
    seen.delete(messageId);
    if (emojiTarget === messageId) {
      emojiTarget = null;
      customReaction = '';
    }
    messages = messages.filter((m) => m.message_id !== messageId);
  }

  function removeRejectedAttachmentFromView(attachmentId: string) {
    forgetVisibleAttachment(attachmentId);

    const emptiedMessageIds: string[] = [];
    let changed = false;
    const nextMessages = messages
      .map((message) => {
        if (!message.attachments.some((att) => att.attachment_id === attachmentId)) {
          return message;
        }

        changed = true;
        const attachments = message.attachments.filter((att) => att.attachment_id !== attachmentId);
        if (attachments.length === 0) {
          emptiedMessageIds.push(message.message_id);
          return null;
        }
        return { ...message, attachments };
      })
      .filter((message): message is MessageHistoryItem => message !== null);

    if (!changed) return;
    emptiedMessageIds.forEach((messageId) => {
      removedMessageIds.add(messageId);
      seen.delete(messageId);
    });
    if (emojiTarget && emptiedMessageIds.includes(emojiTarget)) {
      emojiTarget = null;
      customReaction = '';
    }
    messages = nextMessages;
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  onMount(() => {
    cleanups.push(
      on('UnreadSummary', ({ rooms }) => {
        if (initialUnreadCount !== null) return;
        initialUnreadCount = rooms.find((room) => room.room_name === roomName)?.unread ?? unreadCount;
        applyInitialScroll();
      }),

      on('MessageHistory', async ({ room_name, messages: batch }) => {
        if (room_name !== roomName) return;
        const ordered = [...batch]
          .reverse()
          .map(sanitizeMessage)
          .filter((message): message is MessageHistoryItem => message !== null);
        ordered.forEach(m => seen.add(m.message_id));

        if (!initialLoaded) {
          initialLoaded = true;
          messages = ordered;
          pendingInitialScroll = true;
          applyInitialScroll();
        } else if (loadingMore) {
          messages = [...ordered, ...messages];
          loadingMore = false;
          await tick();
          if (scrollEl) scrollEl.scrollTop = savedScrollTop + (scrollEl.scrollHeight - savedScrollHeight);
        }

        hasMore = batch.length >= 50;
      }),

      on('MessageCreated', async ({ message_id, message }) => {
        if (!waitingForCreated) return;
        waitingForCreated = false;
        const cleanMessage = sanitizeMessage(message);
        if (!cleanMessage) return;
        const id = cleanMessage.message_id || message_id;
        if (seen.has(id)) return;
        seen.add(id);
        messages = [...messages, cleanMessage];
        stickToBottom = true;
        await tick();
        scrollToBottom();
        markLatestRead();
      }),

      on('NewMessage', async ({ room_name, message }) => {
        if (room_name !== roomName) return;
        const cleanMessage = sanitizeMessage(message);
        if (!cleanMessage) return;
        if (seen.has(cleanMessage.message_id)) return;
        seen.add(cleanMessage.message_id);
        messages = [...messages, cleanMessage];
        if (atBottom) {
          await tick();
          scrollToBottom();
          markLatestRead();
        }
      }),

      on('MessageRemoved', ({ room_name, message_id }) => {
        if (room_name !== roomName) return;
        removedMessageIds.add(message_id);
        removeMessageFromView(message_id);
      }),

      on('AttachmentRejected', ({ attachment_id }) => {
        rejectedAttachmentIds.add(attachment_id);
        removeRejectedAttachmentFromView(attachment_id);
      }),

      on('Resync', ({ room_name }) => {
        if (room_name !== roomName) return;
        seen.clear();
        messages = [];
        initialLoaded = false;
        hasMore = true;
        stickToBottom = true;
        loadingMore = false;
        send({ GetMessages: { room_name: roomName, limit: 50 } });
      }),
    );

    send('GetUnreadSummary');
    send({ GetMessages: { room_name: roomName, limit: 50 } });
  });

  onDestroy(() => cleanups.forEach(u => u()));

  // ── Actions ───────────────────────────────────────────────────────────────

  function sendMessage() {
    const content = input.trim();
    if (!content) return;
    waitingForCreated = true;
    send({ SendMessage: { room_name: roomName, content } });
    clearEditor();
  }

  function onKeyDown(e: KeyboardEvent) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  }

  // Empty message content is rejected by the server, so an image with no caption
  // falls back to its filename as the message text.
  async function sendFiles(files: FileList | File[]) {
    const list = Array.from(files);
    for (const file of list) {
      const caption = input.trim() || file.name || 'attachment';
      uploading = true;
      uploadError = '';
      waitingForCreated = true;
      try {
        await uploadAttachment(roomName, caption, file);
      } catch (err) {
        waitingForCreated = false;
        uploadError = err instanceof Error ? err.message : 'attachment upload failed';
        console.error('relay: attachment upload failed before send', err);
      } finally {
        uploading = false;
      }
      clearEditor();
    }
  }

  function onFileChange(e: Event) {
    const t = e.target as HTMLInputElement;
    if (t.files && t.files.length) sendFiles(t.files);
    t.value = ''; // allow re-selecting the same file
  }

  async function onPaste(e: ClipboardEvent) {
    const files = filesFromTransfer(e.clipboardData);
    if (files.length > 0) {
      const images = files.filter((f) =>
        f.type.startsWith('image/') || /\.gif$/i.test(f.name),
      );
      if (images.length === 0) return;
      e.preventDefault(); // don't also paste the filename as text
      sendFiles(images);
      return;
    }

    const url = await imageUrlFromTransfer(e.clipboardData);
    if (!url) return;
    e.preventDefault();
    sendFiles([await fileFromImageUrl(url)]);
  }

  function onBeforeInput(e: InputEvent) {
    const files = filesFromTransfer((e as InputEvent & { dataTransfer?: DataTransfer }).dataTransfer);
    if (files.length === 0) return;
    e.preventDefault();
    sendFiles(files);
  }

  function filesFromTransfer(transfer?: DataTransfer | null): File[] {
    if (!transfer) return [];
    const files = Array.from(transfer.files ?? []);
    if (files.length > 0) return files;
    return Array.from(transfer.items ?? [])
      .filter((item) => item.kind === 'file')
      .map((item) => item.getAsFile())
      .filter((file): file is File => file !== null);
  }

  function transferString(item: DataTransferItem): Promise<string> {
    return new Promise((resolve) => item.getAsString((value) => resolve(value ?? '')));
  }

  async function imageUrlFromTransfer(transfer?: DataTransfer | null): Promise<string | null> {
    if (!transfer) return null;
    for (const item of Array.from(transfer.items ?? [])) {
      if (item.kind !== 'string') continue;
      if (!['text/html', 'text/uri-list', 'text/plain'].includes(item.type)) continue;
      const value = await transferString(item);
      const url = imageUrlFromString(value);
      if (url) return url;
    }
    return null;
  }

  function imageUrlFromString(value: string): string | null {
    const trimmed = value.trim();
    if (!trimmed) return null;
    if (/^data:image\/(?:gif|png|jpe?g|webp);base64,/i.test(trimmed)) return trimmed;

    const doc = new DOMParser().parseFromString(trimmed, 'text/html');
    const img = doc.querySelector('img[src]');
    const src = img?.getAttribute('src')?.trim();
    if (src && isImageUrl(src)) return src;

    const match = trimmed.match(/https?:\/\/[^\s"'<>]+/i);
    if (match && isImageUrl(match[0])) return match[0];
    return null;
  }

  function isImageUrl(url: string): boolean {
    return /^data:image\//i.test(url) || /\.(gif|png|jpe?g|webp)(?:[?#].*)?$/i.test(url);
  }

  async function fileFromImageUrl(url: string): Promise<File> {
    const res = await fetch(url);
    if (!res.ok) throw new Error('could not fetch pasted image');
    const blob = await res.blob();
    const type = blob.type || mimeFromUrl(url) || 'image/gif';
    return new File([blob], filenameFromUrl(url, type), { type });
  }

  function mimeFromUrl(url: string): string {
    if (/\.png(?:[?#].*)?$/i.test(url)) return 'image/png';
    if (/\.jpe?g(?:[?#].*)?$/i.test(url)) return 'image/jpeg';
    if (/\.webp(?:[?#].*)?$/i.test(url)) return 'image/webp';
    if (/\.gif(?:[?#].*)?$/i.test(url)) return 'image/gif';
    return '';
  }

  function filenameFromUrl(url: string, type: string): string {
    if (url.startsWith('data:')) return type === 'image/gif' ? 'pasted.gif' : 'pasted-image';
    try {
      const name = new URL(url).pathname.split('/').pop();
      if (name) return name;
    } catch {
      // Fall through to MIME-based default.
    }
    return type === 'image/gif' ? 'pasted.gif' : 'pasted-image';
  }

  function onDrop(e: DragEvent) {
    const files = e.dataTransfer?.files;
    if (!files || files.length === 0) return;
    e.preventDefault();
    sendFiles(files);
  }

  // ── Non-image downloads ──────────────────────────────────────────────────────

  // Per-attachment download state (reassigned, not mutated, so Svelte reacts).
  let downloadingIds = new Set<string>();
  let downloadErrorIds = new Set<string>();

  async function downloadFile(att: AttachmentSummary) {
    if (downloadingIds.has(att.attachment_id)) return;
    downloadErrorIds = new Set([...downloadErrorIds].filter((id) => id !== att.attachment_id));
    downloadingIds = new Set(downloadingIds).add(att.attachment_id);
    try {
      const url = await getAttachmentUrl(att);
      const a = document.createElement('a');
      a.href = url;
      a.download = att.filename || 'download';
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch {
      downloadErrorIds = new Set(downloadErrorIds).add(att.attachment_id);
    } finally {
      const next = new Set(downloadingIds);
      next.delete(att.attachment_id);
      downloadingIds = next;
    }
  }

  function syncEditorInput() {
    input = editor?.innerText.replace(/\n$/, '') ?? '';
    if (editor && input.trim() === '') editor.innerHTML = '';
  }

  function clearEditor() {
    input = '';
    if (editor) editor.innerHTML = '';
  }

  function toggleReaction(msgId: string, emoji: string, reacted: boolean) {
    messages = messages.map(m => {
      if (m.message_id !== msgId) return m;
      let reactions = m.reactions.map(r =>
        r.emoji === emoji
          ? { ...r, count: r.count + (reacted ? -1 : 1), reacted_by_me: !reacted }
          : r,
      );
      if (!reacted && !m.reactions.find(r => r.emoji === emoji)) {
        reactions = [...reactions, { emoji, count: 1, reacted_by_me: true }];
      }
      return { ...m, reactions: reactions.filter(r => r.count > 0) };
    });
    send(reacted
      ? { RemoveReaction: { message_id: msgId, emoji } }
      : { AddReaction: { message_id: msgId, emoji } },
    );
    customReaction = '';
    emojiTarget = null;
  }

  function addCustomReaction(msgId: string) {
    const reaction = customReaction.trim();
    if (!reaction) return;
    const msg = messages.find((m) => m.message_id === msgId);
    const existing = msg?.reactions.find((r) => r.emoji === reaction);
    toggleReaction(msgId, reaction, existing?.reacted_by_me ?? false);
  }

  function sameUsername(a: string | null | undefined, b: string | null | undefined): boolean {
    const left = a?.trim().toLocaleLowerCase() ?? '';
    const right = b?.trim().toLocaleLowerCase() ?? '';
    return left !== '' && left === right;
  }

  function ownMessage(msg: MessageHistoryItem): boolean {
    return sameUsername(msg.sender_username, $currentUsername);
  }

  // A message may be removed by its own sender (an "unsend") or by an admin. The
  // server re-checks this; the client only shows the control when it applies.
  function canDelete(msg: MessageHistoryItem): boolean {
    return $isAdmin || ownMessage(msg);
  }

  // Ask the server to delete a message. On success it broadcasts MessageRemoved,
  // which drops the message from every client (this one included) -- so there's no
  // optimistic removal here; a rejected delete simply leaves the message in place.
  // The deletion is permanent and cascades attachments/reactions, so confirm first.
  function deleteMessage(msg: MessageHistoryItem) {
    const own = ownMessage(msg);
    const prompt = own
      ? 'Unsend this message? This cannot be undone.'
      : `Remove ${msg.sender_username}'s message? This cannot be undone.`;
    if (!confirm(prompt)) return;
    send({ DeleteMessage: { message_id: msg.message_id } });
  }

  function onUsernameRightClick(e: MouseEvent, username: string) {
    e.preventDefault();
    colorMenu = { username, x: e.clientX, y: e.clientY };
  }

  function onColorPick(e: CustomEvent<{ color: string | null }>) {
    if (!colorMenu) return;
    if (e.detail.color) userColors.set(colorMenu.username, e.detail.color);
    else userColors.clear(colorMenu.username);
    colorMenu = null;
  }

  function closeOverlays() {
    colorMenu = null;
    emojiTarget = null;
  }

  function openInfo() {
    openPane(`roominfo:${roomName}`, `#${roomName} · info`, 'roominfo', { width: 340, height: 540 });
  }

  // ── Formatting ────────────────────────────────────────────────────────────

  function formatTime(iso: string): string {
    return new Date(iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  function formatBytes(n: number): string {
    if (n < 1024) return `${n}B`;
    if (n < 1048576) return `${(n / 1024).toFixed(1)}KB`;
    return `${(n / 1048576).toFixed(1)}MB`;
  }

  // A short, friendly format tag derived from the server-recorded content_type
  // (the authoritative value stored at upload). Falls back to the MIME subtype.
  const FORMAT_LABELS: Record<string, string> = {
    'application/octet-stream': 'binary',
    'application/pdf': 'pdf',
    'application/zip': 'zip',
    'application/json': 'json',
    'application/gzip': 'gzip',
    'text/plain': 'text',
    'text/csv': 'csv',
    'text/markdown': 'md',
    'image/svg+xml': 'svg',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/msword': 'doc',
    'application/vnd.ms-excel': 'xls',
  };

  function formatLabel(contentType: string): string {
    if (!contentType) return '';
    const ct = contentType.split(';')[0].trim().toLowerCase();
    if (FORMAT_LABELS[ct]) return FORMAT_LABELS[ct].toUpperCase();
    const sub = ct.split('/')[1] ?? ct;
    return sub.replace(/^x-/, '').replace(/\+.*$/, '').toUpperCase();
  }
</script>

<svelte:window on:click={closeOverlays} />

{#if colorMenu}
  <ColorMenu
    username={colorMenu.username}
    x={colorMenu.x}
    y={colorMenu.y}
    on:pick={onColorPick}
  />
{/if}

<div class="flex h-full flex-col text-slate-800 dark:text-slate-200">

  <!-- Slim header: room actions -->
  <div class="flex shrink-0 items-center justify-between border-b border-slate-300/65 bg-slate-200/35 px-4 py-2 dark:border-slate-800/80 dark:bg-slate-900/35">
    <span class="truncate text-sm font-semibold text-slate-700 dark:text-slate-200">#{roomName}</span>
    <button
      class="rounded-md px-2 py-1 text-xs font-medium text-slate-500 transition hover:bg-slate-300/45 hover:text-sky-700 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-sky-200"
      on:click={openInfo}
      title="Members & settings"
    >
      members
    </button>
  </div>

  <!-- Message list -->
  <!-- svelte-ignore a11y-no-static-element-interactions -->
  <div
    class="min-h-0 flex-1 overflow-y-auto px-5 py-4"
    bind:this={scrollEl}
    on:scroll={onScroll}
    on:drop={onDrop}
    on:dragover|preventDefault
  >
    <!-- Load-more trigger at the top -->
    {#if hasMore}
      <div class="py-2 text-center">
        {#if loadingMore}
          <span class="text-xs text-slate-400 dark:text-slate-500">loading</span>
        {:else}
          <button
            class="rounded-md px-2 py-1 text-xs text-slate-500 transition hover:bg-slate-300/45 hover:text-slate-800 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-slate-100"
            on:click={loadMore}
          >
            load older
          </button>
        {/if}
      </div>
    {/if}

    {#if messages.length === 0 && initialLoaded}
      <p class="py-8 text-center text-sm text-slate-500 dark:text-slate-400">no messages yet</p>
    {/if}

    {#each messages as msg, i}
      {@const showHeader = i === 0 || messages[i - 1].sender_username !== msg.sender_username}
      {@const color = $userColors[msg.sender_username]}

      <div class="group {showHeader && i > 0 ? 'mt-4' : 'mt-1'}" data-message-id={msg.message_id}>

        {#if showHeader}
          <!-- svelte-ignore a11y-no-static-element-interactions -->
          <div class="mb-1 flex items-baseline gap-2">
            <span
              class="cursor-default select-none text-sm font-semibold"
              style:color={color ?? ''}
              on:contextmenu={(e) => onUsernameRightClick(e, msg.sender_username)}
            >{msg.sender_username}</span>
            <span class="text-xs text-slate-400 dark:text-slate-500">{formatTime(msg.timestamp)}</span>
          </div>
        {/if}

        <p class="whitespace-pre-wrap break-words text-sm leading-relaxed text-slate-700 dark:text-slate-200">{msg.content}</p>

        {#if msg.attachments.length > 0}
          <div class="flex flex-col gap-1 mt-1">
            {#each msg.attachments as att (att.attachment_id)}
              {@const upload = $uploadProgress[att.attachment_id]}
              {#if att.content_type.startsWith('image/')}
                <AttachmentImage
                  {att}
                  on:open={(e) => openImageViewer(e.detail.url, e.detail.filename)}
                  on:loaded={onAttachmentLoaded}
                />
              {:else if upload?.status === 'error'}
                <div
                  class="flex w-fit max-w-full items-center gap-1.5 rounded-md border border-red-200 bg-red-50 px-2.5 py-1 text-xs text-red-600 dark:border-red-900/70 dark:bg-red-950/35 dark:text-red-300"
                  title={upload.reason ?? 'unsupported file'}
                >
                  <span>📎</span>
                  <span class="truncate max-w-[140px]">{att.filename}</span>
                  <span>rejected</span>
                </div>
              {:else}
                <button
                  type="button"
                  class="flex w-fit items-center gap-1.5 rounded-md border border-slate-300/75 bg-slate-100/72 px-2.5 py-1 text-xs text-slate-600 transition hover:border-sky-300 hover:bg-sky-100/60 dark:border-slate-700 dark:bg-slate-900/65 dark:text-slate-300 dark:hover:border-sky-800 dark:hover:bg-sky-950/30"
                  on:click|stopPropagation={() => downloadFile(att)}
                  title="Download {att.filename}"
                >
                  <span>📎</span>
                  <span class="truncate max-w-[140px]">{att.filename}</span>
                  <span
                    class="rounded-sm border border-slate-200 px-1 py-px text-[0.62rem] uppercase tracking-wider text-slate-500 dark:border-slate-700 dark:text-slate-400"
                    title={att.content_type}
                  >{formatLabel(att.content_type)}</span>
                    <span class="text-slate-400 dark:text-slate-500">{formatBytes(att.size_bytes)}</span>
                  {#if upload?.status === 'uploading'}
                    <span class="relative h-1 w-6 overflow-hidden rounded-full bg-slate-300 dark:bg-slate-700">
                      <span class="relay-sweep absolute inset-y-0 left-0 w-1/3 rounded-full bg-sky-500 dark:bg-sky-300"></span>
                    </span>
                  {:else if upload?.status === 'complete'}
                    <span class="text-emerald-600 dark:text-emerald-500">✓</span>
                  {:else if downloadingIds.has(att.attachment_id)}
                    <span class="text-slate-500 dark:text-slate-400">↓ ...</span>
                  {:else if downloadErrorIds.has(att.attachment_id)}
                    <span class="text-red-500 dark:text-red-400">unavailable</span>
                  {:else}
                    <span class="text-slate-400 dark:text-slate-500">↓</span>
                  {/if}
                </button>
              {/if}
            {/each}
          </div>
        {/if}

        <!-- Reactions row -->
        <div class="mt-1 flex min-h-6 flex-wrap items-center gap-1.5">
          {#each msg.reactions as r (r.emoji)}
            <button
              class="flex items-center gap-0.5 rounded-md border px-1.5 py-1 text-xs leading-none transition-colors
                {r.reacted_by_me
                  ? 'border-sky-300/80 bg-sky-100/65 text-sky-800 dark:border-sky-800 dark:bg-sky-950/45 dark:text-sky-200'
                  : 'border-slate-200 text-slate-400 dark:border-slate-800 dark:text-slate-400'}"
              on:click|stopPropagation={() => toggleReaction(msg.message_id, r.emoji, r.reacted_by_me)}
            >
              <span>{r.emoji}</span>
              <span>{r.count}</span>
            </button>
          {/each}

          {#if emojiTarget === msg.message_id}
            <!-- svelte-ignore a11y-no-static-element-interactions -->
            <!-- svelte-ignore a11y-click-events-have-key-events -->
            <div
              class="w-full max-w-[18rem] rounded-lg border border-slate-300/80 bg-slate-100 p-2 shadow-lg shadow-slate-400/15 dark:border-slate-700 dark:bg-slate-900 dark:shadow-black/40 sm:w-fit"
              on:click|stopPropagation
            >
              <div class="grid grid-cols-8 gap-1">
                {#each REACTION_PRESETS as emoji}
                  <button
                    class="grid h-8 w-8 place-items-center rounded-md text-lg leading-none transition hover:bg-slate-200/75 hover:scale-105 dark:hover:bg-slate-800"
                    on:click={() => toggleReaction(msg.message_id, emoji, msg.reactions.some(r => r.emoji === emoji && r.reacted_by_me))}
                    title={emoji}
                    aria-label="React with {emoji}"
                  >{emoji}</button>
                {/each}
              </div>
              <form class="mt-2 flex gap-1" on:submit|preventDefault={() => addCustomReaction(msg.message_id)}>
                <input
                  class="min-w-0 flex-1 rounded-md border border-slate-300/80 bg-slate-200/45 px-2 py-1.5 text-sm text-slate-800 outline-none placeholder:text-slate-400 focus:border-sky-500 focus:ring-2 focus:ring-sky-200/45 dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 dark:placeholder:text-slate-500"
                  bind:value={customReaction}
                  maxlength="24"
                  placeholder="custom"
                  autocomplete="off"
                  spellcheck="false"
                />
                <button
                  class="rounded-md bg-slate-800 px-2.5 py-1.5 text-xs font-medium text-white disabled:opacity-40 dark:bg-sky-500 dark:text-slate-950"
                  type="submit"
                  disabled={!customReaction.trim()}
                >
                  add
                </button>
              </form>
            </div>
          {:else}
            <button
              class="rounded-md px-2 py-1 text-xs leading-none text-slate-400 opacity-100 transition hover:bg-slate-300/45 hover:text-slate-700 sm:opacity-0 sm:group-hover:opacity-100 dark:text-slate-500 dark:hover:bg-slate-800 dark:hover:text-slate-200"
              on:click|stopPropagation={() => { customReaction = ''; emojiTarget = msg.message_id; }}
              title="react"
            >+</button>
          {/if}

          {#if canDelete(msg)}
            {@const own = ownMessage(msg)}
            <button
              class="rounded-md px-2 py-1 text-xs leading-none text-slate-400 transition hover:bg-red-100/60 hover:text-red-600 dark:text-slate-500 dark:hover:bg-red-950/40 dark:hover:text-red-300"
              on:click|stopPropagation={() => deleteMessage(msg)}
              title={own ? 'Unsend this message' : `Remove ${msg.sender_username}'s message`}
            >{own ? 'unsend' : 'remove'}</button>
          {/if}
        </div>

      </div>
    {/each}
  </div>

  <!-- Input bar (hidden in read-only moderation view) -->
  {#if readOnly}
    <div class="shrink-0 border-t border-slate-200 px-4 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400 select-none dark:border-slate-800 dark:text-slate-500">
      read-only · moderation view
    </div>
  {:else}
  <div class="flex shrink-0 border-t border-slate-300/70 bg-slate-200/35 px-3 py-3 dark:border-slate-800 dark:bg-slate-900/35 sm:px-4">
    <div class="flex min-w-0 flex-1 items-end gap-2">
    <label
      class="relative mb-0.5 grid h-8 w-8 shrink-0 place-items-center overflow-hidden rounded-md text-base text-slate-500 transition hover:bg-slate-300/45 hover:text-sky-700 has-[:disabled]:opacity-40 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-sky-200"
      title="Attach a file"
    >
      <span aria-hidden="true">📎</span>
      <input
        type="file"
        class="absolute inset-0 h-full w-full cursor-pointer opacity-0"
        on:change={onFileChange}
        disabled={uploading}
        multiple
        aria-label="Attach a file"
      />
    </label>
    <!-- svelte-ignore a11y-no-static-element-interactions -->
    <div
      class="min-h-8 max-h-[120px] flex-1 overflow-y-auto whitespace-pre-wrap break-words rounded-md border border-slate-300/80 bg-slate-100/78 px-3 py-1.5 text-sm leading-relaxed text-slate-800 outline-none transition empty:before:text-slate-400 empty:before:content-[attr(data-placeholder)] focus:border-sky-500 focus:bg-slate-100 focus:ring-4 focus:ring-sky-200/45 dark:border-slate-700 dark:bg-slate-950/45 dark:text-slate-200 dark:empty:before:text-slate-500 dark:focus:border-sky-500 dark:focus:bg-slate-950/70 dark:focus:ring-sky-950"
      bind:this={editor}
      contenteditable="true"
      role="textbox"
      tabindex="0"
      aria-disabled={uploading}
      aria-multiline="true"
      data-placeholder={uploading ? 'uploading...' : `message #${roomName}`}
      on:keydown={onKeyDown}
      on:beforeinput={onBeforeInput}
      on:input={syncEditorInput}
      on:paste={onPaste}
      spellcheck="false"
    ></div>
    </div>
    {#if uploadError}
      <p class="ml-2 self-center text-xs text-red-500 dark:text-red-400">{uploadError}</p>
    {/if}
  </div>
  {/if}

</div>
