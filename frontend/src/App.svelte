<script lang="ts">
  import { onMount } from 'svelte';
  import { connect, authed } from './lib/ws';
  import './lib/theme'; // initializes isDark store from localStorage/system pref
  import Login from './lib/Login.svelte';
  import Canvas from './lib/canvas/Canvas.svelte';

  onMount(() => connect());
</script>

{#if $authed}
  <Canvas />
{:else}
  <Login />
{/if}
