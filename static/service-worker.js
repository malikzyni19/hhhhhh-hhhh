/* ─────────────────────────────────────────────────────────
   ZyNi SMC — Service Worker  v1.0.0
   Strategy:
     • Static PWA assets  → Cache-first (icons, offline page)
     • Static images      → Cache-first, network fallback
     • All HTML pages     → Network-first, offline fallback
     • /api/* endpoints   → Network only (never cached)
     • /admin/*           → Network only
   ───────────────────────────────────────────────────────── */

const CACHE_VERSION = 'zyni-smc-v2';
const OFFLINE_URL   = '/offline';

const PRECACHE_ASSETS = [
  OFFLINE_URL,
  '/static/icons/icon-192x192.png',
  '/static/icons/icon-512x512.png',
  '/static/images/z-logo.png',
  '/static/images/favicon.png',
];

/* ── Patterns that must NEVER be served from cache ── */
const NETWORK_ONLY = [
  /^\/api\//,
  /^\/admin\//,
  /^\/logout/,
  /^\/login/,
  /^\/verify/,
  /^\/reset/,
  /^\/forgot/,
];

/* ── Patterns for static image assets (cache-first) ── */
const STATIC_ASSET = [
  /^\/static\/icons\//,
  /^\/static\/images\//,
];

/* ─── Install — precache critical offline assets ─── */
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_VERSION).then(cache =>
      cache.addAll(PRECACHE_ASSETS)
    ).then(() => self.skipWaiting())
  );
});

/* ─── Activate — remove stale caches ─── */
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k !== CACHE_VERSION)
          .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

/* ─── Fetch ─── */
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  /* Only handle same-origin GET requests */
  if (request.method !== 'GET' || url.origin !== self.location.origin) {
    return;
  }

  const path = url.pathname;

  /* Network-only routes — never intercept */
  if (NETWORK_ONLY.some(p => p.test(path))) {
    return;
  }

  /* Static assets — cache-first */
  if (STATIC_ASSET.some(p => p.test(path))) {
    event.respondWith(cacheFirst(request));
    return;
  }

  /* Navigation requests (HTML pages) — network-first */
  if (request.mode === 'navigate') {
    event.respondWith(networkFirstWithOfflineFallback(request));
    return;
  }

  /* Everything else — network-first, no offline fallback */
  event.respondWith(networkFirst(request));
});

/* ─── Strategies ─── */

async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('Resource unavailable offline.', { status: 503 });
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    return cached || new Response('Unavailable offline.', { status: 503 });
  }
}

async function networkFirstWithOfflineFallback(request) {
  try {
    const response = await fetch(request);
    /* Cache successful navigations so we can serve them later */
    if (response.ok) {
      const cache = await caches.open(CACHE_VERSION);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    /* Last resort: branded offline page */
    const offline = await caches.match(OFFLINE_URL);
    return offline || new Response('<h1>You are offline</h1>', {
      status: 503,
      headers: { 'Content-Type': 'text/html' }
    });
  }
}

/* ─── Background sync — notify clients of available updates ─── */
self.addEventListener('message', event => {
  if (event.data === 'SKIP_WAITING') {
    self.skipWaiting();
  }
});
