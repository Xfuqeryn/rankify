require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
const fetch = require('node-fetch');
const cron = require('node-cron');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Auth ─────────────────────────────────────────────────────────────────────
const ADMIN_USER    = process.env.ADMIN_USER || 'Administrator';
const ADMIN_PASS    = process.env.ADMIN_PASS || 'BigKuk69!';
const SESSION_TOKEN = Buffer.from(`${ADMIN_USER}:${ADMIN_PASS}:rankify`).toString('base64');

app.use('/api', (req, res, next) => {
  if (req.path === '/login') return next();
  const auth = (req.headers['authorization'] || '').replace('Bearer ', '');
  if (auth === SESSION_TOKEN) return next();
  res.status(401).json({ error: 'Unauthorized' });
});

app.post('/api/login', (req, res) => {
  const { username, password } = req.body || {};
  if (username === ADMIN_USER && password === ADMIN_PASS) {
    res.json({ ok: true, token: SESSION_TOKEN });
  } else {
    res.status(401).json({ ok: false, error: 'Invalid credentials' });
  }
});

const isLocal =
  !process.env.DATABASE_URL ||
  process.env.DATABASE_URL.includes('localhost') ||
  process.env.DATABASE_URL.includes('127.0.0.1');

const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://localhost/rankify',
  ssl: isLocal ? false : { rejectUnauthorized: false },
});

// ─── Database init ──────────────────────────────────────────────────────────

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS playlists (
      id         SERIAL PRIMARY KEY,
      spotify_id TEXT UNIQUE NOT NULL,
      name       TEXT NOT NULL,
      owner      TEXT,
      followers  INT DEFAULT 0,
      image_url  TEXT,
      label      TEXT,
      notes      TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS keywords (
      id     SERIAL PRIMARY KEY,
      term   TEXT NOT NULL,
      market TEXT NOT NULL,
      UNIQUE (term, market)
    );

    CREATE TABLE IF NOT EXISTS rank_history (
      id          SERIAL PRIMARY KEY,
      playlist_id INT REFERENCES playlists(id) ON DELETE CASCADE,
      keyword_id  INT REFERENCES keywords(id) ON DELETE CASCADE,
      position    INT,
      followers   INT,
      checked_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS alerts (
      id          SERIAL PRIMARY KEY,
      playlist_id INT REFERENCES playlists(id) ON DELETE CASCADE,
      keyword_id  INT REFERENCES keywords(id) ON DELETE CASCADE,
      type        TEXT NOT NULL,
      message     TEXT NOT NULL,
      seen        BOOLEAN DEFAULT FALSE,
      created_at  TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS settings (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_rh_playlist  ON rank_history(playlist_id);
    CREATE INDEX IF NOT EXISTS idx_rh_keyword   ON rank_history(keyword_id);
    CREATE INDEX IF NOT EXISTS idx_rh_checked   ON rank_history(checked_at);
  `);

  // Safe migrations — idempotent on every start (works on Render + existing DBs)
  await pool.query(`
    ALTER TABLE playlists ADD COLUMN IF NOT EXISTS is_competitor   BOOLEAN DEFAULT FALSE;
    ALTER TABLE playlists ADD COLUMN IF NOT EXISTS genre           TEXT;
    ALTER TABLE playlists ADD COLUMN IF NOT EXISTS last_discovery  TIMESTAMPTZ;

    CREATE TABLE IF NOT EXISTS curator_snapshots (
      id            SERIAL PRIMARY KEY,
      keyword_id    INT  REFERENCES keywords(id) ON DELETE CASCADE,
      position      INT  NOT NULL,
      spotify_id    TEXT NOT NULL,
      playlist_name TEXT,
      owner         TEXT,
      followers     INT  DEFAULT 0,
      description   TEXT,
      contact_info  JSONB DEFAULT '{}',
      checked_at    TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_cs_keyword ON curator_snapshots(keyword_id);
    CREATE INDEX IF NOT EXISTS idx_cs_checked ON curator_snapshots(checked_at);
    CREATE INDEX IF NOT EXISTS idx_cs_owner   ON curator_snapshots(owner);
  `);

  // Session 4 migrations — idempotent
  await pool.query(`
    ALTER TABLE keywords          ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'manual';
    ALTER TABLE curator_snapshots ADD COLUMN IF NOT EXISTS genre  TEXT;
  `);

  // Session 5 — M&A Suite tables
  await pool.query(`
    CREATE TABLE IF NOT EXISTS target_genres (
      id         SERIAL PRIMARY KEY,
      name       TEXT UNIQUE NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS acquisition_crm (
      spotify_id         TEXT PRIMARY KEY,
      status             TEXT DEFAULT 'New',
      notes              TEXT DEFAULT '',
      snapshot_at        TIMESTAMPTZ DEFAULT NOW(),
      snapshot_followers INT,
      updated_at         TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  // Session 7 — Market Intelligence Terminal
  await pool.query(`
    CREATE TABLE IF NOT EXISTS keyword_intel (
      id             SERIAL PRIMARY KEY,
      term           TEXT    NOT NULL,
      market         TEXT    NOT NULL,
      search_volume  INT     DEFAULT 0,
      growth_pct     FLOAT   DEFAULT 0,
      competition    FLOAT   DEFAULT 1.0,
      traffic_score  INT     DEFAULT 0,
      total_followers BIGINT DEFAULT 0,
      top1_followers INT     DEFAULT 0,
      updated_at     TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(term, market)
    );

    CREATE INDEX IF NOT EXISTS idx_ki_term   ON keyword_intel(term);
    CREATE INDEX IF NOT EXISTS idx_ki_score  ON keyword_intel(traffic_score DESC);
  `);

  // Session 8 — Enterprise: monitor watchlist
  await pool.query(`
    CREATE TABLE IF NOT EXISTS monitor_watchlist (
      id         SERIAL PRIMARY KEY,
      spotify_id TEXT UNIQUE NOT NULL,
      name       TEXT,
      reason     TEXT,
      added_at   TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_mw_spotify ON monitor_watchlist(spotify_id);
  `);

  // Sessions 9/10 — Song Scout track cache
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scout_tracks (
      id                 SERIAL PRIMARY KEY,
      track_id           TEXT NOT NULL,
      track_name         TEXT,
      artist_name        TEXT,
      artist_id          TEXT,
      album_art          TEXT,
      popularity         INTEGER DEFAULT 0,
      release_date       TEXT,
      duration_ms        INTEGER,
      playlist_id        TEXT,
      playlist_name      TEXT,
      playlist_followers INTEGER DEFAULT 0,
      market             TEXT DEFAULT 'US',
      genre              TEXT,
      stickiness_score   INTEGER DEFAULT 0,
      is_t1_trending     BOOLEAN DEFAULT FALSE,
      spotify_url        TEXT,
      discovered_at      TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(track_id, playlist_id)
    );
    CREATE INDEX IF NOT EXISTS idx_st_score  ON scout_tracks(stickiness_score DESC);
    CREATE INDEX IF NOT EXISTS idx_st_market ON scout_tracks(market);
    CREATE INDEX IF NOT EXISTS idx_st_disc   ON scout_tracks(discovered_at DESC);
  `);

  // Seed default target genres if table is empty
  await pool.query(`
    INSERT INTO target_genres (name) VALUES
      ('lo-fi'), ('ambient'), ('sleep'), ('study'), ('focus'),
      ('chill'), ('house'), ('deep house'), ('tech house'),
      ('indie pop'), ('hip hop'), ('r&b'), ('jazz'), ('pop')
    ON CONFLICT (name) DO NOTHING
  `);

  console.log('Database initialized');
}

// ─── Spotify auth ────────────────────────────────────────────────────────────

let spotifyToken = null;
let tokenExpiry = 0;

async function getToken() {
  if (spotifyToken && Date.now() < tokenExpiry) return spotifyToken;
  const creds = Buffer.from(
    `${process.env.SPOTIFY_CLIENT_ID}:${process.env.SPOTIFY_CLIENT_SECRET}`
  ).toString('base64');
  const res = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      Authorization: `Basic ${creds}`,
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: 'grant_type=client_credentials',
  });
  const data = await res.json();
  if (!data.access_token) throw new Error('Spotify auth failed: ' + JSON.stringify(data));
  spotifyToken = data.access_token;
  tokenExpiry = Date.now() + (data.expires_in - 60) * 1000;
  return spotifyToken;
}

async function spotifyGet(urlPath) {
  const token = await getToken();
  const res = await fetch(`https://api.spotify.com/v1${urlPath}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) throw new Error(`Spotify ${res.status}: ${urlPath}`);
  return res.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ─── Valuation 3.0 — 2026 Royalty Tiers ──────────────────────────────────────
// Tier 1 ($0.0035+): premium English-speaking + Nordic + core EU markets
const TIER1 = ['US','GB','UK','DK','IS','NO','MC','FI','CH','IE','LI','SE','NZ','LU','AD','NL','AU','AT','DE','FR','BE'];
// Tier 2 ($0.0018–$0.0035): secondary developed markets
const TIER2 = ['CA','CY','IL','HK','EE','MT','SG','AE','ES','CZ','IT','LT','GR','HU','RO','SK','UY','PT','BR','MX'];
// Tier 3 (<$0.0018): emerging/growth markets
const TIER3 = ['ZA','PL','SA','TW','MY','TH','PE','CL','UA','PH','MA','PY','ID','TR','AR','VN'];
const ALL_MARKETS = [...new Set([...TIER1, ...TIER2, ...TIER3])];

// Per-stream royalty rate midpoints for revenue calculation
const ROYALTY_RATE = (market) => {
  if (TIER1.includes(market)) return 0.0038;  // T1 avg
  if (TIER2.includes(market)) return 0.0026;  // T2 avg
  return 0.0015;                               // T3 avg
};
const TIER_MULT = (market) =>
  TIER1.includes(market) ? 1.3 : TIER2.includes(market) ? 1.1 : 0.9;

// Valuation 3.2 — Passive Genre LTV detection
const PASSIVE_GENRES = new Set([
  'sleep','lo-fi','lofi','lo fi','ambient','study','focus','relax','relaxing',
  'meditation','chill','nature','white noise','rain','piano','classical','deep sleep',
  'binaural','spa','yoga','mindfulness','acoustic chill','peaceful','calming',
]);
function isPassiveGenre(genre) {
  if (!genre) return false;
  const g = genre.toLowerCase();
  for (const pg of PASSIVE_GENRES) { if (g.includes(pg)) return true; }
  return false;
}

// ─── Sync state (shared across requests) ─────────────────────────────────────
const syncState = {
  running:   false,
  current:   '',
  progress:  0,
  total:     0,
  startedAt: null,
};

// ─── Genre detection ─────────────────────────────────────────────────────────

async function detectGenre(spotifyId) {
  try {
    // Grab first 5 tracks — only need artist IDs
    const tracksData = await spotifyGet(
      `/playlists/${spotifyId}/tracks?limit=5&fields=items(track(artists(id)))`
    );
    const artistIds = [
      ...new Set(
        (tracksData?.items || [])
          .flatMap((it) => (it?.track?.artists || []).map((a) => a.id))
          .filter(Boolean)
      ),
    ].slice(0, 5);

    if (!artistIds.length) return null;

    await sleep(350);
    const artistsData = await spotifyGet(`/artists?ids=${artistIds.join(',')}`);
    const allGenres = (artistsData?.artists || []).flatMap((a) => a?.genres || []);
    if (!allGenres.length) return null;

    // Return the most-frequently appearing genre
    const freq = {};
    for (const g of allGenres) freq[g] = (freq[g] || 0) + 1;
    return Object.entries(freq).sort((a, b) => b[1] - a[1])[0][0];
  } catch (e) {
    console.error(`[genre] ${spotifyId}:`, e.message);
    return null;
  }
}

// ─── Localized keyword generator (for Master Sync) ───────────────────────────

function getLocalizedTerms(genre, market) {
  const g    = (genre || 'music').toLowerCase().trim();
  const year = new Date().getFullYear();

  const base = [
    `${g} playlist`, `${g} mix`, `best ${g}`, `top ${g}`,
    `new ${g}`, `${g} hits`, `${g} vibes`, `${g} essentials`,
    `${g} ${year}`, `${g} radio`, `${g} new music`,
    `chill ${g}`, `${g} songs`, `${g} classics`,
    `best ${g} playlist`, `top ${g} songs ${year}`,
  ];

  const byMarket = {
    DE: [`${g} playlist deutsch`, `${g} musik`, `beste ${g} songs`,
         `neue ${g} musik`, `${g} hits deutsch`],
    FR: [`playlist ${g} français`, `musique ${g}`, `meilleure playlist ${g}`,
         `nouveau ${g}`, `${g} hits français`],
    NL: [`${g} playlist nederland`, `${g} muziek`, `beste ${g} playlist`,
         `nieuwe ${g} muziek`, `${g} hits nederland`],
    BR: [`playlist ${g} brasil`, `música ${g}`, `melhor playlist ${g}`,
         `${g} brasileiro`, `${g} hits brasil`],
    TR: [`${g} playlist türkçe`, `${g} müzik`, `en iyi ${g} playlist`,
         `yeni ${g} müzik`, `${g} türkçe hits`],
    ID: [`playlist ${g} indonesia`, `musik ${g}`, `${g} terbaik indonesia`,
         `${g} terbaru`, `lagu ${g} terpopuler`],
    PH: [`${g} playlist philippines`, `${g} opm`, `best ${g} ph`,
         `${g} music ph ${year}`, `pinoy ${g} playlist`],
    VN: [`nhạc ${g}`, `playlist ${g} việt`, `${g} việt nam`,
         `nhạc ${g} hay nhất`, `bài hát ${g}`],
  };

  const localized = byMarket[market] || [];
  return [...new Set([...base, ...localized])].slice(0, 50);
}

// ─── SEO keyword discovery ───────────────────────────────────────────────────

const SEO_STOP_WORDS = new Set([
  'the','and','for','with','from','this','that','are','was','not','but',
  'have','had','its','you','your','can','all','just','will','been','her',
  'his','our','their','they','them','has','who','what','which','how',
]);

function discoverKeywords(playlist) {
  const title   = (playlist.name  || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ');
  const genre   = (playlist.genre || '').toLowerCase().trim();

  const titleWords = title.split(/\s+/).filter(w => w.length > 2 && !SEO_STOP_WORDS.has(w));
  const genreCore  = genre
    ? [genre, ...genre.split(/[\s,]+/).filter(w => w.length > 3)]
    : [];

  const moods      = ['chill','happy','sad','dark','melancholy','upbeat','mellow','intense',
                      'peaceful','euphoric','relaxing','energetic','dreamy','nostalgic','romantic'];
  const activities = ['workout','study','sleep','focus','meditation','running','yoga','party',
                      'driving','cooking','coffee','morning','night','gym','work from home'];
  const contexts   = ['playlist','mix','hits','vibes','music','songs','radio','essentials'];
  const prefixes   = ['best','top','new','latest','ultimate','greatest','perfect'];
  const years      = [String(new Date().getFullYear()), String(new Date().getFullYear() - 1)];

  const terms = new Set();

  // Genre-based combinations
  for (const g of genreCore) {
    if (!g || g.length < 3) continue;
    for (const ctx of contexts)         terms.add(`${g} ${ctx}`);
    for (const yr  of years)            terms.add(`${g} ${yr}`);
    for (const pre of prefixes)         terms.add(`${pre} ${g}`);
    for (const act of activities.slice(0, 7)) terms.add(`${g} ${act}`);
    for (const m   of moods.slice(0, 7))      terms.add(`${g} ${m}`);
    terms.add(`${g} playlist ${years[0]}`);
    terms.add(`${g} new music`);
    terms.add(`new ${g} music`);
  }

  // Title-word combinations
  for (const w of titleWords.slice(0, 5)) {
    terms.add(`${w} playlist`);
    terms.add(`${w} music`);
    terms.add(`${w} mix`);
    terms.add(`${w} vibes`);
    if (genre) terms.add(`${w} ${genre}`);
  }

  // Universal mood/activity playlists
  for (const m   of moods)      terms.add(`${m} playlist`);
  for (const act of activities)  terms.add(`${act} playlist`);
  for (const act of activities)  terms.add(`${act} music`);

  // Cross-combos: mood × activity
  for (const m of moods.slice(0, 4)) {
    for (const act of activities.slice(0, 5)) {
      terms.add(`${m} ${act} music`);
    }
  }

  return [...terms]
    .map(t => t.trim().replace(/\s+/g, ' '))
    .filter(t => t.length > 4 && t.split(' ').length >= 2)
    .slice(0, 75);
}

// ─── Contact extraction ──────────────────────────────────────────────────────

function extractContacts(description) {
  if (!description) return {};
  const str = String(description);
  const result = {};

  const emails     = [...new Set(str.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g) || [])];
  const instagrams = [...new Set(
    (str.match(/(?:instagram\.com\/|(?<![a-zA-Z0-9])@)([a-zA-Z0-9_.]{2,30})/gi) || [])
      .map(m => '@' + m.replace(/.*[@\/]/, ''))
      .filter(h => h.length > 2 && !h.includes('.'))
  )];
  const discords   = [...new Set(str.match(/discord(?:\.gg|app\.com\/invite)\/[a-zA-Z0-9]+/gi) || [])];
  const linktrees  = [...new Set(str.match(/linktr\.ee\/[a-zA-Z0-9_.\-]+/gi) || [])];
  const submithubs = [...new Set(str.match(/submithub\.com\/[a-zA-Z0-9_.\-]+/gi) || [])];

  if (emails.length)     result.emails     = emails.slice(0, 5);
  if (instagrams.length) result.instagrams = instagrams.slice(0, 5);
  if (discords.length)   result.discords   = discords.slice(0, 3);
  if (linktrees.length)  result.linktrees  = linktrees.slice(0, 3);
  if (submithubs.length) result.submithubs = submithubs.slice(0, 3);

  return result;
}

// ─── Seasonal suffixes ───────────────────────────────────────────────────────

function getSeasonalSuffixes() {
  const now    = new Date();
  const month  = now.getMonth() + 1; // 1–12
  const year   = now.getFullYear();
  const mName  = ['january','february','march','april','may','june',
                  'july','august','september','october','november','december'][month - 1];
  const season =
    month >= 3 && month <= 5  ? 'spring' :
    month >= 6 && month <= 8  ? 'summer' :
    month >= 9 && month <= 11 ? 'autumn' : 'winter';

  const s = [
    `${year}`,
    `${year} playlist`,
    season,
    `${season} ${year}`,
    `${season} playlist`,
    `${season} mix`,
    `${season} vibes`,
    mName,
    `${mName} ${year}`,
    `${mName} playlist`,
  ];

  if (month === 4 || month === 5)  s.push('coachella','festival season','festival mix','spring festival');
  if (month === 6)                  s.push('pride','pride month','pride playlist');
  if (month === 6 || month === 7)  s.push('glastonbury','summer festival','bbq vibes');
  if (month === 7 || month === 8)  s.push('beach vibes','beach playlist','road trip summer');
  if (month === 10)                 s.push('halloween playlist','spooky mix','halloween mix');
  if (month === 11)                 s.push('thanksgiving','fall harvest');
  if (month === 12)                 s.push('christmas playlist','holiday mix',`best of ${year}`);
  if (month === 1)                  s.push('new year',`new year ${year}`,'fresh start playlist');

  return [...new Set(s)];
}

// ─── Master Sync Engine ───────────────────────────────────────────────────────

async function runCrawl() {
  if (syncState.running) {
    console.log('[sync] already running — skipping duplicate trigger');
    return;
  }

  syncState.running   = true;
  syncState.startedAt = new Date().toISOString();
  syncState.current   = 'Starting…';
  syncState.progress  = 0;
  syncState.total     = 0;

  // Declared outside try so finally can always clearInterval
  let heartbeat;

  console.log('[sync] Master Sync starting…');

  try {
    const { rows: playlists } = await pool.query('SELECT * FROM playlists');

    if (!playlists.length) {
      console.log('[sync] no playlists — aborting');
      return;
    }

    // ── 1. Genre detection for all playlists ────────────────────────────────
    syncState.current = 'Detecting genres…';
    for (const playlist of playlists) {
      if (!playlist.genre) {
        const genre = await detectGenre(playlist.spotify_id);
        if (genre) {
          await pool.query('UPDATE playlists SET genre=$1 WHERE id=$2', [genre, playlist.id]);
          playlist.genre = genre;
          console.log(`[genre] ${playlist.name} → ${genre}`);
        }
        await sleep(350);
      }
    }

    // ── 2. Build Master Sync keyword plan (genre × all markets × 50 terms) ──
    const ownedPlaylists = playlists.filter(p => !p.is_competitor);

    // Merge: user-managed target genres + auto-detected playlist genres
    const { rows: targetGenreRows } = await pool.query('SELECT name FROM target_genres ORDER BY name');
    const targetGenres   = targetGenreRows.map(r => r.name);
    const detectedGenres = ownedPlaylists.map(p => p.genre).filter(Boolean);
    const allGenres      = [...new Set([...targetGenres, ...detectedGenres])];
    if (!allGenres.length) allGenres.push('music'); // last-resort fallback

    syncState.current = 'Planning global keyword matrix…';

    // genre × market combos → localized term lists
    const masterPlan = []; // [{ genre, market, terms[] }]
    for (const genre of allGenres) {
      for (const market of ALL_MARKETS) {
        const terms = getLocalizedTerms(genre, market); // up to 50 terms
        masterPlan.push({ genre, market, terms });
      }
    }

    // Insert master_sync keywords (idempotent — ON CONFLICT DO NOTHING)
    for (const { genre, market, terms } of masterPlan) {
      for (const term of terms) {
        await pool.query(
          `INSERT INTO keywords (term, market, source) VALUES ($1,$2,'master_sync')
           ON CONFLICT (term, market) DO NOTHING`,
          [term.toLowerCase(), market]
        );
      }
    }

    // Build a quick lookup: term+market → genre (for snapshot tagging)
    const termGenreMap = {};
    for (const { genre, market, terms } of masterPlan) {
      for (const t of terms) termGenreMap[`${t.toLowerCase()}|${market}`] = genre;
    }

    // ── 3. Autonomous keyword discovery for owned playlists (once per 7d) ───
    const DISCOVERY_TTL = 7 * 24 * 3600 * 1000;

    for (const playlist of ownedPlaylists) {
      const lastDisc = playlist.last_discovery ? new Date(playlist.last_discovery).getTime() : 0;
      if (Date.now() - lastDisc < DISCOVERY_TTL) continue;

      const candidates = discoverKeywords(playlist);
      console.log(`[discovery] ${playlist.name}: scanning ${candidates.length} candidates`);

      for (const term of candidates) {
        const { rows: existing } = await pool.query(
          'SELECT id FROM keywords WHERE term=$1 AND market=$2',
          [term.toLowerCase(), 'US']
        );
        if (existing.length) continue;

        await sleep(350);
        try {
          const sr    = await spotifyGet(
            `/search?q=${encodeURIComponent(term)}&type=playlist&market=US&limit=50`
          );
          const items = (sr?.playlists?.items || []).filter(Boolean);
          const found = items.some(it => ownedPlaylists.some(p => p.spotify_id === it.id));
          if (found) {
            await pool.query(
              `INSERT INTO keywords (term, market, source) VALUES ($1,'US','manual')
               ON CONFLICT (term, market) DO NOTHING`,
              [term.toLowerCase()]
            );
            console.log(`[discovery] auto-tracked: "${term}"`);
          }
        } catch (e) {
          console.error(`[discovery] "${term}":`, e.message);
        }
      }

      await pool.query(
        'UPDATE playlists SET last_discovery=$1 WHERE id=$2',
        [new Date().toISOString(), playlist.id]
      );
    }

    // Refresh full keyword list after mutations
    const { rows: allKeywords } = await pool.query('SELECT * FROM keywords');
    if (!allKeywords.length) {
      console.log('[sync] no keywords to crawl');
      return;
    }

    // ── 4. Pre-fetch follower counts once per tracked playlist ───────────────
    syncState.current = 'Pre-fetching follower counts…';
    const followerMap = {};
    for (const playlist of playlists) {
      await sleep(350);
      try {
        const pl = await spotifyGet(`/playlists/${playlist.spotify_id}?fields=followers.total`);
        followerMap[playlist.id] = pl?.followers?.total ?? playlist.followers;
      } catch (e) {
        followerMap[playlist.id] = playlist.followers;
        console.error(`[sync] followers ${playlist.spotify_id}:`, e.message);
      }
    }

    // ── 5. Master Crawl Loop ─────────────────────────────────────────────────
    const byMarket = {};
    for (const kw of allKeywords) {
      (byMarket[kw.market] = byMarket[kw.market] || []).push(kw);
    }

    syncState.total    = allKeywords.length;
    syncState.progress = 0;

    const notionRows = [];
    // ── Metadata cache: spotify_id → { followers, description, contacts }
    const metaCache = new Map();

    // ── Load Telegram settings once before crawl loop ──
    const tgSettings = await getTelegramSettings();

    // ── Heartbeat: log every 30s so Render logs confirm the process is alive ──
    heartbeat = setInterval(() => {
      console.log(
        `[sync] ♥ heartbeat — ${syncState.progress}/${syncState.total} keywords done | now: ${syncState.current}`
      );
    }, 30000);

    for (const [market, kws] of Object.entries(byMarket)) {
      for (const kw of kws) {
        syncState.progress++;
        const kwGenre = termGenreMap[`${kw.term}|${kw.market}`] || null;
        syncState.current = `${kwGenre || '—'} · ${market} · "${kw.term}"`;

        // Single call of 50 (Tier 1-3 coverage without double API spend)
        await sleep(350);
        let items = [];
        try {
          const sr = await spotifyGet(
            `/search?q=${encodeURIComponent(kw.term)}&type=playlist&market=${market}&limit=50`
          );
          items = (sr?.playlists?.items || []).filter(Boolean);
        } catch (e) {
          console.error(`[sync] search "${kw.term}"/${market}:`, e.message);
        }

        // Refresh curator snapshots for this keyword
        await pool.query('DELETE FROM curator_snapshots WHERE keyword_id=$1', [kw.id]);

        for (let i = 0; i < Math.min(20, items.length); i++) {
          const it = items[i];
          if (!it) continue;

          let followers   = 0;
          let description = (it.description || '').slice(0, 1000);
          let contacts    = {};

          const ownerName  = (it.owner?.display_name || '').toLowerCase();
          const isSpotify  = ownerName === 'spotify' || ownerName.startsWith('spotify ');

          if (metaCache.has(it.id)) {
            // ── Cache hit: zero API calls ──────────────────────────────────────
            ({ followers, description, contacts } = metaCache.get(it.id));
          } else if (i < 10 && !isSpotify) {
            // ── Deep Scrape: only top-10, only non-Spotify curators ────────────
            await sleep(350);
            try {
              const full  = await spotifyGet(`/playlists/${it.id}?fields=followers.total,description`);
              followers   = full?.followers?.total || 0;
              description = (full?.description || description).slice(0, 1000);
              // Only extract contacts when the playlist has meaningful reach (>100 followers)
              contacts    = followers > 100 ? extractContacts(description) : {};
            } catch (e) {
              console.error(`[sync] detail ${it.id}:`, e.message);
            }
            metaCache.set(it.id, { followers, description, contacts });
          }

          try {
            await pool.query(`
              INSERT INTO curator_snapshots
                (keyword_id, position, spotify_id, playlist_name, owner,
                 followers, description, contact_info, genre)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            `, [
              kw.id, i + 1, it.id,
              (it.name || '').slice(0, 500),
              it.owner?.display_name || null,
              followers,
              description,
              JSON.stringify(contacts),
              kwGenre,
            ]);
          } catch (e) {
            console.error('[sync] snapshot insert:', e.message);
          }
        }

        // Record rank_history for every tracked playlist
        for (const playlist of playlists) {
          const idx       = items.findIndex(it => it.id === playlist.spotify_id);
          const position  = idx === -1 ? null : idx + 1;
          const followers = followerMap[playlist.id] ?? playlist.followers;

          const { rows: prevRows } = await pool.query(
            `SELECT position FROM rank_history
             WHERE playlist_id=$1 AND keyword_id=$2
             ORDER BY checked_at DESC LIMIT 1`,
            [playlist.id, kw.id]
          );
          const prevPos = prevRows[0]?.position ?? null;

          await pool.query(
            `INSERT INTO rank_history (playlist_id, keyword_id, position, followers)
             VALUES ($1,$2,$3,$4)`,
            [playlist.id, kw.id, position, followers]
          );

          if (position !== null && prevPos !== null) {
            if (position <= 3 && prevPos > 3) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message) VALUES ($1,$2,'top3',$3)`,
                [playlist.id, kw.id, `Entered top 3! Now ranked #${position}`]
              );
            }
            const diff = prevPos - position;
            if (diff >= 5) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message) VALUES ($1,$2,'rise',$3)`,
                [playlist.id, kw.id, `Jumped ${diff} spots to #${position}`]
              );
            } else if (diff <= -5) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message) VALUES ($1,$2,'drop',$3)`,
                [playlist.id, kw.id, `Dropped ${Math.abs(diff)} spots to #${position}`]
              );
            }

            // ── Telegram: Top-15 entry alert ──────────────────────────────
            const tier = TIER1.includes(kw.market) ? 'T1' : TIER2.includes(kw.market) ? 'T2' : TIER3.includes(kw.market) ? 'T3' : null;
            if (
              tier &&
              tgSettings.chatId &&
              !tgSettings.excludedCountries.has(kw.market) &&
              position <= 15 &&
              (prevPos === null || prevPos > 15)
            ) {
              const tMsg = [
                `🎯 <b>Top 15 Entry</b>`,
                ``,
                `📋 <b>${playlist.name}</b>`,
                `🔑 Keyword: <i>${kw.term}</i>`,
                `🌍 Market: ${kw.market} (${tier})`,
                `📊 Position: #${position}${prevPos ? ` (was #${prevPos})` : ''}`,
                `👥 Followers: ${(followerMap[playlist.id] || 0).toLocaleString()}`,
              ].join('\n');
              sendTelegramAlert(tgSettings.chatId, tMsg).catch(() => {});
            }
          }

          notionRows.push({ playlist_name: playlist.name, term: kw.term, market: kw.market, position, followers });
        }
      }
    }

    // ── 6. Auto-seed acquisition_crm for new undervalued gems ───────────────
    syncState.current = 'Seeding acquisition CRM…';
    try {
      await pool.query(`
        INSERT INTO acquisition_crm (spotify_id, snapshot_followers, snapshot_at)
        SELECT DISTINCT ON (cs.spotify_id)
          cs.spotify_id,
          cs.followers,
          NOW()
        FROM curator_snapshots cs
        WHERE cs.position BETWEEN 1 AND 10
          AND cs.followers > 0
          AND cs.followers < 5000
          AND cs.spotify_id NOT IN (SELECT spotify_id FROM playlists)
        ORDER BY cs.spotify_id, cs.position ASC
        ON CONFLICT (spotify_id) DO NOTHING
      `);
    } catch (e) {
      console.error('[sync] crm seed error:', e.message);
    }

    // ── 7. Finalise ──────────────────────────────────────────────────────────
    syncState.current = 'Finalizing…';

    await pool.query(`
      UPDATE playlists p
      SET followers = rh.followers
      FROM (
        SELECT DISTINCT ON (playlist_id) playlist_id, followers
        FROM rank_history
        ORDER BY playlist_id, checked_at DESC
      ) rh
      WHERE p.id = rh.playlist_id
    `);

    await pool.query(
      `INSERT INTO settings (key, value) VALUES ('last_crawl', $1)
       ON CONFLICT (key) DO UPDATE SET value = $1`,
      [new Date().toISOString()]
    );

    if (process.env.NOTION_TOKEN && process.env.NOTION_DATABASE_ID) {
      syncToNotion(notionRows).catch(e => console.error('[notion]', e.message));
    }

    // ── 8. Keyword Intel enrichment (background — doesn't block sync done) ──
    syncState.current = 'Enriching keyword intelligence…';
    const { rows: apikeyRow } = await pool.query(
      `SELECT value FROM settings WHERE key='playlistranking_api_key'`
    ).catch(() => ({ rows: [] }));
    const prApiKey = apikeyRow[0]?.value || null;

    // Only enrich manual + recently-used keywords to stay within API rate limits
    const { rows: enrichTargets } = await pool.query(`
      SELECT DISTINCT k.term, k.market
      FROM keywords k
      WHERE k.source != 'master_sync' OR k.id IN (
        SELECT DISTINCT keyword_id FROM rank_history
        WHERE checked_at > NOW() - INTERVAL '24 hours'
        LIMIT 100
      )
      LIMIT 200
    `).catch(() => ({ rows: [] }));

    for (const { term, market } of enrichTargets) {
      try {
        await sleep(150);
        const intel = await enrichKeywordIntel(term, market, prApiKey);
        await pool.query(`
          INSERT INTO keyword_intel (term, market, search_volume, growth_pct, competition, traffic_score, total_followers, top1_followers, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
          ON CONFLICT (term, market) DO UPDATE
            SET search_volume  = EXCLUDED.search_volume,
                growth_pct     = EXCLUDED.growth_pct,
                competition    = EXCLUDED.competition,
                traffic_score  = EXCLUDED.traffic_score,
                total_followers = EXCLUDED.total_followers,
                top1_followers = EXCLUDED.top1_followers,
                updated_at     = NOW()
        `, [term, market, intel.searchVolume, intel.growthPct, intel.competition, intel.trafficScore, intel.totalFollowers, intel.top1Followers]);
      } catch (e) {
        console.error(`[intel] enrich ${term}/${market}:`, e.message);
      }
    }

    console.log(`[sync] Master Sync complete — ${allKeywords.length} keywords across ${Object.keys(byMarket).length} markets`);
  } catch (e) {
    console.error('[sync] error:', e);
  } finally {
    clearInterval(heartbeat);
    syncState.running = false;
    syncState.current = 'Done';
  }
}

// ─── Notion sync ─────────────────────────────────────────────────────────────

async function syncToNotion(rows) {
  const token = process.env.NOTION_TOKEN;
  const dbId = process.env.NOTION_DATABASE_ID;
  const headers = {
    Authorization: `Bearer ${token}`,
    'Notion-Version': '2022-06-28',
    'Content-Type': 'application/json',
  };

  for (const row of rows) {
    await sleep(350);
    const { playlist_name, term, market, position, followers } = row;
    const title = `${playlist_name} — ${term} (${market})`;
    const status =
      position === null
        ? 'Unranked'
        : position <= 3
        ? 'Top 3'
        : position <= 10
        ? 'Top 10'
        : 'Ranked';

    const properties = {
      Name: { title: [{ text: { content: title } }] },
      Playlist: { rich_text: [{ text: { content: playlist_name } }] },
      Keyword: { rich_text: [{ text: { content: term } }] },
      Market: { select: { name: market } },
      Position: { number: position ?? 0 },
      Followers: { number: followers ?? 0 },
      Status: { select: { name: status } },
      'Last Updated': { date: { start: new Date().toISOString() } },
    };

    try {
      const searchRes = await fetch(`https://api.notion.com/v1/databases/${dbId}/query`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          filter: {
            and: [
              { property: 'Playlist', rich_text: { equals: playlist_name } },
              { property: 'Keyword', rich_text: { equals: term } },
              { property: 'Market', select: { equals: market } },
            ],
          },
        }),
      });
      const searchData = await searchRes.json();
      const existing = searchData.results?.[0];

      if (existing) {
        await fetch(`https://api.notion.com/v1/pages/${existing.id}`, {
          method: 'PATCH',
          headers,
          body: JSON.stringify({ properties }),
        });
      } else {
        await fetch('https://api.notion.com/v1/pages', {
          method: 'POST',
          headers,
          body: JSON.stringify({ parent: { database_id: dbId }, properties }),
        });
      }
    } catch (e) {
      console.error('[notion] row error:', e.message);
    }
  }
}

// ─── Telegram Alerts ─────────────────────────────────────────────────────────

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '8710985939:AAGM8ocBOQ3VmuIBXlMjNYr7iP5MgPG0gVE';

async function sendTelegramAlert(chatId, message) {
  if (!chatId) return;
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
    const res = await fetch(url, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text: message, parse_mode: 'HTML' }),
    });
    const data = await res.json();
    if (!data.ok) console.error('[telegram] send failed:', data.description);
    return data;
  } catch (e) {
    console.error('[telegram] error:', e.message);
  }
}

async function getTelegramSettings() {
  try {
    const { rows } = await pool.query(
      `SELECT key, value FROM settings WHERE key IN ('telegram_chat_id', 'telegram_excluded_countries')`
    );
    const out = {};
    for (const r of rows) out[r.key] = r.value;
    return {
      chatId:           out.telegram_chat_id || null,
      excludedCountries: new Set((out.telegram_excluded_countries || '').split(',').map(s => s.trim().toUpperCase()).filter(Boolean)),
    };
  } catch { return { chatId: null, excludedCountries: new Set() }; }
}

// ─── PlaylistRankings API Enrichment ─────────────────────────────────────────

async function enrichKeywordIntel(term, market, apiKey) {
  // Synthetic baseline derived from curator_snapshots (always available)
  let searchVolume = 0;
  let growthPct    = 0;
  let competition  = 1.0;
  let totalFollowers = 0;
  let top1Followers  = 0;

  try {
    // Compute from our own data first
    const { rows: snaps } = await pool.query(`
      SELECT cs.followers, cs.position,
        k.market
      FROM curator_snapshots cs
      JOIN keywords k ON k.id = cs.keyword_id
      WHERE LOWER(k.term) = LOWER($1) AND k.market = $2
        AND cs.position <= 20
      ORDER BY cs.position ASC
    `, [term, market]);

    if (snaps.length) {
      totalFollowers = snaps.reduce((s, r) => s + (Number(r.followers) || 0), 0);
      top1Followers  = Number(snaps.find(r => r.position === 1)?.followers) || 0;
      // Synthetic search volume: geometric mean of total followers / 10
      searchVolume   = Math.round(Math.sqrt(totalFollowers / Math.max(snaps.length, 1)) * 2.5);
      // Competition density: fraction of top-10 playlists with >50k followers
      const heavyHitters = snaps.filter(r => r.position <= 10 && (r.followers || 0) > 50000).length;
      competition = Math.max(0.1, heavyHitters / Math.max(snaps.filter(r => r.position <= 10).length, 1));
    }

    // Growth % from rank_history for this keyword
    const { rows: hist } = await pool.query(`
      SELECT
        MAX(followers) AS peak,
        MIN(followers) AS floor,
        COUNT(DISTINCT DATE(checked_at)) AS days
      FROM rank_history rh
      JOIN keywords k ON k.id = rh.keyword_id
      WHERE LOWER(k.term) = LOWER($1) AND k.market = $2
        AND rh.position IS NOT NULL
        AND rh.checked_at > NOW() - INTERVAL '30 days'
    `, [term, market]);
    if (hist[0] && hist[0].floor > 0 && hist[0].peak > hist[0].floor) {
      growthPct = Math.round(((hist[0].peak - hist[0].floor) / hist[0].floor) * 100);
    }
  } catch (e) {
    console.error(`[intel] baseline ${term}/${market}:`, e.message);
  }

  // Try PlaylistRankings API if key provided
  if (apiKey) {
    try {
      const apiRes = await fetch(
        `https://api.playlistranking.com/v1/keywords?q=${encodeURIComponent(term)}&market=${market}`,
        { headers: { Authorization: `Bearer ${apiKey}` }, timeout: 8000 }
      );
      if (apiRes.ok) {
        const apiData = await apiRes.json();
        // Merge API data — expect { search_volume, growth_pct, competition }
        if (apiData.search_volume != null) searchVolume = apiData.search_volume;
        if (apiData.growth_pct    != null) growthPct    = apiData.growth_pct;
        if (apiData.competition   != null) competition  = apiData.competition;
      }
    } catch (e) {
      // API unreachable or wrong key — silently fall back to synthetic data
    }
  }

  // Traffic Score (1–100): high volume + high growth + low competition = top score
  const rawScore = (searchVolume * (1 + Math.max(growthPct, 0) / 100)) / Math.max(competition, 0.1);
  const trafficScore = Math.min(100, Math.max(1, Math.round(rawScore / 50)));

  return { searchVolume, growthPct, competition, trafficScore, totalFollowers, top1Followers };
}

// ─── Valuation 3.0 — 5-Year ROI with 2026 royalty rates ─────────────────────

// Valuation 3.2 — 2026 Royalty Rates + Revenue Premium + LTV Bonus
function calcValuation3(followers, position, market, genre = '') {
  const f    = Number(followers) || 0;
  const p    = Number(position)  || 10;
  const rate  = ROYALTY_RATE(market);
  const tMult = TIER_MULT(market);
  const rMult = p <= 1 ? 1.6 : p <= 3 ? 1.3 : p <= 5 ? 1.1 : 1.0;
  // Revenue Premium: 1.4× for T1/T2 playlists ranked top-3 (highly liquid assets)
  const inPremiumTier = TIER1.includes(market) || TIER2.includes(market);
  const revPremium    = inPremiumTier && p <= 3 ? 1.4 : 1.0;
  // LTV Bonus: 1.25× for passive genres (longer listener retention → higher lifetime value)
  const ltvBonus = isPassiveGenre(genre) ? 1.25 : 1.0;
  const estMonthlyRevenue = f * 0.15 * rate;
  const base = Math.round(estMonthlyRevenue * 12 * 5 * tMult * rMult * revPremium * ltvBonus);
  return {
    low:  Math.round(base * 0.75),
    mid:  base,
    high: Math.round(base * 1.25),
    est_monthly_revenue: Math.round(estMonthlyRevenue * 100) / 100,
    royalty_rate: rate,
    revenue_premium: revPremium > 1,
    ltv_bonus: ltvBonus > 1,
  };
}
const calcValuation2 = calcValuation3; // backwards-compat alias

/// daily 17:00 UTC = 18:00 GMT+1 — skip if already ran in last 24h
cron.schedule('0 17 * * *', async () => {
  try {
    const { rows } = await pool.query(`SELECT value FROM settings WHERE key='last_crawl'`);
    const lastCrawl = rows[0]?.value ? new Date(rows[0].value) : null;
    if (lastCrawl && (Date.now() - lastCrawl.getTime()) < 24 * 60 * 60 * 1000) {
      console.log('[cron] last sync was <24h ago — skipping auto crawl');
      return;
    }
    runCrawl().catch(console.error);
  } catch(e) {
    console.error('[cron] schedule check failed:', e.message);
    runCrawl().catch(console.error);
  }
}, { timezone: 'UTC' });

// ─── API routes ──────────────────────────────────────────────────────────────

app.get('/api/stats', async (req, res) => {
  try {
    const [pl, kw, dp, t3, lc, ua] = await Promise.all([
      pool.query('SELECT COUNT(*) FROM playlists'),
      pool.query('SELECT COUNT(*) FROM keywords'),
      pool.query('SELECT COUNT(*) FROM rank_history'),
      pool.query(`
        SELECT COUNT(*) FROM (
          SELECT DISTINCT ON (playlist_id, keyword_id) playlist_id, keyword_id, position
          FROM rank_history ORDER BY playlist_id, keyword_id, checked_at DESC
        ) t WHERE position <= 3
      `),
      pool.query(`SELECT value FROM settings WHERE key='last_crawl'`),
      pool.query(`SELECT COUNT(*) FROM alerts WHERE seen=false`),
    ]);
    res.json({
      playlists: +pl.rows[0].count,
      keywords: +kw.rows[0].count,
      data_points: +dp.rows[0].count,
      top3: +t3.rows[0].count,
      last_crawl: lc.rows[0]?.value || null,
      unseen_alerts: +ua.rows[0].count,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Playlists
app.get('/api/playlists', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM playlists ORDER BY created_at DESC');
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/playlists', async (req, res) => {
  try {
    const { spotifyUrl, label, notes, isCompetitor } = req.body;
    const match = spotifyUrl?.match(/playlist\/([A-Za-z0-9]+)/);
    if (!match) return res.status(400).json({ error: 'Invalid Spotify playlist URL' });
    const sid = match[1];

    const data = await spotifyGet(
      `/playlists/${sid}?fields=id,name,owner.display_name,followers.total,images`
    );

    const { rows } = await pool.query(
      `INSERT INTO playlists (spotify_id, name, owner, followers, image_url, label, notes, is_competitor)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (spotify_id) DO UPDATE
         SET name=$2, owner=$3, followers=$4, image_url=$5, label=$6, notes=$7, is_competitor=$8
       RETURNING *`,
      [
        data.id,
        data.name,
        data.owner?.display_name || null,
        data.followers?.total || 0,
        data.images?.[0]?.url || null,
        label || null,
        notes || null,
        isCompetitor === true || isCompetitor === 'true',
      ]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.patch('/api/playlists/:id', async (req, res) => {
  try {
    const { label, notes, is_competitor } = req.body;
    const { rows } = await pool.query(
      `UPDATE playlists SET label=$1, notes=$2, is_competitor=COALESCE($3, is_competitor) WHERE id=$4 RETURNING *`,
      [label, notes, is_competitor ?? null, req.params.id]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/playlists/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM playlists WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Keywords — only expose manually-managed keywords (hide master_sync internal ones)
app.get('/api/keywords', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT k.*,
        ROUND(COALESCE(
          (SELECT STDDEV(position) FROM rank_history
           WHERE keyword_id = k.id AND position IS NOT NULL
             AND checked_at > NOW() - INTERVAL '30 days'),
          0
        )::numeric, 1) AS volatility
      FROM keywords k
      WHERE k.source IS DISTINCT FROM 'master_sync'
      ORDER BY k.market, k.term
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/keywords', async (req, res) => {
  try {
    const term = req.body.term?.toLowerCase()?.trim();
    const market = req.body.market?.toUpperCase()?.trim();
    if (!term || !market) return res.status(400).json({ error: 'term and market required' });
    const { rows } = await pool.query(
      `INSERT INTO keywords (term, market) VALUES ($1,$2)
       ON CONFLICT (term, market) DO NOTHING RETURNING *`,
      [term, market]
    );
    res.json(rows[0] || { term, market, duplicate: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/keywords/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM keywords WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Rankings with LAG for prev_position
app.get('/api/rankings', async (req, res) => {
  try {
    const { playlist_id, market } = req.query;
    const conds = [];
    const params = [];
    let pi = 1;
    if (playlist_id) { conds.push(`l.playlist_id = $${pi++}`); params.push(playlist_id); }
    if (market)      { conds.push(`k.market = $${pi++}`);      params.push(market); }
    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : '';

    const { rows } = await pool.query(`
      WITH numbered AS (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY playlist_id, keyword_id ORDER BY checked_at DESC) AS rn,
          LAG(position)  OVER (PARTITION BY playlist_id, keyword_id ORDER BY checked_at)    AS prev_position
        FROM rank_history
      ),
      l AS (SELECT * FROM numbered WHERE rn = 1)
      SELECT
        l.playlist_id,
        l.keyword_id,
        p.name          AS playlist_name,
        p.image_url,
        p.label,
        p.genre,
        p.is_competitor,
        k.term,
        k.market,
        l.position,
        l.followers,
        l.checked_at,
        l.prev_position
      FROM l
      JOIN playlists p ON p.id = l.playlist_id
      JOIN keywords  k ON k.id = l.keyword_id
      ${where}
      ORDER BY l.position NULLS LAST, p.name, k.term
    `, params);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Follower snapshots (90-day chart)
app.get('/api/followers/:id', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (DATE(checked_at)) DATE(checked_at) AS date, followers
      FROM rank_history
      WHERE playlist_id = $1
        AND checked_at > NOW() - INTERVAL '90 days'
      ORDER BY DATE(checked_at), checked_at DESC
    `, [req.params.id]);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Position history for a single playlist+keyword
app.get('/api/history/:pid/:kid', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT position, followers, checked_at
      FROM rank_history
      WHERE playlist_id=$1 AND keyword_id=$2
      ORDER BY checked_at DESC LIMIT 30
    `, [req.params.pid, req.params.kid]);
    res.json(rows.reverse());
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Opportunities
app.get('/api/opportunities', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH latest AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position
        FROM rank_history
        ORDER BY playlist_id, keyword_id, checked_at DESC
      ),
      week_ago AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position AS old_position
        FROM rank_history
        WHERE checked_at <= NOW() - INTERVAL '6 days'
        ORDER BY playlist_id, keyword_id, checked_at DESC
      )
      SELECT
        p.name  AS playlist_name,
        p.id    AS playlist_id,
        k.term,
        k.market,
        l.position,
        COALESCE(w.old_position, l.position) - COALESCE(l.position, 0) AS improvement,
        w.old_position
      FROM latest l
      JOIN playlists p ON p.id = l.playlist_id
      JOIN keywords  k ON k.id = l.keyword_id
      LEFT JOIN week_ago w ON w.playlist_id = l.playlist_id AND w.keyword_id = l.keyword_id
    `);

    const opps = [];
    for (const r of rows) {
      const pos = r.position;
      const imp = Number(r.improvement) || 0;
      if (pos !== null && pos >= 11 && pos <= 20 && imp > 0) {
        opps.push({ type: 'quick_win', playlist_name: r.playlist_name, playlist_id: r.playlist_id, term: r.term, market: r.market, position: pos, improvement: imp, msg: `Ranked #${pos} and climbing — a small push could break the top 10` });
      }
      if (pos === null) {
        opps.push({ type: 'unranked', playlist_name: r.playlist_name, playlist_id: r.playlist_id, term: r.term, market: r.market, position: null, improvement: 0, msg: `Not yet ranking for "${r.term}" in ${r.market} — consider optimising the playlist` });
      }
      if (pos !== null && pos <= 3) {
        opps.push({ type: 'protect', playlist_name: r.playlist_name, playlist_id: r.playlist_id, term: r.term, market: r.market, position: pos, improvement: imp, msg: `Top 3 position — protect this ranking by keeping the playlist fresh` });
      }
      if (imp >= 5) {
        opps.push({ type: 'momentum', playlist_name: r.playlist_name, playlist_id: r.playlist_id, term: r.term, market: r.market, position: pos, improvement: imp, msg: `Gaining momentum — up ${imp} spots this week to #${pos}` });
      }
    }
    res.json(opps);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Alerts
app.get('/api/alerts', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT a.*, p.name AS playlist_name, k.term, k.market
      FROM alerts a
      JOIN playlists p ON p.id = a.playlist_id
      JOIN keywords  k ON k.id = a.keyword_id
      ORDER BY a.created_at DESC LIMIT 50
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/alerts/seen', async (req, res) => {
  try {
    await pool.query('UPDATE alerts SET seen=true');
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Settings
app.get('/api/settings', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM settings');
    const obj = {};
    for (const r of rows) obj[r.key] = r.value;
    res.json(obj);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/settings', async (req, res) => {
  try {
    const { key, value } = req.body;
    await pool.query(
      `INSERT INTO settings (key,value) VALUES ($1,$2)
       ON CONFLICT (key) DO UPDATE SET value=$2`,
      [key, value]
    );
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Notion test
app.post('/api/notion/test', async (req, res) => {
  try {
    const { token, dbId } = req.body;
    const r = await fetch(`https://api.notion.com/v1/databases/${dbId}`, {
      headers: { Authorization: `Bearer ${token}`, 'Notion-Version': '2022-06-28' },
    });
    if (!r.ok) return res.json({ ok: false, status: r.status });
    const data = await r.json();
    res.json({ ok: true, title: data.title?.[0]?.plain_text || 'Untitled' });
  } catch (e) {
    res.json({ ok: false, error: e.message });
  }
});

// Notion manual sync
app.post('/api/notion/sync', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (p.id, k.id)
        p.name AS playlist_name, k.term, k.market, rh.position, rh.followers
      FROM rank_history rh
      JOIN playlists p ON p.id = rh.playlist_id
      JOIN keywords  k ON k.id = rh.keyword_id
      ORDER BY p.id, k.id, rh.checked_at DESC
    `);
    syncToNotion(rows).catch(console.error);
    res.json({ ok: true, rows: rows.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Ranking trends: position deltas over 1 / 7 / 30 days + volatility index
app.get('/api/trends', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH latest AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position, checked_at
        FROM rank_history WHERE position IS NOT NULL
        ORDER BY playlist_id, keyword_id, checked_at DESC
      ),
      ago1 AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position
        FROM rank_history
        WHERE position IS NOT NULL AND checked_at <= NOW() - INTERVAL '22 hours'
        ORDER BY playlist_id, keyword_id, checked_at DESC
      ),
      ago7 AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position
        FROM rank_history
        WHERE position IS NOT NULL AND checked_at <= NOW() - INTERVAL '6 days'
        ORDER BY playlist_id, keyword_id, checked_at DESC
      ),
      ago30 AS (
        SELECT DISTINCT ON (playlist_id, keyword_id)
          playlist_id, keyword_id, position
        FROM rank_history
        WHERE position IS NOT NULL AND checked_at <= NOW() - INTERVAL '28 days'
        ORDER BY playlist_id, keyword_id, checked_at DESC
      ),
      vol AS (
        SELECT keyword_id, playlist_id,
          ROUND(COALESCE(STDDEV(position), 0)::numeric, 1) AS volatility
        FROM rank_history
        WHERE position IS NOT NULL AND checked_at > NOW() - INTERVAL '30 days'
        GROUP BY keyword_id, playlist_id
      )
      SELECT
        k.id   AS keyword_id,  k.term,  k.market,
        p.id   AS playlist_id, p.name AS playlist_name,
        l.position                                               AS current_pos,
        a1.position                                              AS pos_1d,
        a7.position                                              AS pos_7d,
        a30.position                                             AS pos_30d,
        COALESCE(a1.position,  l.position) - l.position         AS delta_1d,
        COALESCE(a7.position,  l.position) - l.position         AS delta_7d,
        COALESCE(a30.position, l.position) - l.position         AS delta_30d,
        COALESCE(v.volatility, 0)                                AS volatility
      FROM latest l
      JOIN playlists p ON p.id = l.playlist_id
      JOIN keywords  k ON k.id = l.keyword_id
      LEFT JOIN ago1  a1  ON a1.playlist_id  = l.playlist_id AND a1.keyword_id  = l.keyword_id
      LEFT JOIN ago7  a7  ON a7.playlist_id  = l.playlist_id AND a7.keyword_id  = l.keyword_id
      LEFT JOIN ago30 a30 ON a30.playlist_id = l.playlist_id AND a30.keyword_id = l.keyword_id
      LEFT JOIN vol   v   ON v.playlist_id   = l.playlist_id AND v.keyword_id   = l.keyword_id
      WHERE (p.is_competitor = false OR p.is_competitor IS NULL)
      ORDER BY delta_7d DESC NULLS LAST
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Top curators per market by number of top-10 appearances
app.get('/api/market-leaders', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH latest_snaps AS (
        SELECT DISTINCT ON (keyword_id, spotify_id)
          keyword_id, spotify_id, playlist_name, owner, followers, position
        FROM curator_snapshots
        WHERE position <= 10
        ORDER BY keyword_id, spotify_id, checked_at DESC
      )
      SELECT
        ls.owner,
        k.market,
        COUNT(DISTINCT ls.spotify_id)     AS top10_count,
        COUNT(DISTINCT ls.keyword_id)     AS keyword_count,
        ROUND(AVG(ls.followers))::bigint  AS avg_followers,
        ARRAY_AGG(DISTINCT ls.playlist_name) FILTER (WHERE ls.playlist_name IS NOT NULL) AS playlist_names
      FROM latest_snaps ls
      JOIN keywords k ON k.id = ls.keyword_id
      WHERE ls.owner IS NOT NULL AND ls.owner != ''
      GROUP BY ls.owner, k.market
      ORDER BY top10_count DESC, avg_followers DESC
      LIMIT 50
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// SEO ease-of-entry score per keyword (10 = easy, 1 = hard)
app.get('/api/seo-scores', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH top10 AS (
        SELECT DISTINCT ON (keyword_id, spotify_id)
          keyword_id, spotify_id, owner, followers, position
        FROM curator_snapshots
        WHERE position <= 10
        ORDER BY keyword_id, spotify_id, checked_at DESC
      ),
      kstats AS (
        SELECT
          keyword_id,
          COUNT(*)                                                  AS entries,
          ROUND(AVG(followers))::bigint                             AS avg_followers,
          MIN(followers) FILTER (WHERE position = 1)               AS top1_followers,
          (ARRAY_AGG(owner ORDER BY position))[1]                   AS top1_owner,
          COUNT(*) FILTER (WHERE followers < 10000)                 AS indie_count,
          COUNT(*) FILTER (WHERE followers > 500000)                AS major_count
        FROM top10
        GROUP BY keyword_id
      )
      SELECT
        k.id AS keyword_id, k.term, k.market,
        ks.entries,
        ks.avg_followers,
        ks.top1_followers,
        ks.top1_owner,
        ks.indie_count,
        ks.major_count,
        (ks.top1_followers IS NOT NULL AND ks.top1_followers < 50000) AS top_spot_indie,
        GREATEST(1, LEAST(10, ROUND((
          5.0
          + (ks.indie_count::float / GREATEST(ks.entries, 1)) * 5
          - (ks.major_count::float / GREATEST(ks.entries, 1)) * 4
          - (COALESCE(ks.avg_followers, 0) / 200000.0)
        )::numeric, 1)))::float AS ease_score
      FROM kstats ks
      JOIN keywords k ON k.id = ks.keyword_id
      ORDER BY ease_score DESC, avg_followers ASC
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Sync status — polled by frontend during crawl
app.get('/api/sync-status', (req, res) => {
  res.json({
    running:    syncState.running,
    current:    syncState.current,
    progress:   syncState.progress,
    total:      syncState.total,
    startedAt:  syncState.startedAt,
    pct:        syncState.total > 0 ? Math.round((syncState.progress / syncState.total) * 100) : 0,
  });
});

// Seasonal keyword suffix suggestions
app.get('/api/seasonal-suggestions', (req, res) => {
  const suffixes = getSeasonalSuffixes();
  const month    = new Date().toLocaleString('en-US', { month: 'long' });
  res.json({ suffixes, month });
});

// Market share — % of Top-10 rankings owned by label vs competitors, per keyword
app.get('/api/market-share', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH latest AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.position,
          p.is_competitor,
          k.term, k.market
        FROM rank_history rh
        JOIN playlists p ON p.id = rh.playlist_id
        JOIN keywords  k ON k.id = rh.keyword_id
        WHERE rh.position IS NOT NULL AND rh.position <= 10
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      )
      SELECT
        term,
        market,
        COUNT(*) FILTER (WHERE is_competitor = false OR is_competitor IS NULL) AS label_count,
        COUNT(*) FILTER (WHERE is_competitor = true)                           AS competitor_count,
        COUNT(*)                                                               AS total
      FROM latest
      GROUP BY term, market
      ORDER BY term, market
    `);

    const totalLabel      = rows.reduce((s, r) => s + Number(r.label_count), 0);
    const totalCompetitor = rows.reduce((s, r) => s + Number(r.competitor_count), 0);
    const total           = totalLabel + totalCompetitor;

    res.json({
      summary: {
        label:       totalLabel,
        competitors: totalCompetitor,
        total,
        label_pct:      total ? Math.round((totalLabel / total) * 100) : 0,
        competitor_pct: total ? Math.round((totalCompetitor / total) * 100) : 0,
      },
      byKeyword: rows.map((r) => ({
        term:           r.term,
        market:         r.market,
        label_count:    Number(r.label_count),
        competitor_count: Number(r.competitor_count),
        total:          Number(r.total),
      })),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Target Genres CRUD ────────────────────────────────────────────────────────

app.get('/api/genres', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM target_genres ORDER BY name');
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/genres', async (req, res) => {
  try {
    const name = (req.body.name || '').toLowerCase().trim();
    if (!name) return res.status(400).json({ error: 'name required' });
    const { rows } = await pool.query(
      `INSERT INTO target_genres (name) VALUES ($1) ON CONFLICT (name) DO NOTHING RETURNING *`,
      [name]
    );
    res.json(rows[0] || { name, duplicate: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/genres/:id', async (req, res) => {
  try {
    await pool.query('DELETE FROM target_genres WHERE id=$1', [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Acquisition CRM ───────────────────────────────────────────────────────────

app.patch('/api/crm/:spotifyId', async (req, res) => {
  try {
    const { status, notes } = req.body;
    const { rows } = await pool.query(`
      INSERT INTO acquisition_crm (spotify_id, status, notes, updated_at)
      VALUES ($1, $2, $3, NOW())
      ON CONFLICT (spotify_id) DO UPDATE
        SET status = COALESCE($2, acquisition_crm.status),
            notes  = COALESCE($3, acquisition_crm.notes),
            updated_at = NOW()
      RETURNING *
    `, [req.params.spotifyId, status || 'New', notes ?? '']);
    res.json(rows[0]);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Priority Scout Alerts ─────────────────────────────────────────────────────

const SCOUT_GENRES = [
  'lo-fi','lofi','lo fi','ambient','sleep','study','focus','chill',
  'house','deep house','tech house','afro house','afro-house','melodic house',
  'chillout','downtempo','new age','meditation','binaural',
];

app.get('/api/acquisition-alerts', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH our_ids AS (SELECT spotify_id FROM playlists),
      gems AS (
        SELECT DISTINCT ON (cs.spotify_id)
          cs.spotify_id,
          cs.playlist_name,
          cs.owner,
          cs.followers,
          cs.position,
          cs.genre,
          cs.contact_info,
          k.market,
          k.term,
          ac.status AS crm_status
        FROM curator_snapshots cs
        JOIN keywords k ON k.id = cs.keyword_id
        LEFT JOIN acquisition_crm ac ON ac.spotify_id = cs.spotify_id
        WHERE cs.position BETWEEN 1 AND 10
          AND cs.followers > 0
          AND cs.followers < 5000
          AND cs.spotify_id NOT IN (SELECT spotify_id FROM our_ids)
          AND cs.contact_info IS NOT NULL
          AND cs.contact_info != '{}'::jsonb
          AND cs.contact_info::text NOT IN ('{}','null','""')
        ORDER BY cs.spotify_id, cs.position ASC
      )
      SELECT * FROM gems ORDER BY position ASC, followers ASC LIMIT 50
    `);
    // Filter to core genres server-side for flexibility
    const scoutSet = new Set(SCOUT_GENRES);
    const alerts = rows.filter(r => {
      const g = (r.genre || '').toLowerCase();
      return SCOUT_GENRES.some(sg => g.includes(sg));
    });
    res.json(alerts);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Acquisition endpoint — Valuation 3.0, new filters ────────────────────────

app.get('/api/acquisition', async (req, res) => {
  try {
    const { verified, dm_available, genre_filter, market } = req.query;

    // Build dynamic filter conditions
    const extraConds = [];
    if (verified === 'false') {
      // Editorial/verified: name starts with "This Is" or owner is Spotify
      extraConds.push(`(cs.playlist_name ILIKE 'This Is%' OR cs.owner ILIKE 'spotify%')`);
    } else if (verified === 'true') {
      // Non-editorial only
      extraConds.push(`cs.playlist_name NOT ILIKE 'This Is%' AND cs.owner NOT ILIKE 'spotify%'`);
    }
    if (dm_available === 'true') {
      extraConds.push(`cs.contact_info IS NOT NULL AND cs.contact_info != '{}'::jsonb AND cs.contact_info::text NOT IN ('{}','null','""')`);
    } else if (dm_available === 'false') {
      extraConds.push(`(cs.contact_info IS NULL OR cs.contact_info = '{}'::jsonb OR cs.contact_info::text IN ('{}','null','""'))`);
    }
    if (market) extraConds.push(`k.market = '${market.replace(/'/g,"''")}'`);
    if (genre_filter) extraConds.push(`cs.genre ILIKE '%${genre_filter.replace(/'/g,"''").replace(/%/g,'\\%')}%'`);
    const extraWhere = extraConds.length ? 'AND ' + extraConds.join(' AND ') : '';

    const { rows } = await pool.query(`
      WITH our_ids AS (SELECT spotify_id FROM playlists),
      latest AS (
        SELECT DISTINCT ON (cs.spotify_id)
          cs.spotify_id,
          cs.playlist_name,
          cs.owner,
          cs.followers,
          cs.contact_info,
          cs.genre,
          cs.position,
          k.term,
          k.market,
          cs.checked_at,
          ROUND(((6 - cs.position) * 15.0) + GREATEST(0, (5000 - COALESCE(cs.followers,0))::float / 100)) AS opportunity_score,
          ROUND(((11.0 - cs.position) * 100.0) / SQRT(GREATEST(cs.followers, 100)))::int AS yield_score,
          -- Valuation 3.2: royalty rate × tier × rank × Revenue Premium × LTV Bonus × 5yr
          ROUND(
            (COALESCE(cs.followers,0) * 0.15)
            * CASE WHEN k.market IN ('US','GB','UK','DK','IS','NO','MC','FI','CH','IE','LI','SE','NZ','LU','AD','NL','AU','AT','DE','FR','BE') THEN 0.0038
                   WHEN k.market IN ('CA','CY','IL','HK','EE','MT','SG','AE','ES','CZ','IT','LT','GR','HU','RO','SK','UY','PT','BR','MX') THEN 0.0026
                   ELSE 0.0015 END
            * CASE WHEN k.market IN ('US','GB','UK','DK','IS','NO','MC','FI','CH','IE','LI','SE','NZ','LU','AD','NL','AU','AT','DE','FR','BE') THEN 1.3
                   WHEN k.market IN ('CA','CY','IL','HK','EE','MT','SG','AE','ES','CZ','IT','LT','GR','HU','RO','SK','UY','PT','BR','MX') THEN 1.1
                   ELSE 0.9 END
            * CASE WHEN cs.position = 1  THEN 1.6
                   WHEN cs.position <= 3 THEN 1.3
                   WHEN cs.position <= 5 THEN 1.1
                   ELSE 1.0 END
            -- Revenue Premium: T1/T2 top-3 = 1.4×
            * CASE WHEN cs.position <= 3
                        AND k.market IN ('US','GB','UK','DK','IS','NO','MC','FI','CH','IE','LI','SE','NZ','LU','AD','NL','AU','AT','DE','FR','BE',
                                          'CA','CY','IL','HK','EE','MT','SG','AE','ES','CZ','IT','LT','GR','HU','RO','SK','UY','PT','BR','MX')
                   THEN 1.4 ELSE 1.0 END
            -- LTV Bonus: passive genre (sleep, lo-fi, ambient…) = 1.25×
            * CASE WHEN cs.genre ILIKE '%sleep%' OR cs.genre ILIKE '%lo-fi%' OR cs.genre ILIKE '%lofi%'
                        OR cs.genre ILIKE '%ambient%' OR cs.genre ILIKE '%study%' OR cs.genre ILIKE '%focus%'
                        OR cs.genre ILIKE '%relax%' OR cs.genre ILIKE '%meditation%' OR cs.genre ILIKE '%chill%'
                   THEN 1.25 ELSE 1.0 END
            * 60  -- 12 months × 5 years
          ) AS val_mid,
          -- Bonus flags for UI badges
          (cs.position <= 3 AND k.market IN ('US','GB','UK','DK','IS','NO','MC','FI','CH','IE','LI','SE','NZ','LU','AD','NL','AU','AT','DE','FR','BE','CA','CY','IL','HK','EE','MT','SG','AE','ES','CZ','IT','LT','GR','HU','RO','SK','UY','PT','BR','MX')) AS has_rev_premium,
          (cs.genre ILIKE '%sleep%' OR cs.genre ILIKE '%lo-fi%' OR cs.genre ILIKE '%lofi%'
           OR cs.genre ILIKE '%ambient%' OR cs.genre ILIKE '%study%' OR cs.genre ILIKE '%focus%'
           OR cs.genre ILIKE '%relax%' OR cs.genre ILIKE '%meditation%' OR cs.genre ILIKE '%chill%') AS has_ltv_bonus
        FROM curator_snapshots cs
        JOIN keywords k ON k.id = cs.keyword_id
        WHERE cs.position BETWEEN 1 AND 10
          AND cs.followers > 0
          AND cs.followers < 10000
          AND cs.spotify_id NOT IN (SELECT spotify_id FROM our_ids)
          ${extraWhere}
        ORDER BY cs.spotify_id, cs.position ASC
      )
      SELECT
        l.*,
        ROUND(l.val_mid * 0.75)  AS val_low,
        ROUND(l.val_mid * 1.25)  AS val_high,
        COALESCE(ac.status, 'New')                                       AS crm_status,
        COALESCE(ac.notes, '')                                            AS crm_notes,
        (
          ac.snapshot_at IS NOT NULL
          AND ac.snapshot_at < NOW() - INTERVAL '30 days'
          AND ABS(COALESCE(l.followers,0) - COALESCE(ac.snapshot_followers,0)) <= 100
        )                                                                  AS is_dormant
      FROM latest l
      LEFT JOIN acquisition_crm ac ON ac.spotify_id = l.spotify_id
      ORDER BY opportunity_score DESC, position ASC
      LIMIT 300
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Updated Leads endpoint — with CRM status ──────────────────────────────────

app.get('/api/leads', async (req, res) => {
  try {
    const minFollowers = Math.max(0, Number(req.query.min_followers) || 1000);
    const genreFilter  = req.query.genre || '';
    const marketFilter = req.query.market || '';

    const params = [minFollowers];
    let extraWhere = `AND cs.followers >= $1`;
    if (genreFilter)  { params.push(`%${genreFilter}%`);  extraWhere += ` AND cs.genre ILIKE $${params.length}`; }
    if (marketFilter) { params.push(marketFilter);         extraWhere += ` AND k.market = $${params.length}`; }

    const { rows } = await pool.query(`
      WITH ranked_contacts AS (
        SELECT DISTINCT ON (cs.spotify_id)
          cs.spotify_id,
          cs.playlist_name,
          cs.owner,
          cs.followers,
          cs.contact_info,
          cs.position,
          cs.genre,
          k.term,
          k.market,
          cs.checked_at
        FROM curator_snapshots cs
        JOIN keywords k ON k.id = cs.keyword_id
        WHERE cs.contact_info IS NOT NULL
          AND cs.contact_info != '{}'::jsonb
          AND cs.contact_info::text NOT IN ('{}','null','""')
          ${extraWhere}
        ORDER BY cs.spotify_id, cs.position ASC
      ),
      influence AS (
        SELECT cs.spotify_id,
               COUNT(DISTINCT cs.keyword_id) FILTER (WHERE cs.position <= 10) AS influence_count
        FROM curator_snapshots cs
        WHERE cs.position <= 10
        GROUP BY cs.spotify_id
      )
      SELECT
        rc.*,
        COALESCE(inf.influence_count, 0)  AS influence_count,
        COALESCE(ac.status, 'New')        AS crm_status,
        COALESCE(ac.notes, '')            AS crm_notes
      FROM ranked_contacts rc
      LEFT JOIN influence inf ON inf.spotify_id = rc.spotify_id
      LEFT JOIN acquisition_crm ac ON ac.spotify_id = rc.spotify_id
      ORDER BY influence_count DESC, rc.position ASC
      LIMIT 500
    `, params);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Keyword Discovery endpoint ────────────────────────────────────────────────

app.get('/api/discovery', async (req, res) => {
  try {
    const { market, tracked_only } = req.query;

    const params = [];
    let marketCond = '';
    if (market) { params.push(market); marketCond = `AND k.market = $${params.length}`; }

    let trackedCond = '';
    if (tracked_only === 'true') {
      trackedCond = `AND k.source != 'master_sync'`;
    }

    const { rows } = await pool.query(`
      WITH snap_stats AS (
        SELECT
          k.term,
          k.market,
          COUNT(cs.id)                                                              AS playlist_count,
          SUM(cs.followers)                                                         AS total_followers,
          MAX(cs.followers) FILTER (WHERE cs.position = 1)                         AS top1_followers,
          ROUND(AVG(cs.followers) FILTER (WHERE cs.position <= 10))                AS avg_top10_followers,
          BOOL_OR(k.source IS DISTINCT FROM 'master_sync')                         AS is_tracked
        FROM curator_snapshots cs
        JOIN keywords k ON k.id = cs.keyword_id
        WHERE cs.checked_at > NOW() - INTERVAL '7 days'
          ${marketCond} ${trackedCond}
        GROUP BY k.term, k.market
      ),
      with_intel AS (
        SELECT
          ss.*,
          COALESCE(ki.search_volume,  ROUND(SQRT(GREATEST(ss.total_followers::float / GREATEST(ss.playlist_count,1), 0)) * 2.5)::int)  AS search_volume,
          COALESCE(ki.growth_pct,     0)                          AS growth_pct,
          COALESCE(ki.competition,    1.0)                        AS competition,
          COALESCE(ki.traffic_score,  1)                          AS traffic_score,
          ki.updated_at                                           AS intel_updated_at
        FROM snap_stats ss
        LEFT JOIN keyword_intel ki ON ki.term = ss.term AND ki.market = ss.market
      )
      SELECT * FROM with_intel
      ORDER BY traffic_score DESC, total_followers DESC NULLS LAST
      LIMIT 500
    `, params);

    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Trending Keywords endpoint (replaces market share on dashboard) ────────────

app.get('/api/trending-keywords', async (req, res) => {
  try {
    const { market, tracked_only, limit: lim } = req.query;
    const limitN = Math.min(parseInt(lim) || 20, 100);

    const params = [];
    let mkt = '';
    if (market) { params.push(market); mkt = `AND k.market = $${params.length}`; }
    let trackedCond = '';
    if (tracked_only === 'true') trackedCond = `AND k.source != 'master_sync'`;
    params.push(limitN);
    const limitParam = `$${params.length}`;

    const { rows } = await pool.query(`
      WITH current AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.keyword_id, rh.position, rh.followers, rh.checked_at
        FROM rank_history rh
        JOIN keywords k ON k.id = rh.keyword_id
        WHERE rh.position IS NOT NULL ${mkt} ${trackedCond}
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      ),
      week_ago AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.keyword_id, rh.followers AS old_followers
        FROM rank_history rh
        JOIN keywords k ON k.id = rh.keyword_id
        WHERE rh.position IS NOT NULL
          AND rh.checked_at <= NOW() - INTERVAL '6 days'
          ${mkt} ${trackedCond}
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      )
      SELECT
        k.term,
        k.market,
        ROUND(AVG(c.position), 1)                                     AS avg_position,
        SUM(c.followers)                                               AS total_followers,
        CASE
          WHEN SUM(COALESCE(w.old_followers, 0)) > 0
          THEN ROUND(((SUM(c.followers) - SUM(COALESCE(w.old_followers, c.followers)))::float
                       / NULLIF(SUM(COALESCE(w.old_followers, c.followers)), 0)) * 100, 1)
          ELSE 0
        END                                                            AS growth_pct_7d,
        COALESCE(ki.traffic_score, 1)                                  AS traffic_score,
        COALESCE(ki.search_volume, 0)                                  AS search_volume,
        COUNT(DISTINCT c.keyword_id)                                   AS entry_count
      FROM current c
      JOIN keywords k  ON k.id = c.keyword_id
      LEFT JOIN week_ago w  ON w.keyword_id = c.keyword_id
      LEFT JOIN keyword_intel ki ON ki.term = k.term AND ki.market = k.market
      GROUP BY k.term, k.market, ki.traffic_score, ki.search_volume
      ORDER BY traffic_score DESC, growth_pct_7d DESC, total_followers DESC
      LIMIT ${limitParam}
    `, params);

    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Gap-to-#1 Analysis ────────────────────────────────────────────────────────

app.get('/api/gap-analysis', async (req, res) => {
  try {
    const { market } = req.query;
    const params  = [];
    let mktCond = '';
    if (market) { params.push(market); mktCond = `AND k.market = $${params.length}`; }

    // Get all tracked playlists + their current positions and top-1 competitor data
    const { rows } = await pool.query(`
      WITH latest AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id,
          rh.position, rh.followers, rh.checked_at
        FROM rank_history rh
        JOIN keywords k ON k.id = rh.keyword_id
        WHERE rh.position IS NOT NULL ${mktCond}
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      ),
      growth AS (
        SELECT
          rh.playlist_id,
          ROUND(
            (MAX(rh.followers) - MIN(rh.followers))::float /
            NULLIF(EXTRACT(days FROM MAX(rh.checked_at) - MIN(rh.checked_at)), 0) * 30
          ) AS monthly_follower_growth
        FROM rank_history rh
        WHERE rh.checked_at > NOW() - INTERVAL '30 days'
          AND rh.followers > 0
        GROUP BY rh.playlist_id
      ),
      top1 AS (
        SELECT DISTINCT ON (cs.keyword_id)
          cs.keyword_id,
          cs.followers  AS top1_followers,
          cs.owner      AS top1_owner,
          cs.playlist_name AS top1_name
        FROM curator_snapshots cs
        WHERE cs.position = 1
          AND cs.spotify_id NOT IN (SELECT spotify_id FROM playlists)
        ORDER BY cs.keyword_id, cs.checked_at DESC
      )
      SELECT
        p.name        AS playlist_name,
        p.id          AS playlist_id,
        k.term,
        k.market,
        l.position    AS current_position,
        l.followers   AS current_followers,
        COALESCE(g.monthly_follower_growth, 0) AS monthly_growth,
        t.top1_followers,
        t.top1_owner,
        t.top1_name,
        GREATEST(0, COALESCE(t.top1_followers, 0) - COALESCE(l.followers, 0))  AS gap,
        CASE
          WHEN COALESCE(g.monthly_follower_growth, 0) > 0
          THEN ROUND(GREATEST(0, COALESCE(t.top1_followers, 0) - COALESCE(l.followers, 0))
               / g.monthly_follower_growth)
          ELSE NULL
        END AS months_to_catch
      FROM latest l
      JOIN playlists p  ON p.id  = l.playlist_id
      JOIN keywords  k  ON k.id  = l.keyword_id
      LEFT JOIN growth g ON g.playlist_id = l.playlist_id
      LEFT JOIN top1   t ON t.keyword_id  = l.keyword_id
      WHERE l.position > 1
        AND p.is_competitor = FALSE
      ORDER BY l.position ASC, gap DESC
      LIMIT 100
    `, params);

    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Telegram test endpoint ────────────────────────────────────────────────────

app.post('/api/telegram/test', async (req, res) => {
  try {
    const { chatId } = req.body;
    if (!chatId) return res.status(400).json({ error: 'chatId required' });
    const result = await sendTelegramAlert(chatId,
      `✅ <b>Rankify connected!</b>\n\nYour Telegram alerts are configured. You'll be notified when your playlists enter the Top 15 in any tracked market.`
    );
    if (result?.ok) {
      await pool.query(
        `INSERT INTO settings (key,value) VALUES ('telegram_chat_id',$1) ON CONFLICT (key) DO UPDATE SET value=$1`,
        [chatId]
      );
      res.json({ ok: true });
    } else {
      res.json({ ok: false, error: result?.description || 'Unknown error' });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Manual keyword intel refresh ──────────────────────────────────────────────

app.post('/api/intel/refresh', async (req, res) => {
  const { term, market } = req.body;
  if (!term || !market) return res.status(400).json({ error: 'term and market required' });
  try {
    const { rows: kr } = await pool.query(
      `SELECT value FROM settings WHERE key='playlistranking_api_key'`
    );
    const apiKey = kr[0]?.value || null;
    const intel = await enrichKeywordIntel(term, market, apiKey);
    await pool.query(`
      INSERT INTO keyword_intel (term, market, search_volume, growth_pct, competition, traffic_score, total_followers, top1_followers, updated_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
      ON CONFLICT (term, market) DO UPDATE
        SET search_volume=EXCLUDED.search_volume, growth_pct=EXCLUDED.growth_pct,
            competition=EXCLUDED.competition, traffic_score=EXCLUDED.traffic_score,
            total_followers=EXCLUDED.total_followers, top1_followers=EXCLUDED.top1_followers,
            updated_at=NOW()
    `, [term, market, intel.searchVolume, intel.growthPct, intel.competition, intel.trafficScore, intel.totalFollowers, intel.top1Followers]);
    res.json({ ok: true, intel });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── Daily Report ─────────────────────────────────────────────────────────────

app.get('/api/daily-report', async (req, res) => {
  try {
    // Top 10 growers (biggest follower gain in last 7 days)
    const { rows: growers } = await pool.query(`
      WITH recent AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.followers, rh.position
        FROM rank_history rh ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      ),
      week_ago AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.followers AS old_followers
        FROM rank_history rh
        WHERE rh.checked_at <= NOW() - INTERVAL '6 days'
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      )
      SELECT p.name AS playlist_name, p.spotify_id, p.image_url,
             k.term, k.market, r.position, r.followers,
             COALESCE(r.followers, 0) - COALESCE(w.old_followers, r.followers) AS follower_gain,
             CASE WHEN COALESCE(w.old_followers, 0) > 0
               THEN ROUND(((r.followers - w.old_followers)::float / w.old_followers * 100)::numeric, 1)
               ELSE 0 END AS growth_pct
      FROM recent r
      JOIN playlists p ON p.id = r.playlist_id
      JOIN keywords  k ON k.id = r.keyword_id
      LEFT JOIN week_ago w ON w.playlist_id = r.playlist_id AND w.keyword_id = r.keyword_id
      WHERE r.followers > 0
      ORDER BY follower_gain DESC NULLS LAST
      LIMIT 10
    `);

    // Top 10 DM targets (ranked + has contact, not already ours)
    const { rows: dmTargets } = await pool.query(`
      SELECT DISTINCT ON (cs.spotify_id)
        cs.playlist_name, cs.spotify_id, cs.owner, cs.followers,
        cs.contact_info, cs.genre, k.term, k.market, cs.position
      FROM curator_snapshots cs
      JOIN keywords k ON k.id = cs.keyword_id
      WHERE cs.contact_info IS NOT NULL AND cs.contact_info != '{}'::jsonb
        AND cs.contact_info::text NOT IN ('{}','null','""')
        AND cs.position <= 10
        AND cs.checked_at > NOW() - INTERVAL '7 days'
        AND cs.spotify_id NOT IN (SELECT spotify_id FROM playlists)
      ORDER BY cs.spotify_id, cs.position ASC, cs.followers DESC
      LIMIT 10
    `);

    // Top 10 trending keywords by traffic score
    const { rows: trending } = await pool.query(`
      SELECT k.term, k.market,
             COALESCE(ki.traffic_score, 1) AS traffic_score,
             COALESCE(ki.growth_pct, 0)    AS growth_pct,
             COALESCE(ki.search_volume, 0) AS search_volume
      FROM keyword_intel ki
      JOIN keywords k ON k.term = ki.term AND k.market = ki.market
      WHERE ki.traffic_score > 0
      ORDER BY ki.traffic_score DESC, ki.growth_pct DESC
      LIMIT 10
    `);

    res.json({
      growers, dmTargets, trending,
      generated_at: new Date().toISOString(),
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Rankings — Grouped by Playlist view ──────────────────────────────────────

app.get('/api/rankings/grouped', async (req, res) => {
  try {
    const { market, max_position } = req.query;
    const params = [];
    const conds  = [];
    if (market)       { params.push(market);           conds.push(`k.market = $${params.length}`); }
    if (max_position) { params.push(Number(max_position)); conds.push(`c.position <= $${params.length}`); }
    const where = conds.length ? 'AND ' + conds.join(' AND ') : '';

    const { rows } = await pool.query(`
      WITH current AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.position, rh.followers, rh.checked_at
        FROM rank_history rh WHERE rh.position IS NOT NULL
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      ),
      prev7 AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.position AS pos_7d
        FROM rank_history rh
        WHERE rh.position IS NOT NULL AND rh.checked_at <= NOW() - INTERVAL '6 days'
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      ),
      prev30 AS (
        SELECT DISTINCT ON (rh.playlist_id, rh.keyword_id)
          rh.playlist_id, rh.keyword_id, rh.position AS pos_30d
        FROM rank_history rh
        WHERE rh.position IS NOT NULL AND rh.checked_at <= NOW() - INTERVAL '29 days'
        ORDER BY rh.playlist_id, rh.keyword_id, rh.checked_at DESC
      )
      SELECT p.name AS playlist_name, p.spotify_id, p.image_url, p.genre,
             k.term, k.market, c.position, c.followers,
             p7.pos_7d, p30.pos_30d
      FROM current c
      JOIN playlists p ON p.id = c.playlist_id
      JOIN keywords  k ON k.id = c.keyword_id
      LEFT JOIN prev7  p7  ON p7.playlist_id  = c.playlist_id AND p7.keyword_id  = c.keyword_id
      LEFT JOIN prev30 p30 ON p30.playlist_id = c.playlist_id AND p30.keyword_id = c.keyword_id
      WHERE 1=1 ${where}
      ORDER BY p.name, c.position
    `, params);

    // Group client-facing
    const groups = {};
    for (const r of rows) {
      if (!groups[r.spotify_id]) {
        groups[r.spotify_id] = {
          spotify_id: r.spotify_id, playlist_name: r.playlist_name,
          image_url: r.image_url, genre: r.genre, keywords: [],
        };
      }
      groups[r.spotify_id].keywords.push({
        term: r.term, market: r.market, position: r.position,
        followers: r.followers, pos_7d: r.pos_7d, pos_30d: r.pos_30d,
      });
    }
    res.json(Object.values(groups));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Monitor — Suspicious Usage + Watchlist ────────────────────────────────────

app.get('/api/monitor', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      WITH owner_farm AS (
        -- Network Farm: same owner has 3+ playlists appearing in same keyword search
        SELECT owner, keyword_id, COUNT(DISTINCT spotify_id) AS farm_count
        FROM curator_snapshots
        WHERE checked_at > NOW() - INTERVAL '14 days'
          AND owner IS NOT NULL AND owner != ''
        GROUP BY owner, keyword_id
        HAVING COUNT(DISTINCT spotify_id) >= 3
      ),
      farm_owners AS (
        SELECT DISTINCT owner FROM owner_farm
      )
      SELECT DISTINCT ON (cs.spotify_id)
        cs.spotify_id, cs.playlist_name, cs.owner, cs.followers,
        cs.position, cs.genre, cs.contact_info, cs.checked_at,
        k.term, k.market,
        mw.id       AS watched_id,
        mw.reason   AS watch_reason,
        -- Flags
        (cs.position <= 5 AND cs.followers < 500)        AS flag_zero_engage,
        (cs.position <= 3 AND cs.followers > 50000)      AS flag_mega_player,
        (cs.followers > 5000 AND cs.position > 8)        AS flag_growth_spike,
        (fo.owner IS NOT NULL)                            AS flag_network_farm
      FROM curator_snapshots cs
      JOIN keywords k ON k.id = cs.keyword_id
      LEFT JOIN monitor_watchlist mw ON mw.spotify_id = cs.spotify_id
      LEFT JOIN farm_owners fo ON fo.owner = cs.owner
      WHERE cs.checked_at > NOW() - INTERVAL '14 days'
        AND (
          mw.id IS NOT NULL
          OR (cs.position <= 5 AND cs.followers < 500)
          OR (cs.followers > 5000 AND cs.position > 8)
          OR fo.owner IS NOT NULL
        )
      ORDER BY cs.spotify_id, cs.checked_at DESC
      LIMIT 200
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Network Farms summary endpoint
app.get('/api/monitor/farms', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT
        cs.owner,
        COUNT(DISTINCT cs.spotify_id) AS playlist_count,
        SUM(cs.followers)             AS total_followers,
        STRING_AGG(DISTINCT cs.playlist_name, ', ' ORDER BY cs.playlist_name LIMIT 3) AS sample_names,
        MAX(cs.checked_at)            AS last_seen
      FROM curator_snapshots cs
      WHERE cs.checked_at > NOW() - INTERVAL '14 days'
        AND cs.owner IS NOT NULL AND cs.owner != ''
      GROUP BY cs.owner
      HAVING COUNT(DISTINCT cs.spotify_id) >= 3
      ORDER BY playlist_count DESC
      LIMIT 50
    `);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/monitor/watchlist', async (req, res) => {
  try {
    const { rows } = await pool.query(`SELECT * FROM monitor_watchlist ORDER BY added_at DESC`);
    res.json(rows);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/monitor/watch', async (req, res) => {
  const { spotify_id, name, reason } = req.body;
  if (!spotify_id) return res.status(400).json({ error: 'spotify_id required' });
  try {
    await pool.query(
      `INSERT INTO monitor_watchlist (spotify_id, name, reason) VALUES ($1,$2,$3) ON CONFLICT (spotify_id) DO UPDATE SET reason=EXCLUDED.reason`,
      [spotify_id, name || spotify_id, reason || '']
    );
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/monitor/watch/:id', async (req, res) => {
  try {
    await pool.query(`DELETE FROM monitor_watchlist WHERE spotify_id = $1`, [req.params.id]);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Seasonal Opportunity Engine ────────────────────────────────────────────────

app.get('/api/seasonal', async (req, res) => {
  try {
    const month  = new Date().getMonth() + 1;
    const season = (month >= 12 || month <= 2) ? 'winter' :
                   (month >= 3  && month <= 5)  ? 'spring' :
                   (month >= 6  && month <= 8)  ? 'summer' : 'fall';

    const THEMES = {
      winter: [
        { theme: 'Winter Arc', icon: '❄️', peak: 'Dec–Feb', trend_pct: 34,
          keywords: ['gym phonk','workout phonk','dark phonk','winter workout','winter gym','phonk playlist'] },
        { theme: 'Deep Focus', icon: '🧠', peak: 'Year-round', trend_pct: 18,
          keywords: ['study music','focus music','deep work','lo-fi study','concentration music'] },
        { theme: 'Holiday Vibes', icon: '🎄', peak: 'Nov–Jan', trend_pct: 220,
          keywords: ['christmas music','winter songs','holiday playlist','christmas hits','xmas music'] },
      ],
      spring: [
        { theme: 'Spring Energy', icon: '🌸', peak: 'Mar–May', trend_pct: 28,
          keywords: ['spring playlist','happy playlist','uplifting music','spring hits','feel good music'] },
        { theme: 'Study Season', icon: '📚', peak: 'Apr–Jun', trend_pct: 45,
          keywords: ['exam study music','focus playlist','study beats','study music 2026','deep focus study'] },
      ],
      summer: [
        { theme: 'Summer Vibes', icon: '☀️', peak: 'Jun–Aug', trend_pct: 67,
          keywords: ['summer hits','summer playlist','beach music','summer songs 2026','hot summer playlist'] },
        { theme: 'Workout Peak', icon: '💪', peak: 'Apr–Sep', trend_pct: 52,
          keywords: ['gym music','workout playlist','running music','fitness music','gym hits 2026'] },
      ],
      fall: [
        { theme: 'Autumn Mood', icon: '🍂', peak: 'Sep–Nov', trend_pct: 31,
          keywords: ['autumn playlist','fall vibes','cozy music','fall songs','autumn chill'] },
        { theme: 'Back to Work', icon: '💼', peak: 'Sep–Oct', trend_pct: 38,
          keywords: ['focus music','productivity playlist','work from home music','deep work','work playlist'] },
      ],
    };

    const themes = THEMES[season] || [];
    const results = [];

    for (const theme of themes) {
      const lowerKws = theme.keywords.map(k => k.toLowerCase());
      const { rows } = await pool.query(`
        SELECT k.term, k.market,
               COALESCE(ki.traffic_score, 1)   AS traffic_score,
               COALESCE(ki.growth_pct, 0)       AS growth_pct,
               COALESCE(ki.search_volume, 0)    AS search_volume,
               COALESCE(ki.total_followers, 0)  AS total_followers
        FROM keywords k
        LEFT JOIN keyword_intel ki ON ki.term = k.term AND ki.market = k.market
        WHERE LOWER(k.term) = ANY($1::text[])
        ORDER BY traffic_score DESC
        LIMIT 6
      `, [lowerKws]);
      results.push({ ...theme, season, tracked: rows });
    }

    const monthName = new Date().toLocaleString('en-US', { month: 'long' });
    res.json({ season, month: monthName, themes: results });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Selective / Deep Research crawl helpers ───────────────────────────────────

async function deepResearchKeyword(term, market, perPage = 20) {
  console.log(`[research] keyword "${term}" in ${market}, limit ${perPage}`);
  const token = await getToken();
  const encoded = encodeURIComponent(term);
  const res = await fetch(
    `https://api.spotify.com/v1/search?q=${encoded}&type=playlist&market=${market}&limit=${perPage}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  if (!res.ok) throw new Error(`Spotify search error: ${res.status}`);
  const data = await res.json();
  const playlists = data?.playlists?.items || [];

  // Upsert keyword
  const { rows: kwRows } = await pool.query(
    `INSERT INTO keywords (term, market, source) VALUES ($1,$2,'manual')
     ON CONFLICT (term, market) DO UPDATE SET source='manual' RETURNING id`,
    [term.toLowerCase(), market]
  );
  const kwId = kwRows[0].id;

  const results = [];
  for (let i = 0; i < playlists.length; i++) {
    const pl = playlists[i];
    if (!pl) continue;
    await sleep(200);
    try {
      const detail = await spotifyGet(
        `/playlists/${pl.id}?fields=id,name,owner.display_name,followers.total,images,description`
      );
      const followers = detail?.followers?.total || 0;
      const owner     = detail?.owner?.display_name || pl.owner?.display_name || '';
      const imgUrl    = detail?.images?.[0]?.url || pl.images?.[0]?.url || null;
      const name      = detail?.name || pl.name || '';
      const desc      = detail?.description || '';

      // Extract contact info from description
      const emails     = [...new Set(desc.match(/[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g) || [])];
      const instagrams = [...new Set((desc.match(/@([A-Za-z0-9_.]{1,30})/g) || []).map(m => m))];
      const linktrees  = [...new Set((desc.match(/linktr\.ee\/\S+/g) || []))];
      const contactInfo = { emails, instagrams, linktrees };

      await pool.query(`
        INSERT INTO curator_snapshots
          (keyword_id, position, spotify_id, playlist_name, owner, followers, description, contact_info, checked_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT DO NOTHING
      `, [kwId, i + 1, pl.id, name, owner, followers, desc.slice(0, 500), JSON.stringify(contactInfo)]);

      results.push({ position: i + 1, name, followers, spotify_id: pl.id });
    } catch(e) {
      console.error(`[research] playlist ${pl.id}:`, e.message);
    }
  }

  console.log(`[research] "${term}" in ${market}: ${results.length} results saved`);
  return results;
}

async function deepResearchPlaylist(spotifyId) {
  console.log(`[research] playlist ${spotifyId}`);
  const detail = await spotifyGet(
    `/playlists/${spotifyId}?fields=id,name,owner.display_name,followers.total,images,description`
  );
  const followers = detail?.followers?.total || 0;
  const owner     = detail?.owner?.display_name || '';
  const name      = detail?.name || '';
  const imgUrl    = detail?.images?.[0]?.url || null;

  // Update existing playlist record if we own it
  await pool.query(
    `UPDATE playlists SET followers=$1, image_url=COALESCE($2,image_url), name=$3 WHERE spotify_id=$4`,
    [followers, imgUrl, name, spotifyId]
  );

  // Detect genre if not set
  const genre = await detectGenre(spotifyId).catch(() => null);
  if (genre) {
    await pool.query('UPDATE playlists SET genre=$1 WHERE spotify_id=$2 AND genre IS NULL', [genre, spotifyId]);
  }

  // Generate SEO keywords from playlist and record ranking snapshot for each tracked keyword
  const { rows: trackedKws } = await pool.query(`
    SELECT DISTINCT k.id, k.term, k.market FROM keywords k
    JOIN rank_history rh ON rh.keyword_id = k.id
    JOIN playlists p ON p.id = rh.playlist_id WHERE p.spotify_id = $1
    LIMIT 20
  `, [spotifyId]);

  return { name, followers, genre, trackedKeywords: trackedKws.length };
}

async function deepResearchGenre(genre, market, perPage = 20) {
  console.log(`[research] genre "${genre}" in ${market}`);
  const terms = getLocalizedTerms(genre, market).slice(0, 5); // top 5 terms only
  const results = [];
  for (const term of terms) {
    try {
      const res = await deepResearchKeyword(term, market, perPage);
      results.push({ term, count: res.length });
      await sleep(400);
    } catch(e) {
      console.error(`[research] genre term "${term}":`, e.message);
    }
  }
  // Also add genre to target_genres
  await pool.query(
    `INSERT INTO target_genres (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`,
    [genre.toLowerCase().trim()]
  );
  return results;
}

// ── Song Scout Engine ─────────────────────────────────────────────────────────

function computeStickinessScore(popularity, releaseDateStr) {
  const pop = Number(popularity) || 0;
  const daysOld = releaseDateStr
    ? Math.max(0, (Date.now() - new Date(releaseDateStr + (releaseDateStr.length <= 7 ? '-01' : '')).getTime()) / 86400000)
    : 365;
  // Recency: full score at day 0, zero at day 300
  const recency = Math.max(0, 100 - (daysOld / 3));
  return Math.min(100, Math.round(pop * 0.6 + recency * 0.4));
}

let scoutRunning = false;
async function scoutRefresh() {
  if (scoutRunning) { console.log('[scout] already running'); return; }
  scoutRunning = true;
  console.log('[scout] Refreshing track data from top T1/T2 playlists');
  try {
    const t1t2 = [...TIER1, ...TIER2];
    const { rows: topPlaylists } = await pool.query(`
      SELECT DISTINCT ON (cs.spotify_id)
        cs.spotify_id, cs.playlist_name, cs.followers, cs.genre, k.market
      FROM curator_snapshots cs
      JOIN keywords k ON k.id = cs.keyword_id
      WHERE k.market = ANY($1)
        AND cs.position <= 5
        AND cs.followers > 5000
      ORDER BY cs.spotify_id, cs.followers DESC
      LIMIT 20
    `, [t1t2]);

    for (const pl of topPlaylists) {
      try {
        await sleep(350);
        const data = await spotifyGet(
          `/playlists/${pl.spotify_id}/tracks?limit=20&fields=items(track(id,name,artists,album(images,release_date),popularity,duration_ms,external_urls))`
        );
        const items = data?.items || [];
        const isT1  = TIER1.includes(pl.market);
        for (const item of items) {
          const t = item?.track;
          if (!t || !t.id) continue;
          const score = computeStickinessScore(t.popularity, t.album?.release_date);
          await pool.query(`
            INSERT INTO scout_tracks
              (track_id,track_name,artist_name,artist_id,album_art,popularity,release_date,
               duration_ms,playlist_id,playlist_name,playlist_followers,market,genre,
               stickiness_score,is_t1_trending,spotify_url,discovered_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
            ON CONFLICT (track_id,playlist_id) DO UPDATE SET
              popularity=EXCLUDED.popularity, stickiness_score=EXCLUDED.stickiness_score,
              playlist_followers=EXCLUDED.playlist_followers, discovered_at=NOW()
          `, [
            t.id, t.name, t.artists?.[0]?.name||'', t.artists?.[0]?.id||'',
            t.album?.images?.[0]?.url||null, t.popularity||0,
            t.album?.release_date||null, t.duration_ms||0,
            pl.spotify_id, pl.playlist_name, pl.followers||0, pl.market,
            pl.genre||null, score, isT1,
            t.external_urls?.spotify||null,
          ]);
        }
      } catch(e) {
        console.error(`[scout] ${pl.spotify_id}:`, e.message);
      }
    }
    console.log('[scout] Refresh complete');
  } finally { scoutRunning = false; }
}

app.get('/api/scout', async (req, res) => {
  const { market, genre, min_score = 0, limit = 80 } = req.query;
  try {
    const params = [];
    const conds  = [];
    if (market)               { params.push(market);            conds.push(`market=$${params.length}`); }
    if (genre)                { params.push(`%${genre}%`);      conds.push(`genre ILIKE $${params.length}`); }
    if (Number(min_score) > 0){ params.push(Number(min_score)); conds.push(`stickiness_score>=$${params.length}`); }
    const where = conds.length ? 'WHERE ' + conds.join(' AND ') : '';
    params.push(Number(limit));
    const { rows } = await pool.query(
      `SELECT DISTINCT ON (track_id) * FROM scout_tracks ${where} ORDER BY track_id, stickiness_score DESC LIMIT $${params.length}`,
      params
    );
    if (!rows.length) scoutRefresh().catch(console.error); // lazy-load
    res.json(rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/scout/refresh', (req, res) => {
  res.json({ ok: true, message: 'Scout refresh started' });
  scoutRefresh().catch(console.error);
});

// ── Selective Crawl Endpoints ─────────────────────────────────────────────────

app.post('/api/crawl/keyword', async (req, res) => {
  const { term, market, per_page = 20 } = req.body;
  if (!term || !market) return res.status(400).json({ error: 'term and market required' });
  res.json({ ok: true, message: `Deep research started for "${term}" in ${market}` });
  deepResearchKeyword(term, market, Math.min(per_page, 50)).catch(console.error);
});

app.post('/api/crawl/playlist', async (req, res) => {
  const { spotify_id } = req.body;
  if (!spotify_id) return res.status(400).json({ error: 'spotify_id required' });
  try {
    const result = await deepResearchPlaylist(spotify_id);
    res.json({ ok: true, ...result });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/crawl/genre', async (req, res) => {
  const { genre, market, per_page = 20 } = req.body;
  if (!genre || !market) return res.status(400).json({ error: 'genre and market required' });
  res.json({ ok: true, message: `Deep research started for genre "${genre}" in ${market}` });
  deepResearchGenre(genre, market, Math.min(per_page, 50)).catch(console.error);
});

// Get crawl status
app.get('/api/crawl/status', (req, res) => {
  res.json(syncState);
});

// Trigger full crawl (manual — always runs regardless of 24h check)
app.post('/api/crawl/now', (req, res) => {
  res.json({ ok: true });
  runCrawl().catch(console.error);
});

// Expand: fetch +10 more playlists per tracked keyword
app.post('/api/crawl/expand/playlists', (req, res) => {
  res.json({ ok: true, message: '+10 playlists expansion started' });
  (async () => {
    const { rows: kws } = await pool.query(`SELECT term, market FROM keywords ORDER BY id LIMIT 30`);
    for (const kw of kws) {
      try { await deepResearchKeyword(kw.term, kw.market, 10); await sleep(500); }
      catch(e) { console.error(`[expand-pl] ${kw.term}:`, e.message); }
    }
  })().catch(console.error);
});

// Expand: add +10 auto-generated keyword variations for each tracked genre
app.post('/api/crawl/expand/keywords', (req, res) => {
  res.json({ ok: true, message: '+10 keyword expansion started' });
  (async () => {
    const { rows: genres } = await pool.query(`SELECT name FROM target_genres LIMIT 15`);
    for (const g of genres) {
      try {
        const allTerms = getLocalizedTerms(g.name, 'US');
        const newTerms = allTerms.slice(5, 15); // variations 5–15 (skip first 5 already likely tracked)
        for (const term of newTerms) {
          await pool.query(
            `INSERT INTO keywords (term, market, source) VALUES ($1,'US','auto') ON CONFLICT DO NOTHING`,
            [term]
          );
        }
        await sleep(200);
      } catch(e) { console.error(`[expand-kw] ${g.name}:`, e.message); }
    }
  })().catch(console.error);
});

// Health
app.get('/health', (req, res) => res.json({ ok: true, time: new Date().toISOString() }));

// SPA fallback
app.get('*', (req, res) =>
  res.sendFile(path.join(__dirname, 'public', 'index.html'))
);

// ─── Start ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
initDB()
  .then(() => app.listen(PORT, () => console.log(`Rankify running on :${PORT}`)))
  .catch((err) => { console.error('DB init failed:', err); process.exit(1); });
