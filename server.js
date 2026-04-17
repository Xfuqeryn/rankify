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

// ─── Core crawl ──────────────────────────────────────────────────────────────

async function runCrawl() {
  console.log('[crawl] starting…');
  try {
    const { rows: playlists } = await pool.query('SELECT * FROM playlists');

    if (!playlists.length) {
      console.log('[crawl] no playlists');
      return;
    }

    // ── 1. Genre detection ────────────────────────────────────────────────────
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

    // ── 2. Autonomous keyword discovery (owned playlists, once per 7 days) ───
    const ownedPlaylists = playlists.filter(p => !p.is_competitor);
    const DISCOVERY_TTL  = 7 * 24 * 3600 * 1000;

    for (const playlist of ownedPlaylists) {
      const lastDisc = playlist.last_discovery ? new Date(playlist.last_discovery).getTime() : 0;
      if (Date.now() - lastDisc < DISCOVERY_TTL) {
        console.log(`[discovery] ${playlist.name}: skipping (ran recently)`);
        continue;
      }

      const candidates = discoverKeywords(playlist);
      console.log(`[discovery] ${playlist.name}: scanning ${candidates.length} candidate terms`);

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
              `INSERT INTO keywords (term, market) VALUES ($1,'US') ON CONFLICT (term, market) DO NOTHING`,
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

    // Refresh keyword list after discovery
    const { rows: allKeywords } = await pool.query('SELECT * FROM keywords');
    if (!allKeywords.length) {
      console.log('[crawl] no keywords');
      return;
    }

    // ── 3. Pre-fetch follower counts once per playlist ────────────────────────
    const followerMap = {};
    for (const playlist of playlists) {
      await sleep(350);
      try {
        const pl = await spotifyGet(`/playlists/${playlist.spotify_id}?fields=followers.total`);
        followerMap[playlist.id] = pl?.followers?.total ?? playlist.followers;
      } catch (e) {
        followerMap[playlist.id] = playlist.followers;
        console.error(`[crawl] followers ${playlist.spotify_id}:`, e.message);
      }
    }

    // ── 4. Main crawl: top 100 per keyword + curator snapshots ───────────────
    const byMarket = {};
    for (const kw of allKeywords) {
      (byMarket[kw.market] = byMarket[kw.market] || []).push(kw);
    }

    const notionRows = [];

    for (const [market, kws] of Object.entries(byMarket)) {
      for (const kw of kws) {

        // Two calls of 50 = top 100
        let items = [];
        for (const offset of [0, 50]) {
          await sleep(350);
          try {
            const sr = await spotifyGet(
              `/search?q=${encodeURIComponent(kw.term)}&type=playlist&market=${market}&limit=50&offset=${offset}`
            );
            items.push(...(sr?.playlists?.items || []).filter(Boolean));
          } catch (e) {
            console.error(`[crawl] search ${kw.term}/${market} +${offset}:`, e.message);
          }
        }

        // Store fresh top-20 curator snapshots
        await pool.query('DELETE FROM curator_snapshots WHERE keyword_id=$1', [kw.id]);

        for (let i = 0; i < Math.min(20, items.length); i++) {
          const it = items[i];
          if (!it) continue;

          let followers   = 0;
          let description = (it.description || '').slice(0, 1000);

          // Full fetch (followers + description) for top 10 only
          if (i < 10) {
            await sleep(350);
            try {
              const full = await spotifyGet(
                `/playlists/${it.id}?fields=followers.total,description`
              );
              followers   = full?.followers?.total || 0;
              description = (full?.description || description).slice(0, 1000);
            } catch (e) {
              console.error(`[crawl] detail ${it.id}:`, e.message);
            }
          }

          try {
            await pool.query(`
              INSERT INTO curator_snapshots
                (keyword_id, position, spotify_id, playlist_name, owner, followers, description, contact_info)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            `, [
              kw.id, i + 1, it.id,
              (it.name || '').slice(0, 500),
              it.owner?.display_name || null,
              followers,
              description,
              JSON.stringify(extractContacts(description)),
            ]);
          } catch (e) {
            console.error('[crawl] snapshot insert:', e.message);
          }
        }

        // Record positions for all tracked playlists
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
          }

          notionRows.push({ playlist_name: playlist.name, term: kw.term, market: kw.market, position, followers });
        }
      }
    }

    // ── 5. Sync follower counts to playlists table ────────────────────────────
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

    console.log('[crawl] complete');
  } catch (e) {
    console.error('[crawl] error:', e);
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

// daily 06:00 UTC
cron.schedule('0 6 * * *', runCrawl, { timezone: 'UTC' });

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

// Keywords
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

// Curator leads — contact info extracted from top-20 playlists
app.get('/api/leads', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT DISTINCT ON (cs.spotify_id)
        cs.spotify_id,
        cs.playlist_name,
        cs.owner,
        cs.followers,
        cs.contact_info,
        cs.position,
        k.term,
        k.market,
        cs.checked_at
      FROM curator_snapshots cs
      JOIN keywords k ON k.id = cs.keyword_id
      WHERE cs.contact_info IS NOT NULL
        AND cs.contact_info::text NOT IN ('{}','null','""')
        AND cs.contact_info != '{}'::jsonb
      ORDER BY cs.spotify_id, cs.position ASC
    `);
    res.json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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

// Trigger crawl
app.post('/api/crawl/now', (req, res) => {
  res.json({ ok: true });
  runCrawl().catch(console.error);
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
