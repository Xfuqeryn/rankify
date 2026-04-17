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

// ─── Core crawl ──────────────────────────────────────────────────────────────

async function runCrawl() {
  console.log('[crawl] starting…');
  try {
    const { rows: playlists } = await pool.query('SELECT * FROM playlists');
    const { rows: keywords } = await pool.query('SELECT * FROM keywords');

    if (!playlists.length || !keywords.length) {
      console.log('[crawl] nothing to crawl');
      return;
    }

    // group keywords by market
    const byMarket = {};
    for (const kw of keywords) {
      (byMarket[kw.market] = byMarket[kw.market] || []).push(kw);
    }

    const notionRows = [];

    for (const [market, kws] of Object.entries(byMarket)) {
      for (const kw of kws) {
        await sleep(250);

        let items = [];
        try {
          const sr = await spotifyGet(
            `/search?q=${encodeURIComponent(kw.term)}&type=playlist&market=${market}&limit=50`
          );
          items = sr?.playlists?.items || [];
        } catch (e) {
          console.error(`[crawl] search error ${kw.term}/${market}:`, e.message);
        }

        for (const playlist of playlists) {
          const idx = items.findIndex((it) => it && it.id === playlist.spotify_id);
          const position = idx === -1 ? null : idx + 1;

          // fetch live follower count
          let followers = playlist.followers;
          try {
            await sleep(250);
            const pl = await spotifyGet(
              `/playlists/${playlist.spotify_id}?fields=followers.total`
            );
            followers = pl?.followers?.total ?? followers;
          } catch (e) {
            console.error(`[crawl] followers error ${playlist.spotify_id}:`, e.message);
          }

          // previous position for alert logic
          const { rows: prevRows } = await pool.query(
            `SELECT position FROM rank_history
             WHERE playlist_id=$1 AND keyword_id=$2
             ORDER BY checked_at DESC LIMIT 1`,
            [playlist.id, kw.id]
          );
          const prevPos = prevRows[0]?.position ?? null;

          await pool.query(
            `INSERT INTO rank_history (playlist_id, keyword_id, position, followers)
             VALUES ($1, $2, $3, $4)`,
            [playlist.id, kw.id, position, followers]
          );

          // generate alerts
          if (position !== null && prevPos !== null) {
            if (position <= 3 && prevPos > 3) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message)
                 VALUES ($1, $2, 'top3', $3)`,
                [playlist.id, kw.id, `Entered top 3! Now ranked #${position}`]
              );
            }
            const diff = prevPos - position;
            if (diff >= 5) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message)
                 VALUES ($1, $2, 'rise', $3)`,
                [playlist.id, kw.id, `Jumped ${diff} spots to #${position}`]
              );
            } else if (diff <= -5) {
              await pool.query(
                `INSERT INTO alerts (playlist_id, keyword_id, type, message)
                 VALUES ($1, $2, 'drop', $3)`,
                [playlist.id, kw.id, `Dropped ${Math.abs(diff)} spots to #${position}`]
              );
            }
          }

          notionRows.push({
            playlist_name: playlist.name,
            term: kw.term,
            market: kw.market,
            position,
            followers,
          });
        }
      }
    }

    // update followers column on playlists from latest rank_history
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
      syncToNotion(notionRows).catch((e) => console.error('[notion]', e.message));
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
    const { spotifyUrl, label, notes } = req.body;
    const match = spotifyUrl?.match(/playlist\/([A-Za-z0-9]+)/);
    if (!match) return res.status(400).json({ error: 'Invalid Spotify playlist URL' });
    const sid = match[1];

    const data = await spotifyGet(
      `/playlists/${sid}?fields=id,name,owner.display_name,followers.total,images`
    );

    const { rows } = await pool.query(
      `INSERT INTO playlists (spotify_id, name, owner, followers, image_url, label, notes)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (spotify_id) DO UPDATE
         SET name=$2, owner=$3, followers=$4, image_url=$5, label=$6, notes=$7
       RETURNING *`,
      [
        data.id,
        data.name,
        data.owner?.display_name || null,
        data.followers?.total || 0,
        data.images?.[0]?.url || null,
        label || null,
        notes || null,
      ]
    );
    res.json(rows[0]);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.patch('/api/playlists/:id', async (req, res) => {
  try {
    const { label, notes } = req.body;
    const { rows } = await pool.query(
      'UPDATE playlists SET label=$1, notes=$2 WHERE id=$3 RETURNING *',
      [label, notes, req.params.id]
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
    const { rows } = await pool.query('SELECT * FROM keywords ORDER BY market, term');
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
        p.name       AS playlist_name,
        p.image_url,
        p.label,
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
