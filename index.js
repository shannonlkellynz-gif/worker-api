// index.js — Monday.com proxy server (fast cache + loose timesheets + debug)
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const FormData = require("form-data");
const compression = require("compression");

// If your Node version doesn't have global fetch, uncomment the next line:
// global.fetch = require("node-fetch");

dotenv.config();

const app = express();
app.disable("x-powered-by");

// CORS first
app.use(cors());

// gzip compression early
app.use(compression({ level: 6 }));

// Larger body limits (for base64 image uploads)
app.use(express.json({ limit: "25mb" }));
app.use(express.urlencoded({ limit: "25mb", extended: true }));

// ---------- tiny timing logger ----------
app.use((req, res, next) => {
  const t0 = Date.now();
  res.on("finish", () => {
    const ms = Date.now() - t0;
    console.log(`➡️  ${req.method} ${req.originalUrl} -> ${res.statusCode} in ${ms}ms`);
  });
  next();
});

// ---------- simple in-memory cache (TTL seconds) ----------
const CACHE_TTL_SECONDS = 300;
const _cache = new Map(); // key -> { expires:number, data:any, size:number }
function cacheGet(key) {
  const v = _cache.get(key);
  if (!v) return null;
  if (Date.now() > v.expires) {
    _cache.delete(key);
    return null;
  }
  return v.data;
}
function cacheSet(key, data, ttl = CACHE_TTL_SECONDS) {
  const size = typeof data === "string" ? data.length : JSON.stringify(data).length;
  _cache.set(key, { data, expires: Date.now() + ttl * 1000, size });
}
function cacheKeys() {
  const out = [];
  for (const [k, v] of _cache.entries()) {
    out.push({ key: k, ttl_ms: Math.max(0, v.expires - Date.now()), size: v.size });
  }
  return out.sort((a, b) => b.ttl_ms - a.ttl_ms);
}

const MONDAY_API = "https://api.monday.com/v2";
const MONDAY_FILE_API = "https://api.monday.com/v2/file";

const {
  PORT = "4000",
  MONDAY_TOKEN,

  // Contractors board
  CONTRACTORS_BOARD_ID,
  CONTRACTORS_EMAIL_COLUMN_ID,
  CONTRACTORS_PIN_TEXT_COLUMN_ID,

  // Jobs board
  JOBS_BOARD_ID,
  JOBS_ADDRESS_COLUMN_ID,

  // Subitems
  SUBITEMS_CONTRACTOR_COLUMN_ID,
  SUBITEMS_TIMELINE_COLUMN_ID,
  SUBITEMS_JOBNUMBER_COLUMN_ID,
  SUBITEMS_DESCRIPTION_COLUMN_ID,
  SUBITEMS_EMAIL_COLUMN_ID,
  SUBITEMS_FILE_COLUMN_IDS,

  // Timesheets
  TIMESHEETS_BOARD_ID,
  TS_DATE_COLUMN_ID,
  TS_NAME_COLUMN_ID,
  TS_START_NUM_COLUMN_ID,
  TS_FINISH_NUM_COLUMN_ID,
  TS_LUNCH_TEXT_COLUMN_ID,
  TS_JOBNUMBER_TEXT_COLUMN_ID,
  TS_TOTAL_HOURS_NUM_COLUMN_ID,
  TS_NOTES_LONGTEXT_COLUMN_ID,
  TS_JOB_COMPLETE_TEXT_COLUMN_ID,
  TS_PHOTOS_FILE_COLUMN_ID,
} = process.env;

// ---------- Monday helper (with caching for idempotent queries) ----------
async function monday(query, variables = {}, isFile = false, form) {
  const keyBase = isFile ? null : `m:${Buffer.from(query + "::" + JSON.stringify(variables)).toString("base64")}`;
  if (!isFile) {
    const hit = cacheGet(keyBase);
    if (hit) return hit;
  }
  try {
    if (isFile && form) {
      const r = await fetch(MONDAY_FILE_API, {
        method: "POST",
        headers: { Authorization: `Bearer ${MONDAY_TOKEN}`, ...(form.getHeaders?.() || {}) },
        body: form,
      });
      const j = await r.json();
      if (j.errors) throw new Error(JSON.stringify(j.errors));
      return j;
    }
    const r = await fetch(MONDAY_API, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${MONDAY_TOKEN}` },
      body: JSON.stringify({ query, variables }),
    });
    const j = await r.json();
    if (j.errors) throw new Error(JSON.stringify(j.errors));
    if (!isFile) cacheSet(keyBase, j.data);
    return j.data;
  } catch (err) {
    console.error("monday() error:", err?.message || err);
    throw err;
  }
}

// ---------- helpers ----------
function parseConnectIds(value) {
  if (!value) return [];
  try {
    const v = JSON.parse(value);
    const raw = v.linkedPulseIds || v.linkedItemIds || [];
    return raw.map((x) => String(x.linkedPulseId ?? x.linkedItemId));
  } catch {
    return [];
  }
}
function getFileColumnIds() {
  return String(SUBITEMS_FILE_COLUMN_IDS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
}
function to4(nOrStr) {
  const s = String(nOrStr ?? "").replace(/\D/g, "");
  return s ? s.padStart(4, "0").slice(0, 4) : "";
}
const norm = (s) => String(s || "").toLowerCase().replace(/\s+/g, " ").trim();

// ---------- debug ----------
app.get("/debug/ping", (_req, res) => res.json({ ok: true, t: Date.now() }));
app.get("/debug/env", (_req, res) => {
  const safe = {
    PORT,
    CONTRACTORS_BOARD_ID,
    CONTRACTORS_EMAIL_COLUMN_ID,
    CONTRACTORS_PIN_TEXT_COLUMN_ID,
    JOBS_BOARD_ID,
    JOBS_ADDRESS_COLUMN_ID,
    SUBITEMS_CONTRACTOR_COLUMN_ID,
    SUBITEMS_TIMELINE_COLUMN_ID,
    SUBITEMS_JOBNUMBER_COLUMN_ID,
    SUBITEMS_DESCRIPTION_COLUMN_ID,
    SUBITEMS_EMAIL_COLUMN_ID,
    SUBITEMS_FILE_COLUMN_IDS,
    TIMESHEETS_BOARD_ID,
    TS_DATE_COLUMN_ID,
    TS_NAME_COLUMN_ID,
    TS_START_NUM_COLUMN_ID,
    TS_FINISH_NUM_COLUMN_ID,
    TS_LUNCH_TEXT_COLUMN_ID,
    TS_JOBNUMBER_TEXT_COLUMN_ID,
    TS_TOTAL_HOURS_NUM_COLUMN_ID,
    TS_NOTES_LONGTEXT_COLUMN_ID,
    TS_JOB_COMPLETE_TEXT_COLUMN_ID,
    TS_PHOTOS_FILE_COLUMN_ID,
  };
  res.json(safe);
});
app.get("/debug/cache", (_req, res) => res.json({ keys: cacheKeys() }));

// quick sampler to see real stored names on the timesheets board
app.get("/debug/timesheets-sample", async (_req, res) => {
  try {
    const q = `
      query($boardId:ID!, $cursor:String, $colIds:[String!]!) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items { id name column_values(ids:$colIds){ id text } }
          }
        }
      }`;
    const colIds = [TS_NAME_COLUMN_ID].filter(Boolean);
    let cursor = null, out = [];
    do {
      const d = await monday(q, { boardId: TIMESHEETS_BOARD_ID, cursor, colIds });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;
      for (const it of page?.items || []) {
        const nameCv = (it.column_values || []).find((c) => c.id === TS_NAME_COLUMN_ID);
        out.push({ id: it.id, tsName: nameCv?.text || "", itemName: it.name });
        if (out.length >= 100) { cursor = null; break; }
      }
    } while (cursor);
    res.json({ count: out.length, sample: out });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------- health ----------
app.get("/health", (_req, res) => res.json({ ok: true, t: Date.now() }));

// ---------- auth ----------
app.post("/auth/login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const pinRaw = String(req.body?.pin || "").trim();
    if (!email || !/^\S+@\S+\.\S+$/.test(email) || !/^\d{4}$/.test(pinRaw)) {
      return res.status(400).json({ ok: false, error: "Invalid email or PIN format." });
    }

    const q = `
      query($boardId:ID!, $cursor:String) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items { id name column_values { id text } }
          }
        }
      }`;
    let cursor = null, contractor = null;
    do {
      const d = await monday(q, { boardId: CONTRACTORS_BOARD_ID, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;
      for (const it of page?.items || []) {
        const match = it.column_values?.some(
          (cv) => cv.id === CONTRACTORS_EMAIL_COLUMN_ID && String(cv.text || "").trim().toLowerCase() === email
        );
        if (match) { contractor = it; break; }
      }
    } while (!contractor && cursor);

    if (!contractor) return res.status(401).json({ ok: false, error: "Invalid email or PIN" });
    const cvs = contractor.column_values || [];
    let storedPin = "";
    if (CONTRACTORS_PIN_TEXT_COLUMN_ID) {
      const pinCv = cvs.find((cv) => cv.id === CONTRACTORS_PIN_TEXT_COLUMN_ID);
      storedPin = String(pinCv?.text || "").trim();
    }
    const normalized = storedPin.replace(/\D/g, "").padStart(4, "0");
    if (normalized !== pinRaw) return res.status(401).json({ ok: false, error: "Invalid email or PIN" });

    res.json({ ok: true, name: contractor.name || "" });
  } catch (e) {
    console.error("ERROR /auth/login:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- jobs (cached) ----------
app.get("/jobs/my", async (req, res) => {
  try {
    const email = String(req.query.email || "").trim().toLowerCase();
    if (!email) return res.status(400).json({ error: "email required" });

    const onDate = String(req.query.on || "");
    const includeWeekends = String(req.query.includeWeekends || "1") !== "0";
    const page = Math.max(1, parseInt(String(req.query.page || "1"), 10));
    const limit = Math.min(100, Math.max(1, parseInt(String(req.query.limit || "50"), 10)));
    const offset = (page - 1) * limit;

    const cacheKey = `jobs:${email}:${onDate}:${includeWeekends}:${page}:${limit}`;
    const hit = cacheGet(cacheKey);
    if (hit) return res.json(hit);

    const isWeekend = (iso) => {
      if (!iso) return false;
      const dt = new Date(`${iso}T12:00:00Z`);
      const dow = dt.getUTCDay();
      return dow === 0 || dow === 6;
    };

    // find contractor id
    const contractorQ = `
      query($boardId:ID!, $cursor:String){
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor:$cursor){
            cursor items{ id column_values{id text} }
          }
        }
      }`;
    let cCursor = null, contractorId = null;
    do {
      const d = await monday(contractorQ, { boardId: CONTRACTORS_BOARD_ID, cursor: cCursor });
      const pageChunk = d?.boards?.[0]?.items_page;
      cCursor = pageChunk?.cursor || null;
      for (const it of pageChunk?.items || []) {
        const match = it.column_values?.some(
          (cv) => cv.id === CONTRACTORS_EMAIL_COLUMN_ID && String(cv.text || "").trim().toLowerCase() === email
        );
        if (match) { contractorId = String(it.id); break; }
      }
    } while (!contractorId && cCursor);
    if (!contractorId) {
      const out = { items: [], total: 0, page, limit };
      cacheSet(cacheKey, out);
      return res.json(out);
    }

    const subCols = [
      SUBITEMS_CONTRACTOR_COLUMN_ID,
      SUBITEMS_TIMELINE_COLUMN_ID,
      SUBITEMS_JOBNUMBER_COLUMN_ID,
      SUBITEMS_DESCRIPTION_COLUMN_ID,
      SUBITEMS_EMAIL_COLUMN_ID,
    ].filter(Boolean);
    const addrCols = [JOBS_ADDRESS_COLUMN_ID].filter(Boolean);

    const jobsQ = `
      query($boardId:ID!, $cursor:String, $addrCols:[String!], $subCols:[String!]) {
        boards(ids: [$boardId]) {
          items_page(limit:50, cursor:$cursor){
            cursor
            items{
              id name
              column_values(ids:$addrCols){id text}
              subitems{
                id name
                column_values(ids:$subCols){id text value}
              }
            }
          }
        }
      }`;

    let jCursor = null, totalPossible = 0, collected = 0;
    const results = [];

    loopPages:
    do {
      const d = await monday(jobsQ, { boardId: JOBS_BOARD_ID, cursor: jCursor, addrCols, subCols });
      const pageChunk = d?.boards?.[0]?.items_page;
      jCursor = pageChunk?.cursor || null;

      for (const job of pageChunk?.items || []) {
        const address = job.column_values?.[0]?.text || "";

        for (const s of job.subitems || []) {
          const sCols = Object.fromEntries((s.column_values || []).map((cv) => [cv.id, cv]));
          const linkedIds = parseConnectIds(sCols[SUBITEMS_CONTRACTOR_COLUMN_ID]?.value);
          const matchByLink = linkedIds.includes(String(contractorId));
          const matchByEmail = SUBITEMS_EMAIL_COLUMN_ID
            ? (sCols[SUBITEMS_EMAIL_COLUMN_ID]?.text || "").trim().toLowerCase() === email
            : false;
          if (!(matchByLink || matchByEmail)) continue;

          let startDate = "", endDate = "";
          try {
            const tlVal = sCols[SUBITEMS_TIMELINE_COLUMN_ID]?.value;
            if (tlVal) {
              const tl = typeof tlVal === "string" ? JSON.parse(tlVal) : tlVal;
              startDate = tl?.from || "";
              endDate = tl?.to || tl?.from || "";
            }
          } catch {}

          if (onDate) {
            if (!includeWeekends && isWeekend(onDate)) continue;
            if (!(onDate >= startDate && onDate <= endDate)) continue;
          }

          totalPossible++;
          if (totalPossible > offset && collected < limit) {
            results.push({
              parentJobId: job.id,
              parentJobName: job.name,
              address,
              subitemId: s.id,
              subitemName: s.name,
              jobNumber: sCols[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || "",
              description: sCols[SUBITEMS_DESCRIPTION_COLUMN_ID]?.text || "",
              timeline: { startDate, endDate },
            });
            collected++;
          }
          if (collected >= limit && totalPossible >= offset + limit) { jCursor = null; break loopPages; }
        }
      }
    } while (jCursor);

    const out = { items: results, total: totalPossible, page, limit };
    cacheSet(cacheKey, out);
    res.json(out);
  } catch (e) {
    console.error("ERROR /jobs/my:", e);
    res.status(500).json({ error: e.message });
  }
});

// ---------- job details (cached) ----------
app.get("/jobs/:subitemId/details", async (req, res) => {
  const subitemId = String(req.params.subitemId);
  const fileColumnIds = getFileColumnIds();
  const cacheKey = `jobDetails:${subitemId}:${fileColumnIds.join(",")}`;
  const hit = cacheGet(cacheKey);
  if (hit) return res.json(hit);

  try {
    const q = `
      query($id: [ID!], $fileIds: [String!]!) {
        items(ids: $id) {
          id name updated_at created_at
          column_values(ids: $fileIds) { id text value }
        }
      }`;
    const d = await monday(q, { id: [subitemId], fileIds: fileColumnIds });
    const item = d?.items?.[0];
    if (!item) {
      const out = { item: null, files: [], filesByColumn: {}, columnIds: fileColumnIds };
      cacheSet(cacheKey, out);
      return res.json(out);
    }

    const filesByColumn = {};
    const flat = [];

    for (const cv of (item.column_values || [])) {
      let files = [];
      if (cv?.value) {
        try {
          const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
          if (v && Array.isArray(v.files)) {
            files = v.files.map((f) => {
              const assetId = f?.assetId ? String(f.assetId) : null;
              const obj = { columnId: cv.id, name: String(f?.name ?? "file"), assetId, url: f?.url || null };
              flat.push(obj);
              return { name: obj.name, assetId: obj.assetId, url: obj.url };
            });
          }
        } catch {}
      }
      filesByColumn[cv.id] = files;
    }

    const out = {
      item: { id: item.id, name: item.name },
      files: flat,
      filesByColumn,
      columnIds: fileColumnIds,
    };
    cacheSet(cacheKey, out);
    res.json(out);
  } catch (e) {
    console.error("ERROR /jobs/:subitemId/details:", e);
    res.json({
      item: null,
      files: [],
      filesByColumn: {},
      columnIds: fileColumnIds,
      _error: String(e?.message || e),
    });
  }
});

// ---------- files (cached asset lookups) ----------
app.get("/files/:assetId", async (req, res) => {
  try {
    const assetId = String(req.params.assetId).trim();
    const cacheKey = `asset:${assetId}`;
    const hit = cacheGet(cacheKey);
    if (hit) {
      if (!hit.url && !hit.public_url) return res.status(404).send("No URL available for this file.");
      res.set("Cache-Control", "private, max-age=120");
      return res.redirect(hit.public_url || hit.url);
    }

    const q = `query($ids: [ID!]!) { assets(ids: $ids) { id url public_url name file_extension } }`;
    const d = await monday(q, { ids: [assetId] });
    const a = d?.assets?.[0];
    cacheSet(cacheKey, a || {});
    if (!a || !(a.public_url || a.url)) {
      return res.status(404).send("No URL available for this file.");
    }
    res.set("Cache-Control", "private, max-age=120");
    return res.redirect(a.public_url || a.url);
  } catch (e) {
    console.error("FILE PROXY fatal error:", e?.message || e);
    res.status(500).send("Could not resolve file.");
  }
});

// ---------- timesheets (optimized + 5min cache) ----------
app.get("/timesheets", async (req, res) => {
  try {
    const nameRaw = String(req.query.name || "").trim();
    const jobNumberFilter = String(req.query.jobNumber || "").trim();
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));
    const strict = String(req.query.strict || "0") === "1";
    const loose = String(req.query.loose || "1") === "1";
    const wantDebug = String(req.query.debug || "0") === "1";

    const cacheKey = `ts:${TIMESHEETS_BOARD_ID}:${nameRaw}:${jobNumberFilter}:${limit}:${strict}:${loose}`;
    const hit = cacheGet(cacheKey);
    if (hit) {
      if (wantDebug) return res.json(hit);
      const { sampleNames, ...clean } = hit;
      return res.json(clean);
    }

    // only query columns we actually render in the app
    const colIds = [
      TS_DATE_COLUMN_ID,
      TS_NAME_COLUMN_ID,
      TS_START_NUM_COLUMN_ID,
      TS_FINISH_NUM_COLUMN_ID,
      TS_JOBNUMBER_TEXT_COLUMN_ID,
      TS_TOTAL_HOURS_NUM_COLUMN_ID,
      TS_NOTES_LONGTEXT_COLUMN_ID,
    ].filter(Boolean);

    const q = `
      query($boardId:ID!, $cursor:String, $colIds:[String!]!) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items {
              id name updated_at created_at
              group { title }
              column_values(ids: $colIds) { id text value }
            }
          }
        }
      }`;

    const wantName = norm(nameRaw);
    let cursor = null;
    const items = [];
    const sampleNames = [];

    do {
      const d = await monday(q, { boardId: TIMESHEETS_BOARD_ID, cursor, colIds });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of page?.items || []) {
        const cvs = Object.fromEntries((it.column_values || []).map(cv => [cv.id, cv]));
        const worker = TS_NAME_COLUMN_ID ? cvs[TS_NAME_COLUMN_ID]?.text || "" : "";
        if (sampleNames.length < 50) sampleNames.push(worker);

        // filter by name (lenient by default)
        if (wantName) {
          const got = norm(worker);
          const match = strict
            ? got === wantName
            : got === wantName || got.includes(wantName) || wantName.includes(got);
          if (!match) continue;
        }

        const jobNo = TS_JOBNUMBER_TEXT_COLUMN_ID ? cvs[TS_JOBNUMBER_TEXT_COLUMN_ID]?.text || "" : "";
        if (jobNumberFilter && jobNo.trim() !== jobNumberFilter) continue;

        // parse date
        let dateISO = "";
        if (TS_DATE_COLUMN_ID) {
          const val = cvs[TS_DATE_COLUMN_ID]?.value;
          if (val) {
            try {
              const parsed = typeof val === "string" ? JSON.parse(val) : val;
              dateISO = parsed?.date || "";
            } catch {}
          }
        }

        const start4 = to4(cvs[TS_START_NUM_COLUMN_ID]?.text || "");
        const end4 = to4(cvs[TS_FINISH_NUM_COLUMN_ID]?.text || "");
        const totalHours = Number((cvs[TS_TOTAL_HOURS_NUM_COLUMN_ID]?.text || "").replace(",", ".")) || 0;
        const notes = cvs[TS_NOTES_LONGTEXT_COLUMN_ID]?.text || "";
        const groupTitle = String(it.group?.title || "").toLowerCase();
        const approved = groupTitle.includes("approved") || groupTitle.includes("payroll");

        // only keep lightweight payload
        items.push({
          id: it.id,
          dateISO,
          start4,
          end4,
          totalHours,
          jobNumber: jobNo,
          workerName: worker,
          notes,
          status: approved ? "approved" : "pending",
        });

        if (items.length >= limit * 2) break; // prevent runaway pages
      }
    } while (cursor && items.length < limit * 2);

    items.sort((a, b) => (a.dateISO > b.dateISO ? -1 : a.dateISO < b.dateISO ? 1 : 0));

    const payload = { items: items.slice(0, limit), sampleNames };
    cacheSet(cacheKey, payload, 300); // keep 5min

    if (!wantDebug) {
      const { sampleNames: _sn, ...clean } = payload;
      return res.json(clean);
    }
    return res.json(payload);
  } catch (e) {
    console.error("ERROR GET /timesheets:", e);
    res.status(500).json({ error: e.message });
  }
}); // IMPORTANT: close GET /timesheets properly

// ---------- timesheet submit (create item on Monday) ----------
app.post("/timesheets", async (req, res) => {
  try {
    const {
      email,
      workerName,
      subitemId,   // optional
      jobNumber,
      date,        // ISO yyyy-mm-dd
      startNum,    // e.g. 730 or 0730
      endNum,      // e.g. 1700
      tookLunch,   // boolean
      totalHours,  // number
      jobComplete, // boolean
      notes        // string
    } = req.body || {};

    if (!email || !jobNumber || !date) {
      return res.status(400).json({ ok: false, error: "Missing required fields (email, jobNumber, date)" });
    }

    const cols = {};
    if (TS_DATE_COLUMN_ID)              cols[TS_DATE_COLUMN_ID]              = { date };
    if (TS_NAME_COLUMN_ID)              cols[TS_NAME_COLUMN_ID]              = workerName || email;
    if (TS_START_NUM_COLUMN_ID)         cols[TS_START_NUM_COLUMN_ID]         = String(startNum || "").replace(/\D/g, "");
    if (TS_FINISH_NUM_COLUMN_ID)        cols[TS_FINISH_NUM_COLUMN_ID]        = String(endNum || "").replace(/\D/g, "");
    if (TS_LUNCH_TEXT_COLUMN_ID)        cols[TS_LUNCH_TEXT_COLUMN_ID]        = tookLunch ? "Yes" : "No";
    if (TS_JOBNUMBER_TEXT_COLUMN_ID)    cols[TS_JOBNUMBER_TEXT_COLUMN_ID]    = jobNumber;
    if (TS_TOTAL_HOURS_NUM_COLUMN_ID)   cols[TS_TOTAL_HOURS_NUM_COLUMN_ID]   = Number(totalHours) || 0;
    if (TS_NOTES_LONGTEXT_COLUMN_ID)    cols[TS_NOTES_LONGTEXT_COLUMN_ID]    = String(notes || "");
    if (TS_JOB_COMPLETE_TEXT_COLUMN_ID) cols[TS_JOB_COMPLETE_TEXT_COLUMN_ID] = jobComplete ? "Yes" : "No";

    const itemName = `${workerName || email} – ${date} – ${jobNumber} – ${Number(totalHours) || 0}h`;

    const mutation = `
      mutation CreateTs($boardId: ID!, $itemName: String!, $columnVals: JSON!) {
        create_item(board_id: $boardId, item_name: $itemName, column_values: $columnVals) { id }
      }
    `;
    const data = await monday(mutation, {
      boardId: TIMESHEETS_BOARD_ID,
      itemName,
      columnVals: JSON.stringify(cols),
    });

    return res.json({ ok: true, id: data?.create_item?.id || null });
  } catch (e) {
    console.error("ERROR POST /timesheets:", e);
    return res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// ---------- upload (dual-mode: multipart OR legacy JSON base64) ----------
const multer = require("multer"); // add at top of file if not present
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB cap
});

app.post("/upload", upload.single("file"), async (req, res) => {
  const start = Date.now();
  try {
    // ---- Legacy JSON fallback (jobId + base64) ----
    let legacyBuf = null, legacyName = null, legacyType = null, legacyItemId = null, legacyColumnId = null;

    if (!req.file && req.is("application/json") && req.body) {
      const { jobId, base64, fileName = "photo.jpg", mime = "image/jpeg", columnId } = req.body;
      if (jobId && base64) {
        const cleanB64 = String(base64).replace(/^data:[^;]+;base64,/, "");
        legacyBuf = Buffer.from(cleanB64, "base64");
        legacyName = fileName;
        legacyType = mime;
        legacyItemId = jobId;
        legacyColumnId = columnId;
      }
    }

    // ---- Unify inputs (prefer multipart; else legacy) ----
    const itemId = Number((req.body && req.body.itemId) || legacyItemId);
    const columnId = (req.body && req.body.columnId) || legacyColumnId;
    const fileBuffer = req.file ? req.file.buffer : legacyBuf;
    const fileName = req.file ? (req.file.originalname || "photo.jpg") : (legacyName || "photo.jpg");
    const fileType = req.file ? (req.file.mimetype || "image/jpeg") : (legacyType || "image/jpeg");

    // --- DEBUG: see what the server actually received ---
    console.log("UPLOAD DEBUG →", {
      ct: req.headers["content-type"],
      len: req.headers["content-length"],
      hasFile: !!req.file,
      bodyKeys: Object.keys(req.body || {}),
      itemId,
      columnId,
      fileBytes: fileBuffer ? fileBuffer.length : 0,
    });

    // ---- Validate inputs (return clear 400s, no crashes) ----
    if (!itemId || !columnId) {
      return res.status(400).json({ ok: false, code: "E_BAD_INPUT", msg: "Missing itemId/columnId" });
    }
    if (!fileBuffer || !fileBuffer.length) {
      return res.status(400).json({ ok: false, code: "E_NO_FILE", msg: "No file received (multipart 'file' or JSON 'base64')" });
    }

    // ---- Build GraphQL multipart for Monday ----
    const operations = JSON.stringify({
      query: `
        mutation ($file: File!, $item_id: Int!, $column_id: String!) {
          add_file_to_column(file: $file, item_id: $item_id, column_id: $column_id) { id }
        }`,
      variables: { file: null, item_id: itemId, column_id: columnId },
    });
    const map = JSON.stringify({ "0": ["variables.file"] });

    const form = new FormData();
    form.append("operations", operations);
    form.append("map", map);
    form.append("0", fileBuffer, {
      filename: fileName,
      contentType: fileType,
      knownLength: fileBuffer.length,
    });

    // ---- Post to Monday with a hard timeout ----
    const MONDAY_FILE_API = "https://api.monday.com/v2/file";
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 60_000); // 60s

    const r = await fetch(MONDAY_FILE_API, {
      method: "POST",
      headers: { Authorization: process.env.MONDAY_TOKEN }, // note: no "Bearer "
      body: form,
      signal: ac.signal,
    }).catch((e) => {
      throw new Error("E_MONDAY_FETCH:" + e.message);
    });
    clearTimeout(timer);

    const text = await r.text();
    let json;
    try { json = JSON.parse(text); } catch {}

    console.log("UPLOAD DEBUG ← Monday", r.status, (text || "").slice(0, 300));

    if (!r.ok || (json && json.errors)) {
      return res.status(502).json({
        ok: false,
        code: "E_MONDAY_GRAPHQL",
        status: r.status,
        errors: (json && json.errors) || text,
      });
    }

    return res.json({
      ok: true,
      took_ms: Date.now() - start,
      result: json || text,
      file_bytes: fileBuffer.length,
    });
  } catch (err) {
    const msg = String(err || "");
    let code = "E_UNKNOWN";
    if (msg.includes("E_MONDAY_FETCH")) code = "E_MONDAY_FETCH";
    if (msg.includes("aborted")) code = "E_TIMEOUT";
    if (msg.includes("too large") || msg.includes("LIMIT_FILE_SIZE")) code = "E_FILE_TOO_LARGE";
    console.error("UPLOAD DEBUG ✖", code, msg);
    return res.status(500).json({ ok: false, code, msg });
  }
});

// Global multer error handler (returns JSON instead of crashing)
app.use((err, req, res, next) => {
  if (err && err.code === "LIMIT_FILE_SIZE") {
    return res.status(413).json({ ok: false, code: "E_FILE_TOO_LARGE", msg: "File exceeds 20MB" });
  }
  next(err);
});
// ---------- server ----------
const server = app.listen(Number(PORT), () => console.log("API running on :" + PORT));
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;