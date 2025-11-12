// index.js — Monday.com proxy server (fast cache + loose timesheets + debug)
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const FormData = require("form-data");
const compression = require("compression");
const fetch = require("node-fetch");            // Force node-fetch v2 for multipart compatibility
const bodyParser = require("body-parser");

dotenv.config();

const app = express();
app.disable("x-powered-by");

// CORS first
app.use(cors());

// gzip compression early
app.use(compression({ level: 6 }));

// Larger body limits (for base64 image uploads, legacy client)
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

  // Subitems (Jobs)
  SUBITEMS_CONTRACTOR_COLUMN_ID,
  SUBITEMS_TIMELINE_COLUMN_ID,
  SUBITEMS_JOBNUMBER_COLUMN_ID,
  SUBITEMS_DESCRIPTION_COLUMN_ID,
  SUBITEMS_EMAIL_COLUMN_ID,
  SUBITEMS_FILE_COLUMN_IDS,

  // Scope + Materials-scope status on Job subitems
  SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID,
  SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID,

  // ✅ NEW: Time allowance (job subitem)
  TIME_ALLOWANCE_COLUMN_ID,

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
  TS_CONNECT_TO_SUBITEMS_COLUMN_ID,
  TS_PHOTOS_FILE_COLUMN_ID,
  TS_JOB_COMPLETE_CHECKBOX_COLUMN_ID,

  // H&S board (job number item + link column)
  HS_BOARD_ID,
  HS_JOBNUMBER_TEXT_COLUMN_ID,
  HS_PDF_URL_COLUMN_ID,

  // Materials (parent + subitems boards)
  MATERIALS_BOARD_ID,
  MAT_JOBNUMBER_TEXT_COLUMN_ID,
  SUBITEMS_MATERIALS_BOARD_ID,
  SUBITEMS_MAT_JOBNUMBER_TEXT_COLUMN_ID,
  SUBITEMS_MAT_TITLE_TEXT_COLUMN_ID,
  SUBITEMS_MAT_NOTES_LONGTEXT_COLUMN_ID,
  SUBITEMS_MAT_NOTES_LONGTEXT_STATUS,

  // ✅ NEW (used in getMaterialsForJob): relation column id for Supplier on materials subitems board
  SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID,
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

// Safe link getter for Link columns (uses value.url if present, else falls back to text)
function cvLink(cvs, id) {
  const cv = cvs[id];
  if (!cv) return "";
  try {
    const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
    return (v && v.url) ? String(v.url) : String(cv.text || "");
  } catch {
    return String(cv.text || "");
  }
}

// materials helpers (legacy regex used elsewhere)
const SUBJOB_RE = /\b\d{4}-\d\b/;                       // e.g. 2762-5
const MAINJOB_RE = /\b(\d{4})\b/;                       // e.g. 2762
function pickMainAndSub(jobNumberText) {
  const sub = (jobNumberText.match(SUBJOB_RE) || [])[0] || "";
  const main = sub ? sub.split("-")[0] : ((jobNumberText.match(MAINJOB_RE)||[])[1] || "");
  return { main, sub };
}

// Safe text getter for CV map
function cvText(cvs, id) {
  return String(cvs[id]?.text ?? "").trim();
}
/** ----------------- PUSH: INIT (Firebase Admin + token store) ----------------- */
const admin = require("firebase-admin");

// You can either put the raw service account JSON into an env var
// FCM_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'
// or point to a local file via FCM_SERVICE_ACCOUNT_FILE='/path/to/serviceAccount.json'
function loadServiceAccount() {
  if (process.env.FCM_SERVICE_ACCOUNT_JSON) {
    return JSON.parse(process.env.FCM_SERVICE_ACCOUNT_JSON);
  }
  if (process.env.FCM_SERVICE_ACCOUNT_FILE) {
    // eslint-disable-next-line import/no-dynamic-require, global-require
    return require(process.env.FCM_SERVICE_ACCOUNT_FILE);
  }
  console.warn("⚠️  No FCM service account provided. Set FCM_SERVICE_ACCOUNT_JSON or FCM_SERVICE_ACCOUNT_FILE.");
  return null;
}

const svc = loadServiceAccount();
if (svc && !admin.apps.length) {
  admin.initializeApp({ credential: admin.credential.cert(svc) });
}

// In-memory token store: email -> Set<token>
// (Good enough for testing; move to Redis/DB for production)
const TOKENS = new Map();

function addToken(email, token) {
  const key = String(email || "").trim().toLowerCase();
  if (!key || !token) return;
  const set = TOKENS.get(key) || new Set();
  set.add(token);
  TOKENS.set(key, set);
}

async function sendToTokens(tokens, payload) {
  if (!tokens || !tokens.length) return { success: 0, error: "no_tokens" };
  try {
    const res = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: payload.notification,
      data: payload.data || {},
      android: { priority: "high" },
    });
    return res;
  } catch (e) {
    console.error("FCM send error:", e);
    return { success: 0, error: e.message || String(e) };
  }
}
async function notifyJobUpdate(subitemId, jobNumber, assignedEmails = []) {
  const payload = {
    notification: {
      title: jobNumber ? `Job ${jobNumber} Updated` : `Job Updated`,
      body: "Open the job to see new changes.",
    },
    data: {
      type: "job_update",
      subitemId: String(subitemId),
    },
    android: { priority: "high" },
  };

  for (const raw of assignedEmails) {
    const email = String(raw || "").trim().toLowerCase();
    if (!email) continue;
    const tokens = Array.from(TOKENS.get(email) || []);
    if (!tokens.length) continue;
    await sendToTokens(tokens, payload);
  }
}

// ---------------- PUSH ROUTES (register / unregister / test / debug) ----------------

// register a device token to an email (call this from the app after login)
app.post("/push/register", express.json(), (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const token = String(req.body?.token || "").trim();
    if (!email || !token) return res.status(400).json({ ok: false, error: "email and token required" });

    addToken(email, token);
    const count = (TOKENS.get(email) || new Set()).size;
    console.log("✅ registered token", { email, count });
    res.json({ ok: true, email, tokens: count });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// optional: unregister (on logout)
app.post("/push/unregister", express.json(), (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const token = String(req.body?.token || "").trim();
    if (!email || !token) return res.status(400).json({ ok: false, error: "email and token required" });

    const set = TOKENS.get(email);
    if (set) {
      set.delete(token);
      if (!set.size) TOKENS.delete(email);
    }
    res.json({ ok: true, email, tokens: (TOKENS.get(email) || new Set()).size });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// manual test push (useful to verify FCM & token)
app.post("/push/test", express.json(), async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim().toLowerCase();
    const token = String(req.body?.token || "").trim();
    const subitemId = String(req.body?.subitemId || "").trim();
    const jobNumber = String(req.body?.jobNumber || "").trim();

    let tokens = [];
    if (token) tokens = [token];
    else if (email) tokens = Array.from(TOKENS.get(email) || []);
    else return res.status(400).json({ ok: false, error: "email or token required" });

    const payload = {
      notification: {
        title: jobNumber ? `Job ${jobNumber} Updated` : "Job Updated",
        body: "Open the job to see new changes.",
      },
      data: { type: "job_update", subitemId },
      android: { priority: "high" },
    };

    const result = await sendToTokens(tokens, payload);
    res.json({ ok: true, sent_to: tokens.length, result });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// quick visibility of registered tokens
app.get("/debug/push-tokens", (_req, res) => {
  const rows = [];
  for (const [email, set] of TOKENS.entries()) rows.push({ email, tokens: set.size });
  res.json({ count: rows.length, entries: rows });
});

/** ----------------- Materials + H&S helpers ----------------- */
const SUBTOKEN_RE = /\b\d{4}-\d\b/;      // e.g. 2762-5
const MAINTOKEN_RE = /\b(\d{4})\b/;      // e.g. 2762

function splitJobTokens(jobNumRaw) {
  const subToken = (String(jobNumRaw || "").match(SUBTOKEN_RE) || [])[0] || "";
  const mainToken = subToken ? subToken.split("-")[0] : ((String(jobNumRaw || "").match(MAINTOKEN_RE) || [])[1] || "");
  return { subToken, mainToken };
}

function groupByStatus(rows) {
  const byStatus = {};
  for (const r of rows) {
    const key = r.status || "Uncategorised";
    if (!byStatus[key]) byStatus[key] = [];
    byStatus[key].push(r);
  }
  return byStatus;
}

// Pull a Link column's real URL (value.url), or fall back to text
function cvUrl(cvs, id) {
  const cv = cvs[id];
  if (!cv) return "";
  try {
    const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
    return (v && v.url) ? String(v.url) : String(cv.text || "");
  } catch {
    return String(cv.text || "");
  }
}

/**
 * Board-relation helper: tries to return readable names.
 * - Prefer column text (Monday usually renders related item titles here)
 * - Also expose linked IDs if you want to use them later
 */
function cvRelation(cvs, id) {
  const cv = cvs[id];
  if (!cv) return { text: "", ids: [] };

  // Best-effort: text often contains comma-separated related item names
  const text = String(cv.text || "").trim();

  let ids = [];
  try {
    const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
    const linked = v?.linkedPulseIds || v?.linkedPulseId || [];
    if (Array.isArray(linked)) {
      ids = linked.map((x) => String(x?.linkedPulseId ?? x)).filter(Boolean);
    }
  } catch {
    // ignore parse errors
  }

  return { text, ids };
}

/**
 * Fetch materials based on status mode:
 * - "Only Sub Task Materials": read SUBITEMS_MATERIALS_BOARD_ID; include items whose name starts with subToken (e.g. 2762-5)
 * - "Include Main Scope Materials": read MATERIALS_BOARD_ID; find parent whose name starts with mainToken (e.g. 2762);
 *   include its subitems EXCEPT those whose name starts with `${mainToken}-` (exclude subjob lines)
 *
 * NEW: Adds Supplier from board-relation column SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID (e.g. "connect_boards6")
 */
async function getMaterialsForJob(jobNumRaw, matScopeStatus) {
  if (!matScopeStatus || /no materials/i.test(matScopeStatus)) return null;

  const { subToken, mainToken } = splitJobTokens(jobNumRaw);
  const wantOnlySub = /only sub task materials/i.test(matScopeStatus);
  const wantMain    = /include main scope materials/i.test(matScopeStatus);

  const subBoardId   = SUBITEMS_MATERIALS_BOARD_ID;
  const parentBoard  = MATERIALS_BOARD_ID;

  const titleColId   = SUBITEMS_MAT_TITLE_TEXT_COLUMN_ID;
  const notesColId   = SUBITEMS_MAT_NOTES_LONGTEXT_COLUMN_ID;
  const statusColId  = SUBITEMS_MAT_NOTES_LONGTEXT_STATUS;
  const supplierColId = SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID; // ✅ new

  if (!titleColId || !notesColId || !statusColId) return null;

  // CASE A: Only Sub Task Materials
  if (wantOnlySub) {
    if (!subToken || !subBoardId) return null;

    const q = `
      query($boardId:ID!, $cursor:String){
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items { id name column_values { id text type value } }
          }
        }
      }`;
    let cursor = null;
    const rows = [];

    do {
      const d = await monday(q, { boardId: subBoardId, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of (page?.items || [])) {
        const nm = String(it.name || "");
        if (!nm.startsWith(subToken)) continue; // strict startsWith
        const cv = Object.fromEntries((it.column_values || []).map(c => [c.id, c]));

        // Supplier (relation): use text for display; keep IDs if needed later
        const supplier = supplierColId ? cvRelation(cv, supplierColId) : { text: "", ids: [] };

        rows.push({
          id: it.id,
          name: nm,
          title: cvText(cv, titleColId),
          notes: cvText(cv, notesColId),
          status: cvText(cv, statusColId) || "Uncategorised",
          supplier: supplier.text || "",        // display string
          supplierIds: supplier.ids || [],      // optional: raw IDs
        });
      }
    } while (cursor);

    return rows.length ? { mode: "Only Sub Task Materials", byStatus: groupByStatus(rows) } : null;
  }

  // CASE B: Include Main Scope Materials
  if (wantMain) {
    if (!mainToken || !parentBoard) return null;

    // 1) Find the parent item on MATERIALS_BOARD_ID whose name starts with "2762"
    const qParent = `
      query($boardId:ID!, $cursor:String){
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items {
              id
              name
              subitems {
                id
                name
                column_values { id text type value }
              }
            }
          }
        }
      }`;
    let cursor = null;
    let parent = null;

    do {
      const d = await monday(qParent, { boardId: parentBoard, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of (page?.items || [])) {
        if (String(it.name || "").startsWith(mainToken)) {
          parent = it;
          cursor = null;
          break;
        }
      }
    } while (cursor && !parent);

    if (!parent || !Array.isArray(parent.subitems)) return null;

    // 2) From that parent's subitems: include all except names that start with `${mainToken}-`
    const rows = [];
    for (const si of parent.subitems) {
      const nm = String(si.name || "");
      if (nm.startsWith(`${mainToken}-`)) continue; // exclude explicit subjob subitems

      const cv = Object.fromEntries((si.column_values || []).map(c => [c.id, c]));
      const supplier = supplierColId ? cvRelation(cv, supplierColId) : { text: "", ids: [] };

      rows.push({
        id: si.id,
        name: nm,
        title: cvText(cv, titleColId),
        notes: cvText(cv, notesColId),
        status: cvText(cv, statusColId) || "Uncategorised",
        supplier: supplier.text || "",
        supplierIds: supplier.ids || [],
      });
    }

    return rows.length ? { mode: "Include Main Scope Materials", byStatus: groupByStatus(rows) } : null;
  }

  return null;
}
// ---------- debug ----------
app.get("/debug/ping", (_req, res) => res.json({ ok: true, t: Date.now() }));
app.get("/debug/env", (_req, res) => {
  const safe = {
    PORT,
    // contractors
    CONTRACTORS_BOARD_ID,
    CONTRACTORS_EMAIL_COLUMN_ID,
    CONTRACTORS_PIN_TEXT_COLUMN_ID,
    // jobs
    JOBS_BOARD_ID,
    JOBS_ADDRESS_COLUMN_ID,
    // subitems
    SUBITEMS_CONTRACTOR_COLUMN_ID,
    SUBITEMS_TIMELINE_COLUMN_ID,
    SUBITEMS_JOBNUMBER_COLUMN_ID,
    SUBITEMS_DESCRIPTION_COLUMN_ID,
    SUBITEMS_EMAIL_COLUMN_ID,
    SUBITEMS_FILE_COLUMN_IDS,
    // scope/materials status on job subitem
    SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID,
    SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID,
    // ✅ NEW
    TIME_ALLOWANCE_COLUMN_ID,
    // timesheets
    TIMESHEETS_BOARD_ID,
    TS_DATE_COLUMN_ID,
    TS_NAME_COLUMN_ID,
    TS_START_NUM_COLUMN_ID,
    TS_FINISH_NUM_COLUMN_ID,
    TS_LUNCH_TEXT_COLUMN_ID,
    TS_JOBNUMBER_TEXT_COLUMN_ID,
    TS_TOTAL_HOURS_NUM_COLUMN_ID,
    TS_NOTES_LONGTEXT_COLUMN_ID,
    TS_PHOTOS_FILE_COLUMN_ID,
    TS_JOB_COMPLETE_CHECKBOX_COLUMN_ID,
    // H&S
    HS_BOARD_ID,
    HS_JOBNUMBER_TEXT_COLUMN_ID,
    HS_PDF_URL_COLUMN_ID,
    // materials
    MATERIALS_BOARD_ID,
    MAT_JOBNUMBER_TEXT_COLUMN_ID,
    SUBITEMS_MATERIALS_BOARD_ID,
    SUBITEMS_MAT_JOBNUMBER_TEXT_COLUMN_ID,
    SUBITEMS_MAT_TITLE_TEXT_COLUMN_ID,
    SUBITEMS_MAT_NOTES_LONGTEXT_COLUMN_ID,
    SUBITEMS_MAT_NOTES_LONGTEXT_STATUS,
    // ✅ NEW (to verify)
    SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID,
  };
  res.json(safe);
});
app.get("/debug/cache", (_req, res) => res.json({ keys: cacheKeys() }));

// --- RAW SUBITEM DEBUG: see every column id/text on a subitem ---
app.get("/debug/subitem/:id", async (req, res) => {
  try {
    const id = String(req.params.id).trim();
    const q = `
      query($id:[ID!]) {
        items(ids:$id){
          id
          name
          board { id name }
          column_values { id text type value }
        }
      }`;
    const d = await monday(q, { id: [id] });
    res.json(d?.items?.[0] || { error: "not found" });
  } catch (e) {
    res.status(500).json({ error: String(e?.message || e) });
  }
});

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

// ---------- details+HS+materials in one shot ----------
app.get("/jobs/:subitemId/details2", async (req, res) => {
  try {
    const subitemId = String(req.params.subitemId).trim();

    // 1) Pull Job subitem for: scope text, job number, materials-scope status, time allowance
    const qSub = `
      query($id: [ID!], $colIds: [String!]!) {
        items(ids: $id) {
          id
          name
          column_values(ids: $colIds) { id text value }
          board { id name }
        }
      }`;
    const colIds = [
      SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID,
      SUBITEMS_JOBNUMBER_COLUMN_ID,          // may be blank; we’ll fall back to name
      SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID,
      TIME_ALLOWANCE_COLUMN_ID,              // ✅ NEW
    ].filter(Boolean);

    const dSub = await monday(qSub, { id: [subitemId], colIds });
    const item = dSub?.items?.[0];
    if (!item) return res.json({ scope: "", timeAllowance: "", hs: null, materials: null });

    const cvMap = Object.fromEntries((item.column_values || []).map(cv => [cv.id, cv]));

    // ✅ robust text reader (uses cv.text, then value.text)
    function cvBestText(cv) {
      if (!cv) return "";
      const t = String(cv.text || "").trim();
      if (t) return t;
      try {
        const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
        const vt = String(v?.text || "").trim();
        return vt || "";
      } catch { return ""; }
    }

    const scope         = cvBestText(cvMap[SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID]);
    const timeAllowance = cvBestText(cvMap[TIME_ALLOWANCE_COLUMN_ID]);

    // Job number: prefer configured column, else extract from subitem name (e.g. "2762-5" or "2762")
    let jobNumRaw = (cvMap[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || "").trim();
    if (!jobNumRaw) {
      const m = String(item.name || "").match(/\b\d{4}(?:-\d)?\b/);
      jobNumRaw = m ? m[0] : "";
    }
    const { subToken, mainToken } = splitJobTokens(jobNumRaw);
    const matScopeStatus = (cvMap[SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID]?.text || "").trim();

    // 2) H&S link (search H&S board items by name prefix "2762-5" or fallback "2762")
    let hs = null;
    if (HS_BOARD_ID && HS_PDF_URL_COLUMN_ID) {
      const qHS = `
        query($boardId: ID!, $cursor: String) {
          boards(ids: [$boardId]) {
            items_page(limit: 100, cursor: $cursor) {
              cursor
              items {
                id
                name
                column_values { id text type value }
              }
            }
          }
        }`;
      let cursor = null;
      const wantPrefix = subToken || mainToken;

      if (wantPrefix) {
        do {
          const dHS = await monday(qHS, { boardId: HS_BOARD_ID, cursor });
          const page = dHS?.boards?.[0]?.items_page;
          cursor = page?.cursor || null;

          for (const it of (page?.items || [])) {
            const nm = String(it.name || "").trim();
            if (!nm.startsWith(wantPrefix)) continue;

            const cvs = Object.fromEntries((it.column_values || []).map(cv => [cv.id, cv]));
            const url = cvUrl(cvs, HS_PDF_URL_COLUMN_ID);
            if (url) { hs = { job: wantPrefix, url }; cursor = null; break; }
          }
        } while (cursor && !hs);
      }
    }

    // 3) Materials using the exact business rules
    const materials = await getMaterialsForJob(jobNumRaw, matScopeStatus);

    return res.json({ scope, timeAllowance, hs, materials, jobNumber: jobNumRaw, subitemId });
  } catch (e) {
    console.error("ERROR /jobs/:subitemId/details2", e);
    res.status(500).json({ error: e?.message || "Server error" });
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
    const loose = String(req.query.loose || "1") === "1"; // kept for compatibility
    const wantDebug = String(req.query.debug || "0") === "1";

    const cacheKey = `ts:${TIMESHEETS_BOARD_ID}:${nameRaw}:${jobNumberFilter}:${limit}:${strict}:${loose}`;
    const hit = cacheGet(cacheKey);
    if (hit) {
      if (wantDebug) return res.json(hit);
      const { sampleNames, ...clean } = hit;
      return res.json(clean);
    }

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

        // Worker / name filter
        const worker = TS_NAME_COLUMN_ID ? (cvs[TS_NAME_COLUMN_ID]?.text || "") : "";
        if (sampleNames.length < 50) sampleNames.push(worker);

        if (wantName) {
          const got = norm(worker);
          const match = strict
            ? got === wantName
            : got === wantName || got.includes(wantName) || wantName.includes(got);
          if (!match) continue;
        }

        // Optional job number filter
        const jobNo = TS_JOBNUMBER_TEXT_COLUMN_ID ? (cvs[TS_JOBNUMBER_TEXT_COLUMN_ID]?.text || "") : "";
        if (jobNumberFilter && jobNo.trim() !== jobNumberFilter) continue;

        // Date (ISO YYYY-MM-DD)
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
        const end4   = to4(cvs[TS_FINISH_NUM_COLUMN_ID]?.text || "");
        const totalHours = Number((cvs[TS_TOTAL_HOURS_NUM_COLUMN_ID]?.text || "").replace(",", ".")) || 0;
        const notes = cvs[TS_NOTES_LONGTEXT_COLUMN_ID]?.text || "";

        // ---------- FIXED: status from group title (no false "approved") ----------
        const groupTitleRaw = String(it.group?.title || "");
        const g = groupTitleRaw.trim().toLowerCase();

        // Default pending
        let status = "pending";
        if (g.includes("to be approved")) {
          status = "pending";
        } else if (
          g.includes("payroll processed") ||
          g.includes("approved - upcoming payroll") ||
          /^approved\b/.test(g) // matches "Approved..." but NOT "to be approved"
        ) {
          status = "approved";
        }

        items.push({
          id: it.id,
          dateISO,
          start4,
          end4,
          totalHours,
          jobNumber: jobNo,
          workerName: worker,
          notes,
          status,
          groupTitle: groupTitleRaw, // useful for debugging
        });

        if (items.length >= limit * 2) break;
      }
    } while (cursor && items.length < limit * 2);

    // Newest first by date
    items.sort((a, b) => (a.dateISO > b.dateISO ? -1 : a.dateISO < b.dateISO ? 1 : 0));

    const payload = { items: items.slice(0, limit), sampleNames };
    cacheSet(cacheKey, payload, 300); // 5 min

    if (!wantDebug) {
      const { sampleNames: _sn, ...clean } = payload;
      return res.json(clean);
    }
    return res.json(payload);
  } catch (e) {
    console.error("ERROR GET /timesheets:", e);
    res.status(500).json({ error: e.message });
  }
}); // IMPORTANT: close GET /timesheets properly// ---------- timesheet submit (create item on Monday) ----------
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
    if (TS_JOB_COMPLETE_CHECKBOX_COLUMN_ID) {
      cols[TS_JOB_COMPLETE_CHECKBOX_COLUMN_ID] = jobComplete ? "Yes" : "No";
    }

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

// ---------- upload (dual-mode: client-multipart OR legacy JSON) ----------
const multer = require("multer");
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }, // 20MB
});

app.post("/upload", upload.single("file"), async (req, res) => {
  const start = Date.now();
  try {
    // Detect legacy JSON payload (jobId + base64)
    let legacy = { buf: null, name: null, type: null, itemId: null, columnId: null };
    const isJson = !req.file && req.is("application/json") && req.body;
    if (isJson) {
      const { jobId, base64, fileName = "photo.jpg", mime = "image/jpeg", columnId } = req.body;
      if (jobId && base64) {
        const clean = String(base64).replace(/^data:[^;]+;base64,/, "");
        legacy.buf = Buffer.from(clean, "base64");
        legacy.name = fileName;
        legacy.type = mime;
        legacy.itemId = Number(jobId);
        legacy.columnId = columnId;
      }
    }

    // Unify inputs
    const itemId   = Number((req.body && req.body.itemId) || legacy.itemId);
    const columnId = (req.body && req.body.columnId) || legacy.columnId;
    const buf      = req.file ? req.file.buffer : legacy.buf;
    const fname    = req.file ? (req.file.originalname || "photo.jpg") : (legacy.name || "photo.jpg");
    const ftype    = req.file ? (req.file.mimetype   || "image/jpeg")  : (legacy.type || "image/jpeg");

    // Debug
    console.log("UPLOAD DEBUG →", {
      ct: req.headers["content-type"],
      len: req.headers["content-length"],
      hasFile: !!req.file,
      bodyKeys: Object.keys(req.body || {}),
      itemId, columnId,
      fileBytes: buf ? buf.length : 0,
    });

    // Validate
    if (!itemId || !columnId) {
      return res.status(400).json({ ok: false, code: "E_BAD_INPUT", msg: "Missing itemId/columnId" });
    }
    if (!buf || !buf.length) {
      return res.status(400).json({ ok: false, code: "E_NO_FILE", msg: "No file received (multipart 'file' or JSON 'base64')" });
    }

    // Build the multipart for Monday — choose format based on input mode
    const MONDAY_FILE_API = "https://api.monday.com/v2/file";
    let form;

    if (req.file) {
      // Path A: client sent multipart → use GraphQL operations/map
      const operations = JSON.stringify({
        query: `
          mutation ($file: File!, $item_id: Int!, $column_id: String!) {
            add_file_to_column(file: $file, item_id: $item_id, column_id: $column_id) { id }
          }`,
        variables: { file: null, item_id: itemId, column_id: columnId },
      });
      const map = JSON.stringify({ "0": ["variables.file"] });

      form = new (require("form-data"))();
      form.append("operations", operations);
      form.append("map", map);
      form.append("0", buf, { filename: fname, contentType: ftype, knownLength: buf.length });
    } else {
      // Path B: legacy JSON/base64 → Monday's legacy multipart (query + variables[file])
      const gql = `
        mutation ($file: File!) {
          add_file_to_column(item_id: ${itemId}, column_id: "${columnId}", file: $file) { id }
        }`;
      form = new (require("form-data"))();
      form.append("query", gql.trim());
      form.append("variables[file]", buf, { filename: fname, contentType: ftype, knownLength: buf.length });
    }

    // Send to Monday (include boundary headers)
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), 60_000);

    const r = await fetch(MONDAY_FILE_API, {
      method: "POST",
      headers: {
        // Monday expects the raw token (no "Bearer ")
        Authorization: process.env.MONDAY_TOKEN,
        ...(form.getHeaders ? form.getHeaders() : {}),
      },
      body: form,
      signal: ac.signal,
    }).catch((e) => { throw new Error("E_MONDAY_FETCH:" + e.message); });
    clearTimeout(timer);

    const text = await r.text();
    let json; try { json = JSON.parse(text); } catch {}

    console.log("UPLOAD DEBUG ← Monday", r.status, (text || "").slice(0, 300));

    if (!r.ok || (json && json.errors)) {
      return res.status(502).json({
        ok: false,
        code: "E_MONDAY_GRAPHQL",
        status: r.status,
        errors: (json && json.errors) || text,
      });
    }

    // ---- Auto-notify all assigned emails (no manual subscribe) ----
    try {
      // 1) Read assigned email(s) from the subitem’s email column
      let assignedEmails = [];
      if (SUBITEMS_EMAIL_COLUMN_ID) {
        const qAssigned = `
          query($id:[ID!]) {
            items(ids:$id) { column_values(ids:["${SUBITEMS_EMAIL_COLUMN_ID}"]) { text } }
          }`;
        const dAssigned = await monday(qAssigned, { id: [itemId] });
        const raw = dAssigned?.items?.[0]?.column_values?.[0]?.text || "";
        assignedEmails = raw.split(/[,;]/).map(s => s.trim()).filter(Boolean);
      }

      // 2) Pull job number for nicer title (best effort)
      let jobNumber = "";
      if (SUBITEMS_JOBNUMBER_COLUMN_ID) {
        const qJobNo = `
          query($id:[ID!], $colId:String!) {
            items(ids:$id){ name column_values(ids:[$colId]){ text } }
          }`;
        const dJobNo = await monday(qJobNo, { id: [itemId], colId: SUBITEMS_JOBNUMBER_COLUMN_ID });
        jobNumber = dJobNo?.items?.[0]?.column_values?.[0]?.text || "";
        if (!jobNumber) {
          const nm = dJobNo?.items?.[0]?.name || "";
          const m = String(nm).match(/\b\d{4}(?:-\d)?\b/);
          jobNumber = m ? m[0] : "";
        }
      }

      // 3) Fire push
      await notifyJobUpdate(itemId, jobNumber, assignedEmails);
    } catch (e) {
      console.warn("⚠️ notifyJobUpdate failed (not fatal):", e.message || String(e));
    }

    // Final response
    return res.json({
      ok: true,
      took_ms: Date.now() - start,
      result: json || text,
      file_bytes: buf.length,
    });
  } catch (e) {
    console.error("ERROR /upload:", e?.message || e);
    return res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// MONDAY WEBHOOK: handle Monday challenge OR Zapier JSON and PUSH
app.all("/monday/webhook", express.json({ type: "*/*" }), async (req, res) => {
  try {
    // --- Monday verification challenge (used when linking directly from Monday)
    const challenge =
      (req.method === "GET" && req.query?.challenge) ||
      (req.body && req.body.challenge);
    if (challenge) {
      res.set("Content-Type", "text/plain");
      return res.status(200).send(String(challenge));
    }

    // --- Normal POST body (Zapier)
    const b = req.body || {};
    const subitemId = String(
      b.item_id || b.pulseId || b.pulse_id || b.event?.pulseId || b.event?.pulse_id || ""
    ).trim();

    console.log("🔔 /monday/webhook", {
      keys: Object.keys(b || {}),
      item_id: b.item_id,
      subitemId,
    });

    if (!subitemId) {
      return res.status(200).send("ok"); // Nothing useful to process
    }

    // --- Fetch assigned emails + job number for this subitem
    async function getAssignedEmailsAndJobNumber(itemId) {
      let emails = [];
      let jobNumber = "";

      // 1. Pull assigned emails
      if (SUBITEMS_EMAIL_COLUMN_ID) {
        const q = `
          query($id:[ID!]) {
            items(ids:$id) {
              column_values(ids:["${SUBITEMS_EMAIL_COLUMN_ID}"]) { text }
            }
          }`;
        const d = await monday(q, { id: [itemId] });
        const raw = d?.items?.[0]?.column_values?.[0]?.text || "";
        emails = raw.split(/[,;]/).map((s) => s.trim()).filter(Boolean);
      }

      // 2. Get job number
      if (SUBITEMS_JOBNUMBER_COLUMN_ID) {
        const q = `
          query($id:[ID!], $colId:String!) {
            items(ids:$id){ name column_values(ids:[$colId]){ text } }
          }`;
        const d = await monday(q, { id: [itemId], colId: SUBITEMS_JOBNUMBER_COLUMN_ID });
        jobNumber = d?.items?.[0]?.column_values?.[0]?.text || "";
        if (!jobNumber) {
          const nm = d?.items?.[0]?.name || "";
          const m = String(nm).match(/\b\d{4}(?:-\d)?\b/);
          jobNumber = m ? m[0] : "";
        }
      }

      return { emails, jobNumber };
    }

    // --- Send push notification
    const { emails, jobNumber } = await getAssignedEmailsAndJobNumber(subitemId);
    await notifyJobUpdate(subitemId, jobNumber, emails);

    return res.json({
      ok: true,
      notified: emails.length,
      jobNumber,
      item_id: subitemId,
    });
  } catch (err) {
    console.error("ERROR /monday/webhook:", err?.message || err);
    return res.status(200).send("ok");
  }
});
// ---------- server ----------
const server = app.listen(Number(PORT), () => console.log("API running on :" + PORT));
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;