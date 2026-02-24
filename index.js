// index.js — Monday.com proxy server (fast cache + loose timesheets + debug)
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const FormData = require("form-data");
const compression = require("compression");
const fetch = require("node-fetch");            // Force node-fetch v2 for multipart compatibility
const bodyParser = require("body-parser");

dotenv.config();
console.log("ENV TEST:", process.env.SUBITEMS_SIGNED_HS_FILE_COL_ID);

const app = express();
app.disable("x-powered-by");

// CORS first
app.use(cors());

// gzip compression early
app.use(compression({ level: 6 }));

// Larger body limits (for base64 image uploads, legacy client)
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

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
CONTRACTORS_LOGIN_NAME_COLUMN_ID,

  // Jobs board
  JOBS_BOARD_ID,
  JOBS_ADDRESS_COLUMN_ID,

  // Subitems (Jobs)
// Subitems (Jobs)
  SUBITEMS_BOARD_ID, // ✅ NEW: Jobs subitems board id (you said 1888971901)
  SUBITEMS_CONTRACTOR_COLUMN_ID,
  SUBITEMS_TIMELINE_COLUMN_ID,
  SUBITEMS_JOBNUMBER_COLUMN_ID,
  SUBITEMS_DESCRIPTION_COLUMN_ID,
  SUBITEMS_EMAIL_COLUMN_ID,
  SUBITEMS_FILE_COLUMN_IDS,
  SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID,
  SUBITEMS_SUBCONTRACTOR_TEXT_COLUMN_ID,

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

  // ✅ NEW: H&S field mapping + signed PDF destination (job subitem)
  HS_HEADER_COLUMN_IDS,
  HS_HAZARD_COLUMN_IDS,
  SUBITEMS_SIGNED_HS_FILE_COL_ID,

  // Materials (parent + subitems boards)
  MATERIALS_BOARD_ID,
  MAT_JOBNUMBER_TEXT_COLUMN_ID,
  SUBITEMS_MATERIALS_BOARD_ID,
  SUBITEMS_MAT_JOBNUMBER_TEXT_COLUMN_ID,
  SUBITEMS_MAT_TITLE_TEXT_COLUMN_ID,
  SUBITEMS_MAT_NOTES_LONGTEXT_COLUMN_ID,
  SUBITEMS_MAT_NOTES_LONGTEXT_STATUS,

   HAZARD_REGISTER_BOARD_ID,
  HS_HAZARD_REGISTER_CONNECT_COL_ID, // ✅ NEW: connect_boards column on HS hazard subitems
  HZ_RISK_COL_ID,
  HZ_INITIAL_RISK_COL_ID,
  HZ_CONTROLS_COL_ID,
  HZ_RESIDUAL_RISK_COL_ID,

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
headers: { Authorization: MONDAY_TOKEN, ...(form.getHeaders?.() || {}) },
        body: form,
      });
      const j = await r.json();
      if (j.errors) throw new Error(JSON.stringify(j.errors));
      return j;
    }
    const r = await fetch(MONDAY_API, {
      method: "POST",
headers: { "Content-Type": "application/json", Authorization: MONDAY_TOKEN },
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
function nameKey(s) {
  // "Winston Wiggill" -> "winstonw"
  // "Winston W" -> "winstonw"
  const t = String(s || "").trim().toLowerCase();
  if (!t) return "";
  const parts = t.split(/\s+/).filter(Boolean);
  const first = parts[0] || "";
  const lastInitial = (parts[1] || "").slice(0, 1);
  return (first + lastInitial).replace(/[^a-z0-9]/g, "");
}
// ✅ ADD THIS HERE
async function getContractorNameById(contractorId) {
  const q = `
    query($ids:[ID!]) {
      items(ids:$ids) { id name }
    }`;
  const d = await monday(q, { ids: [String(contractorId)] });
  return String(d?.items?.[0]?.name || "").trim();
}

async function getItemNamesByIds(ids = []) {
  const clean = Array.from(new Set((ids || []).map(String).filter(Boolean)));
  if (!clean.length) return [];

  const q = `
    query($ids:[ID!]) {
      items(ids:$ids) { id name }
    }`;
  const d = await monday(q, { ids: clean });
  return (d?.items || [])
    .map((it) => String(it?.name || "").trim())
    .filter(Boolean);
}
function cleanPin4(pinRaw) {
  return String(pinRaw || "").replace(/\D/g, "").padStart(4, "0").slice(0, 4);
}


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
// Only return columns that actually have data
function cvHasData(cv) {
  if (!cv) return false;

  const text = String(cv.text || "").trim();
  if (text) return true;

  // Some columns store values in JSON (e.g. date, people, status, dropdown)
  try {
    const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
    if (!v) return false;

    // Common patterns that indicate "has something"
    if (typeof v === "string") return v.trim().length > 0;
    if (typeof v === "number") return true;
    if (typeof v === "boolean") return true;

    if (v.date) return true;                    // date column
    if (v.from || v.to) return true;            // timeline-like
    if (v.url) return true;                     // link
    if (Array.isArray(v.personsAndTeams) && v.personsAndTeams.length) return true;
    if (Array.isArray(v.labels) && v.labels.length) return true;
    if (Array.isArray(v.files) && v.files.length) return true;

    // Fallback: any keys at all
    return Object.keys(v).length > 0;
  } catch {
    return false;
  }
}

/* =====================================================================
   ADDITIONS FOR H&S SIGN-OFF (DO NOT MODIFY EXISTING CODE ABOVE)
   ===================================================================== */
// ---------- Resolve TA PDF from HS board (files column) ----------
async function getHsTaPdfAssetForJobPrefix(jobPrefix) {
  const filesColId = "files"; // TA Doc column id on HS board
  const q = `
    query($boardId: ID!, $cursor: String) {
      boards(ids: [$boardId]) {
        items_page(limit: 100, cursor: $cursor) {
          cursor
          items {
            id
            name
            column_values(ids:["${filesColId}"]) { id text value }
          }
        }
      }
    }`;

  let cursor = null;

  do {
    const d = await monday(q, { boardId: HS_BOARD_ID, cursor, _bust: Date.now() });
    const page = d?.boards?.[0]?.items_page;
    cursor = page?.cursor || null;

    for (const it of (page?.items || [])) {
      const nm = String(it.name || "").trim();
      if (!nm.startsWith(String(jobPrefix || "").trim())) continue;

      const cv = it.column_values?.[0];
      const v = cv?.value ? (typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value) : null;
      const files = Array.isArray(v?.files) ? v.files : [];

      // pick PDF only (best)
      const pdf = files.find(f => String(f?.name || "").toLowerCase().endsWith(".pdf"));
      if (!pdf?.assetId) {
        throw new Error(`HS item ${it.id} matched '${jobPrefix}' but no PDF found in TA Doc files column`);
      }

      return {
        hsItemId: String(it.id),
        hsItemName: nm,
        assetId: String(pdf.assetId),
        fileName: String(pdf.name || "TA.pdf"),
      };
    }
  } while (cursor);

  throw new Error(`No HS item found on HS_BOARD_ID starting with '${jobPrefix}'`);
}

async function downloadMondayAssetToBuffer(assetId) {
  const q = `query($ids: [ID!]!) { assets(ids: $ids) { id url public_url name file_extension } }`;
  const d = await monday(q, { ids: [String(assetId)] });
  const a = d?.assets?.[0];

  const url = a?.public_url || a?.url;
  if (!url) throw new Error(`Asset ${assetId} has no public_url/url`);

  const r = await fetch(url);
  if (!r.ok) throw new Error(`Failed to download asset ${assetId}: ${r.status}`);
  const arr = await r.arrayBuffer();
  return Buffer.from(arr);
}

// ---------- Combine: existing TA PDF + your sign-off page ----------
async function appendSignoffPageToExistingPdf(existingPdfBuffer, signoffPdfBuffer) {
  const { PDFDocument } = require("pdf-lib");

  const src = await PDFDocument.load(existingPdfBuffer);
  const sign = await PDFDocument.load(signoffPdfBuffer);

  const out = await PDFDocument.create();

  const srcPages = await out.copyPages(src, src.getPageIndices());
  srcPages.forEach(p => out.addPage(p));

  const signPages = await out.copyPages(sign, sign.getPageIndices());
  signPages.forEach(p => out.addPage(p));

  const bytes = await out.save();
  return Buffer.from(bytes);
}

// ---------- Fetch TA PDF from H&S Docs board for a job (by job number prefix) ----------
async function fetchTaPdfBufferForSubitem(subitemId) {
  // 1) Get job number from the job subitem
  const qSub = `
    query($id:[ID!], $colIds:[String!]) {
      items(ids:$id){
        id
        name
        column_values(ids:$colIds){ id text value }
      }
    }`;

  const colIds = [SUBITEMS_JOBNUMBER_COLUMN_ID].filter(Boolean);
  const dSub = await monday(qSub, { id: [String(subitemId)], colIds });
  const jobSub = dSub?.items?.[0];
  if (!jobSub) throw new Error("Could not find job subitem to get job number.");

  let jobNumRaw = "";
  if (SUBITEMS_JOBNUMBER_COLUMN_ID) {
    const cv = (jobSub.column_values || []).find(c => c.id === SUBITEMS_JOBNUMBER_COLUMN_ID);
    jobNumRaw = String(cv?.text || "").trim();
  }
  if (!jobNumRaw) {
    const m = String(jobSub.name || "").match(/\b\d{4}(?:-\d+)?\b/);
    jobNumRaw = m ? m[0] : "";
  }

  const { subToken, mainToken } = splitJobTokens(jobNumRaw);
  const wantPrefix = subToken || mainToken;
  if (!wantPrefix) throw new Error("Could not derive job number prefix to locate H&S doc.");

  if (!HS_BOARD_ID) throw new Error("HS_BOARD_ID missing.");
  const TA_FILE_COL_ID = String(process.env.HS_TA_FILE_COLUMN_ID || "files").trim();

  // 2) Find matching H&S item by name prefix
  const qHSFind = `
    query($boardId: ID!, $cursor: String) {
      boards(ids: [$boardId]) {
        items_page(limit: 100, cursor: $cursor) {
          cursor
          items { id name }
        }
      }
    }`;

  let cursor = null;
  let hsItemId = null;

  do {
    const d = await monday(qHSFind, { boardId: HS_BOARD_ID, cursor, _bust: Date.now() });
    const page = d?.boards?.[0]?.items_page;
    cursor = page?.cursor || null;

    for (const it of (page?.items || [])) {
      const nm = String(it.name || "").trim();
      if (!nm.startsWith(wantPrefix)) continue;
      hsItemId = String(it.id);
      cursor = null;
      break;
    }
  } while (cursor && !hsItemId);

  if (!hsItemId) throw new Error(`Could not find H&S item starting with ${wantPrefix}.`);

  // 3) Pull the TA Doc file column from that H&S item (assetId lives in value.files[])
  const qHSFile = `
    query($id:[ID!], $colIds:[String!]) {
      items(ids:$id){
        id
        name
        column_values(ids:$colIds){ id text value }
      }
    }`;

  const dFile = await monday(qHSFile, { id: [hsItemId], colIds: [TA_FILE_COL_ID] });
  const hs = dFile?.items?.[0];
  const cv = hs?.column_values?.[0];

  let files = [];
  try {
    const v = typeof cv?.value === "string" ? JSON.parse(cv.value) : cv?.value;
    files = Array.isArray(v?.files) ? v.files : [];
  } catch {}

  if (!files.length) throw new Error(`No files found in TA Doc column (${TA_FILE_COL_ID}) on H&S item ${hsItemId}.`);

  // Prefer PDF
  const pdfFile = files.find(f => /\.pdf$/i.test(String(f?.name || ""))) || files[0];
  const assetId = pdfFile?.assetId ? String(pdfFile.assetId) : null;
  if (!assetId) throw new Error("Could not read assetId for TA PDF from Monday file column.");

  // 4) Resolve asset -> public_url/url then download bytes
  const qAsset = `query($ids: [ID!]!) { assets(ids: $ids) { id url public_url name file_extension } }`;
  const dAsset = await monday(qAsset, { ids: [assetId] });
  const a = dAsset?.assets?.[0];
  const dlUrl = a?.public_url || a?.url;
  if (!dlUrl) throw new Error("Asset has no url/public_url.");

  const r = await fetch(dlUrl);
  if (!r.ok) throw new Error(`Failed to download TA PDF. HTTP ${r.status}`);
  const arr = await r.arrayBuffer();
  return { buffer: Buffer.from(arr), jobNumRaw };
}

// ---------- Stamp worker signature + name + datetime onto page 1 of an existing TA PDF ----------
async function stampWorkerSignoffOnTaPdf({ taPdfBuffer, workerName, signedAtISO, signaturePngBase64 }) {
  const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");

  const pdfDoc = await PDFDocument.load(taPdfBuffer);
  const pages = pdfDoc.getPages();
  if (!pages.length) throw new Error("TA PDF has no pages.");

  const page1 = pages[0];
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  // Format in NZ time
  const dtNZ = new Intl.DateTimeFormat("en-NZ", {
    timeZone: "Pacific/Auckland",
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(new Date(signedAtISO));

  // -------------------------
  // 🔧 POSITION (tweak these)
  // -------------------------
  // -------------------------
// 🔧 LANDSCAPE POSITIONING
// about 1/3 down the page
// -------------------------

const { width, height } = page1.getSize();

// One-line layout sitting in the sign-on band
const BASELINE_Y = height * 0.67;      // tweak 0.64–0.70 if needed

// Signature (smaller)
const X_SIG = 70;
const SIG_W = 120;
const SIG_H = 28;

// pdf-lib uses bottom-left origin; image y is bottom of image
const Y_SIG = BASELINE_Y - SIG_H + 6;

// Text on the same baseline
const Y_TEXT = BASELINE_Y - 8;
const X_NAME = X_SIG + SIG_W + 16;
const X_DATE = width - 210;            // keep to the right (adjust if needed)

  // Signature image
  if (signaturePngBase64) {
    const pngBytes = Buffer.from(
      String(signaturePngBase64).replace(/^data:image\/png;base64,/, ""),
      "base64"
    );
    const png = await pdfDoc.embedPng(pngBytes);

    // Fit into SIG_W x SIG_H
    const scale = Math.min(SIG_W / png.width, SIG_H / png.height);
    const w = png.width * scale;
    const h = png.height * scale;

    page1.drawImage(png, {
      x: X_SIG,
      y: Y_SIG,
      width: w,
      height: h,
    });
  }

// Name + datetime (same line)
page1.drawText(String(workerName || "").trim(), {
  x: X_NAME,
  y: Y_TEXT,
  size: 10,
  font: bold,
  color: rgb(0, 0, 0),
});

page1.drawText(String(dtNZ), {
  x: X_DATE,
  y: Y_TEXT,
  size: 10,
  font,
  color: rgb(0, 0, 0),
});

  const outBytes = await pdfDoc.save();
  return Buffer.from(outBytes);
}
// ---------- Upload file to Monday file column (subitem-safe, proven format) ----------
async function uploadFileToMondayColumn({ itemId, columnId, fileName, buffer }) {
  const gql = `
    mutation ($file: File!) {
      add_file_to_column(item_id: ${Number(itemId)}, column_id: "${String(columnId)}", file: $file) { id }
    }`;

  const form = new (require("form-data"))();
  form.append("query", gql.trim());

  // ✅ This is the same pattern your /upload legacy path uses (known-good)
  form.append("variables[file]", buffer, {
    filename: fileName,
    contentType: "application/pdf",
    knownLength: buffer.length,
  });

  // Use your existing monday() helper in "file" mode
  await monday(null, null, true, form);
}

// ---------- Build signed H&S PDF ----------
async function buildSignedHsPdf({
  jobNumber,
  workerName,
  signedAtISO,
  signaturePngBase64,
  additionalHazards = [], // <- add this
}) {
  const { PDFDocument, StandardFonts, rgb } = require("pdf-lib");

  const pdfDoc = await PDFDocument.create();
  const font = await pdfDoc.embedFont(StandardFonts.Helvetica);
  const bold = await pdfDoc.embedFont(StandardFonts.HelveticaBold);

  const A4 = { w: 595, h: 842 };
  const M = { l: 50, r: 50, t: 50, b: 50 };
  const contentW = A4.w - M.l - M.r;

  const COLORS = {
    text: rgb(0.13, 0.13, 0.13),
    line: rgb(0.78, 0.78, 0.78),
    headerLine: rgb(0, 0, 0),
    tableHeaderFill: rgb(0.95, 0.95, 0.95),
    lightFill: rgb(0.98, 0.98, 0.98),
  };

  const S = {
    h1: 18,
    h2: 12,
    body: 10.5,
    small: 9,
  };

  let page = pdfDoc.addPage([A4.w, A4.h]);
  let y = A4.h - M.t;

  const ensureSpace = (needed) => {
    if (y - needed < M.b) {
      page = pdfDoc.addPage([A4.w, A4.h]);
      y = A4.h - M.t;
      return true;
    }
    return false;
  };

  const drawText = (text, x, y, size = S.body, isBold = false) => {
    page.drawText(String(text ?? ""), {
      x,
      y,
      size,
      font: isBold ? bold : font,
      color: COLORS.text,
    });
  };

  const drawHRule = (yPos, thickness = 1) => {
    page.drawLine({
      start: { x: M.l, y: yPos },
      end: { x: A4.w - M.r, y: yPos },
      thickness,
      color: COLORS.headerLine,
    });
  };

  const drawBox = (x, yTop, w, h, { fill, border = true, dashed = false } = {}) => {
    page.drawRectangle({
      x,
      y: yTop - h,
      width: w,
      height: h,
      color: fill,
      borderColor: border ? COLORS.line : undefined,
      borderWidth: border ? 1 : 0,
      borderDashArray: dashed ? [4, 4] : undefined,
    });
  };

  const drawSectionTitle = (title) => {
    ensureSpace(30);
    drawText(title, M.l, y, S.h2, true);
    y -= 12;
    page.drawLine({
      start: { x: M.l, y },
      end: { x: A4.w - M.r, y },
      thickness: 1,
      color: COLORS.line,
    });
    y -= 16;
  };

  // Simple two-column key/value table
  const drawKeyValueTable = (rows) => {
    const rowH = 20;
    const tableH = rows.length * rowH;
    ensureSpace(tableH + 10);

    const x = M.l;
    const w = contentW;
    const col1 = 160; // label column
    const col2 = w - col1;

    // Outer border
    drawBox(x, y, w, tableH, { fill: undefined, border: true });

    // Vertical divider
    page.drawLine({
      start: { x: x + col1, y },
      end: { x: x + col1, y: y - tableH },
      thickness: 1,
      color: COLORS.line,
    });

    rows.forEach((r, i) => {
      const rowTop = y - i * rowH;

      // header-ish fill for label cell
      drawBox(x, rowTop, col1, rowH, { fill: COLORS.tableHeaderFill, border: false });
      // light fill for value cell
      drawBox(x + col1, rowTop, col2, rowH, { fill: COLORS.lightFill, border: false });

      // row line
      page.drawLine({
        start: { x, y: rowTop - rowH },
        end: { x: x + w, y: rowTop - rowH },
        thickness: 1,
        color: COLORS.line,
      });

      drawText(r.label, x + 8, rowTop - 14, S.body, true);
      drawText(r.value, x + col1 + 8, rowTop - 14, S.body, false);
    });

    y -= tableH + 18;
  };

  // Basic paragraph wrap (good enough for declarations)
  const drawWrappedParagraph = (text, { maxWidth, lineHeight }) => {
    const words = String(text ?? "").split(/\s+/);
    let line = "";
    const lines = [];

    const measure = (str) => font.widthOfTextAtSize(str, S.body);

    words.forEach((w) => {
      const test = line ? `${line} ${w}` : w;
      if (measure(test) <= maxWidth) {
        line = test;
      } else {
        if (line) lines.push(line);
        line = w;
      }
    });
    if (line) lines.push(line);

    const needed = lines.length * lineHeight + 8;
    ensureSpace(needed);

    lines.forEach((ln) => {
      drawText(ln, M.l, y, S.body, false);
      y -= lineHeight;
    });

    y -= 10;
  };

  // Hazards table (2 columns)
  const drawHazardsTable = (hazards) => {
    const rowH = 22;
    const headerH = 22;
    const col1 = Math.floor(contentW * 0.45);
    const col2 = contentW - col1;

    // header + rows
    const totalH = headerH + hazards.length * rowH;
    ensureSpace(totalH + 10);

    const x = M.l;
    const tableTop = y;

    // Header fill
    drawBox(x, tableTop, contentW, headerH, { fill: COLORS.tableHeaderFill, border: true });
    // Column divider in header + body
    page.drawLine({
      start: { x: x + col1, y: tableTop },
      end: { x: x + col1, y: tableTop - totalH },
      thickness: 1,
      color: COLORS.line,
    });

    drawText("Hazard", x + 8, tableTop - 15, S.body, true);
    drawText("Control", x + col1 + 8, tableTop - 15, S.body, true);

    // Rows
    hazards.forEach((h, i) => {
      const rowTop = tableTop - headerH - i * rowH;

      drawBox(x, rowTop, contentW, rowH, { fill: COLORS.lightFill, border: true });

      const hazardText = String(h?.hazard ?? "");
      const controlText = String(h?.control ?? "");

      // (No fancy wrap here to keep it simple; if you want wrap, we can add it)
      drawText(hazardText.slice(0, 90), x + 8, rowTop - 15, S.small, false);
      drawText(controlText.slice(0, 110), x + col1 + 8, rowTop - 15, S.small, false);
    });

    y -= totalH + 18;
  };

  // -------------------------
  // DOCUMENT START
  // -------------------------

  // Title
  drawText("Health & Safety Sign-Off", M.l, y, S.h1, true);
  y -= 18;
  drawText("Asset Improvements & Maintenance", M.l, y, S.body, false);
  y -= 12;
  drawHRule(y, 2);
  y -= 22;

  // Job Details
  drawSectionTitle("Job Details");
  drawKeyValueTable([
    { label: "Job Number", value: jobNumber || "-" },
    { label: "Worker Name", value: workerName || "-" },
    { label: "Date Signed", value: signedAtISO || "-" },
  ]);

  // Declaration
  drawSectionTitle("Worker Declaration");
  drawWrappedParagraph(
    "I confirm that I have reviewed the Task Analysis / SWMS hazards for this job and will follow the required control measures and safety procedures at all times while on site.",
    { maxWidth: contentW, lineHeight: 14 }
  );

  // Additional Hazards (optional)
  if (additionalHazards?.length) {
    drawSectionTitle("Additional Hazards Identified");
    drawHazardsTable(additionalHazards);
  }

  // Signature
  drawSectionTitle("Worker Signature");

  ensureSpace(170);

  // Signature box
  const sigBoxW = 340;
  const sigBoxH = 140;
  drawBox(M.l, y, sigBoxW, sigBoxH, { fill: undefined, border: true, dashed: true });
  drawText("Signature:", M.l, y + 8, S.body, true); // sits just above box visually

  if (signaturePngBase64) {
    const pngBytes = Buffer.from(
      signaturePngBase64.replace(/^data:image\/png;base64,/, ""),
      "base64"
    );
    const png = await pdfDoc.embedPng(pngBytes);

    // Fit image into box with padding
    const pad = 10;
    const maxW = sigBoxW - pad * 2;
    const maxH = sigBoxH - pad * 2;

    const scale = Math.min(maxW / png.width, maxH / png.height);

    const imgW = png.width * scale;
    const imgH = png.height * scale;

    page.drawImage(png, {
      x: M.l + pad,
      y: y - sigBoxH + pad,
      width: imgW,
      height: imgH,
    });
  }

  y -= sigBoxH + 20;

  // Footer
  drawText("Generated via Worker App • Asset Improvements & Maintenance", M.l, M.b - 10, S.small, false);

  const pdfBytes = await pdfDoc.save();
  return Buffer.from(pdfBytes);
}

// ----------------- PUSH: INIT (Firebase Admin + token store) ----------------- */
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
// In-memory token store: contractorId -> Set<token>
const TOKENS = new Map();

function addToken(contractorId, token) {
  const key = String(contractorId || "").trim();
  if (!key || !token) return;
  const set = TOKENS.get(key) || new Set();
  set.add(token);
  TOKENS.set(key, set);
}
// Resolve contractor board item IDs -> email addresses (from Contractors board)
async function getContractorEmailsByIds(contractorIds = []) {
  const ids = Array.from(new Set((contractorIds || []).map((x) => String(x || "").trim()).filter(Boolean)));
  if (!ids.length) return [];

  // Fast path: if we ever expand, we can cache this later
  const q = `
    query($ids:[ID!]) {
      items(ids:$ids) {
        id
        name
        column_values(ids:["${CONTRACTORS_EMAIL_COLUMN_ID}"]) { id text }
      }
    }`;

  const d = await monday(q, { ids });
  const emails = [];
  for (const it of (d?.items || [])) {
    const raw = String(it?.column_values?.[0]?.text || "").trim().toLowerCase();
    if (raw && /^\S+@\S+\.\S+$/.test(raw)) emails.push(raw);
  }
  return Array.from(new Set(emails));
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
async function notifyJobUpdate(subitemId, jobNumber, assignedContractorIds = []) {
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

  for (const raw of assignedContractorIds) {
    const contractorId = String(raw || "").trim();
    if (!contractorId) continue;

    const tokens = Array.from(TOKENS.get(contractorId) || []);
    if (!tokens.length) continue;

    await sendToTokens(tokens, payload);
  }
}

// ---------------- PUSH ROUTES (register / unregister / test / debug) ----------------

// register a device token to an email (call this from the app after login)
app.post("/push/register", express.json(), (req, res) => {
  try {
    const contractorId = String(req.body?.contractorId || "").trim();
    const token = String(req.body?.token || "").trim();
    if (!contractorId || !token) {
      return res.status(400).json({ ok: false, error: "contractorId and token required" });
    }

    addToken(contractorId, token);
    const count = (TOKENS.get(contractorId) || new Set()).size;
    console.log("✅ registered token", { contractorId, count });
    res.json({ ok: true, contractorId, tokens: count });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// optional: unregister (on logout)
app.post("/push/unregister", express.json(), (req, res) => {
  try {
    const contractorId = String(req.body?.contractorId || "").trim();
    const token = String(req.body?.token || "").trim();
    if (!contractorId || !token) {
      return res.status(400).json({ ok: false, error: "contractorId and token required" });
    }

    const set = TOKENS.get(contractorId);
    if (set) {
      set.delete(token);
      if (!set.size) TOKENS.delete(contractorId);
    }
    res.json({ ok: true, contractorId, tokens: (TOKENS.get(contractorId) || new Set()).size });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || "Server error" });
  }
});

// manual test push (useful to verify FCM & token)
app.post("/push/test", express.json(), async (req, res) => {
  try {
    const contractorId = String(req.body?.contractorId || "").trim();
    const token = String(req.body?.token || "").trim();
    const subitemId = String(req.body?.subitemId || "").trim();
    const jobNumber = String(req.body?.jobNumber || "").trim();

    let tokens = [];
    if (token) {
      tokens = [token];
    } else if (contractorId) {
      tokens = Array.from(TOKENS.get(contractorId) || []);
    } else {
      return res.status(400).json({ ok: false, error: "contractorId or token required" });
    }

    if (!tokens.length) {
      return res.status(404).json({ ok: false, error: "No tokens registered for that contractorId" });
    }

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

// --------------------------------------------------
// 🔔 ZAPIER → POST /push → SEND BEAUTIFUL NOTIFICATION
// --------------------------------------------------
app.post("/push", async (req, res) => {
  try {
    const body = req.body || {};

    // Allow many possible Zapier field names
    const subitemId =
      body.subitemId ||
      body.itemId ||
      body.id ||
      body.pulseId ||
      body.pulse_id ||
      "";
    const jobNumber = body.jobNumber || body.job_number || body.number || "";
    const jobName = body.jobName || body.name || body.title || "";
    const change = body.change || body.changed || body.column || "Job updated";

    // --------------------------
    // 🔧 BUILD NOTIFICATION TEXT
    // --------------------------

    // TITLE
    let title = "Job Updated";
    if (jobNumber && jobName) title = `Job ${jobNumber} – ${jobName}`;
    else if (jobNumber) title = `Job ${jobNumber} Updated`;
    else if (jobName) title = jobName;

    // BODY MESSAGE
    const msg = `${change}`;

    console.log("📨 /push received", { subitemId, jobNumber, jobName, change });

    // --------------------------
    // 🔧 LOAD ALL REGISTERED TOKENS
    // --------------------------
    const tokens = [];
    for (const set of TOKENS.values()) {
      for (const t of set) tokens.push(t);
    }

    if (tokens.length === 0) {
      return res.json({ ok: true, sent: 0, reason: "no tokens" });
    }

    // --------------------------
    // 🔥 SEND NOTIFICATION
    // --------------------------
    const result = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: {
        title,
        body: msg,
      },
      data: {
        type: "job_update",
        subitemId: String(subitemId),
        jobNumber: String(jobNumber),
        jobName: String(jobName),
      },
      android: { priority: "high" },
    });

    return res.json({
      ok: true,
      sent: tokens.length,
      result,
    });

  } catch (err) {
    console.error("❌ ERROR /push:", err);
    res.status(500).json({ ok: false, error: String(err) });
  }
});
// quick visibility of registered tokens
app.get("/debug/push-tokens", (_req, res) => {
  const rows = [];
  for (const [contractorId, set] of TOKENS.entries()) {
    rows.push({ contractorId, tokens: set.size });
  }
  res.json({ count: rows.length, entries: rows });
});

/** ----------------- Materials + H&S helpers ----------------- */
const SUBTOKEN_RE = /\b\d{4}-\d+\b/;     // e.g. 2762-5, 2418-10
const MAINTOKEN_RE = /\b(\d{4})\b/;      // e.g. 2762

function splitJobTokens(jobNumRaw) {
  const s = String(jobNumRaw || "");
  const subToken = (s.match(SUBTOKEN_RE) || [])[0] || "";
  const mainToken = subToken ? subToken.split("-")[0] : ((s.match(MAINTOKEN_RE) || [])[1] || "");
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
function groupByStatusThenSupplier(rows) {
  const out = {};
  for (const r of rows) {
    const status = r.status || "Uncategorised";
    const supplier = String(r.supplier || "No supplier").trim() || "No supplier";

    if (!out[status]) out[status] = {};
    if (!out[status][supplier]) out[status][supplier] = [];
    out[status][supplier].push(r);
  }
  return out;
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
// Pull a column's best text:
// - prefer cv.text
// - else try JSON-parsed cv.value and common fields
function cvText(cvsOrCv, idMaybe) {
  // Support both cvText(cvMap, colId) and cvText(cvObj)
  const cv = (idMaybe !== undefined)
    ? (cvsOrCv ? cvsOrCv[idMaybe] : null)
    : cvsOrCv;

  if (!cv) return "";

  const t = String(cv.text || "").trim();
  if (t) return t;

  try {
    const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
    if (!v) return "";

    // common patterns
    if (typeof v === "string") return v.trim();
    if (v.text) return String(v.text).trim();
    if (v.label && typeof v.label === "string") return v.label.trim();
    if (v.date) return String(v.date).trim();
    if (v.url) return String(v.url).trim();

    return "";
  } catch {
    return "";
  }
}

// ---------- H&S field mapping (labels) ----------
const HS_FIELD_LABELS = {
  color_mknjqnhc: "H&S Doc",
  color_mkvdw9a7: "Assessor / PM Sign Off",
  date_mkvdfra8: "Sign Date",
  text: "Signed By",
  long_text_mkvcarzd: "Scope",
  status9: "Emergency Plan Required",
  color1: "Does WorkSafe need to be notified for any planned activities?",
  status_mkmvrw88: "Notification Activities",
  color8: "Have you provided a copy of the WorkSafe notification?",
  color5: "Will any hazardous materials be brought onsite to conduct planned activities?",
  dropdown7: "If yes, SDS available and read",
  dropdown0: "Familiar with WorkSafe guidelines",
  dropdown_mkvc1qe0: "Hold a current safety card or similar",
  dropdown_mkvc4pvt: "Will be given a job-specific safety induction",
  dropdown_mkvczdje: "Qualified/competent OR fully supervised",
  dropdown_mkmvp845: "Conduct Prestart",
  dropdown_mkvcyybv: "TA Frequency - Pre Start",
  date_mknwjks7: "SSSP Date - Pre Start",
  dropdown_mkvcq1nv: "TA Conduct Daily Pre Start",
  dropdown_mkvchzez: "TA Frequency Daily Pre Start",
  dropdown_mkvcqdtn: "TA Conduct Progress Meeting",
  dropdown_mkvcfv58: "TA Frequency Progress Meeting",
  date_mknwj2yx: "SSSP - Date Progress Meetings",
  dropdown_mkvcfcbg: "TA - Conduct Completion Communication",
  dropdown_mkvc5dha: "TA - Completion who to notify",
  date_mknwve04: "SSSP Date - Tool Box Talk",
  dropdown_mkmv9xnx: "Conduct Tool Box",
  dropdown_mkvcyv8: "Communicate Serious Injury",
  dropdown_mkvcd860: "Communicate Injury requiring First Aid",
  dropdown_mkvc81j7: "Communicate Near Miss - Serious",
  dropdown_mkvcmwnn: "Communicate Near Miss - Minor",
  dropdown_mkvcdymj: "Damage to plant/equipment",
  dropdown_mkvchha8: "Earmuffs",
  dropdown_mkvc9vr0: "HiVis",
  dropdown_mkvcnkj9: "Safety Boots",
  dropdown_mkvcts58: "Gloves",
  dropdown_mkvcvwxf: "Eyewear",
  dropdown_mkvcergx: "Hard Hat",
  dropdown_mkvc8sz2: "Respirator",
  dropdown_mkvctmrq: "Harness",
};

// columns we definitely do NOT want to show in “fields”
const HS_FIELDS_EXCLUDE = new Set([
  "name",
  "subitems",
  "person",
  "button_mm0d2f0a",
  "button_mkn7awv9",
  "numbers_mkn3ttp4",
  "files",               // TA Doc (file)
  "files_mkmvqmk",       // Work Safe Notification (file)
  "link_mm0dr449",       // TA Link
  "link_mkvck5hx",       // Link
  "link_mkvew2y2",       // Link 1
]);

/**
 * Board-relation helper: tries to return readable names.
 * - Prefer column text (Monday usually renders related item titles here)
 * - Also expose linked IDs if you want to use them later
 */
function cvRelation(cvs, id) {
  const cv = cvs[id];
  if (!cv) return { text: "", ids: [] };

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
async function getHazardRegisterMapByName() {
  if (!HAZARD_REGISTER_BOARD_ID) return {};

  const colIds = [
    HZ_RISK_COL_ID,
    HZ_INITIAL_RISK_COL_ID,
    HZ_CONTROLS_COL_ID,
    HZ_RESIDUAL_RISK_COL_ID,
  ].filter(Boolean);

  const q = `
    query($boardId:ID!, $cursor:String, $colIds:[String!]) {
      boards(ids: [$boardId]) {
        items_page(limit: 100, cursor: $cursor) {
          cursor
          items {
            id
            name
            column_values(ids:$colIds){ id text value }
          }
        }
      }
    }`;

  let cursor = null;
  const map = {}; // norm(name) -> details

  do {
    const d = await monday(q, {
      boardId: HAZARD_REGISTER_BOARD_ID,
      cursor,
      colIds,
      _bust: Date.now(),
    });

    const page = d?.boards?.[0]?.items_page;
    cursor = page?.cursor || null;

    for (const it of page?.items || []) {
      const key = norm(it.name);
      if (!key) continue;

      const cv = Object.fromEntries((it.column_values || []).map(c => [c.id, c]));

      map[key] = {
        id: String(it.id || "").trim(),
        name: String(it.name || "").trim(),
        risks: (cv[HZ_RISK_COL_ID]?.text || "").trim(),
        initialRisk: (cv[HZ_INITIAL_RISK_COL_ID]?.text || "").trim(),
        controls: (cv[HZ_CONTROLS_COL_ID]?.text || "").trim(),
        postRisk: (cv[HZ_RESIDUAL_RISK_COL_ID]?.text || "").trim(),
      };
    }
  } while (cursor);

  return map;
}

/**
 * Fetch materials based on status mode:
 * - "Only Sub Task Materials": read SUBITEMS_MATERIALS_BOARD_ID; include items whose name starts with subToken (e.g. 2762-5)
 * - "Include Main Scope Materials": read MATERIALS_BOARD_ID; find parent whose name starts with mainToken (e.g. 2762);
 *   include its subitems EXCEPT those whose name starts with `${mainToken}-` (exclude subjob lines)
 *
 * NEW: Adds Supplier from board-relation column SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID (e.g. "connect_boards6")
 * and logs debug info so we can see what the server is doing.
 */
async function getMaterialsForJob(jobNumRaw, matScopeStatus) {
  if (!matScopeStatus || /no materials/i.test(matScopeStatus)) {
    console.log("getMaterialsForJob: status says no materials", { jobNumRaw, matScopeStatus });
    return null;
  }

  const { subToken, mainToken } = splitJobTokens(jobNumRaw);
  const wantOnlySub = /only sub task materials/i.test(matScopeStatus);
  const wantMain    = /include main scope materials/i.test(matScopeStatus);

  const subBoardId    = SUBITEMS_MATERIALS_BOARD_ID;
  const parentBoardId = MATERIALS_BOARD_ID;

  const titleColId    = SUBITEMS_MAT_TITLE_TEXT_COLUMN_ID;
  const notesColId    = SUBITEMS_MAT_NOTES_LONGTEXT_COLUMN_ID;
  const statusColId   = SUBITEMS_MAT_NOTES_LONGTEXT_STATUS;        // may be blank
  const supplierColId = SUBITEMS_MAT_SUPPLIER_RELATION_COLUMN_ID;  // relation col

  console.log("getMaterialsForJob DEBUG →", {
    jobNumRaw,
    matScopeStatus,
    wantOnlySub,
    wantMain,
    subToken,
    mainToken,
    subBoardId,
    parentBoardId,
    titleColId,
    notesColId,
    statusColId,
    supplierColId,
  });

  // If we don't even know which columns hold the material title, we can't do much
  if (!titleColId) {
    console.log("getMaterialsForJob: missing titleColId, aborting");
    return null;
  }

  // Helper: safe status text (if no status column, just bucket everything together)
  const pickStatus = (cvMap) =>
    statusColId ? (cvText(cvMap, statusColId) || "Uncategorised") : "Uncategorised";

  // CASE A: Only Sub Task Materials
  if (wantOnlySub) {
    if (!subToken || !subBoardId) {
      console.log("getMaterialsForJob: ONLY SUB but missing subToken or subBoardId", { subToken, subBoardId });
      return null;
    }

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
        if (!nm.startsWith(subToken)) continue; // strict startsWith: "2788-2..."

        const cv = Object.fromEntries((it.column_values || []).map(c => [c.id, c]));
        const supplierRel = supplierColId ? cvRelation(cv, supplierColId) : { text: "", ids: [] };

let supplierText = String(supplierRel.text || "").trim();
if (!supplierText && Array.isArray(supplierRel.ids) && supplierRel.ids.length) {
  const names = await getItemNamesByIds(supplierRel.ids);
  supplierText = names.join(", ");
}

rows.push({
  id: it.id,
  name: nm,

  // ✅ keep material title as data (not the header label)
  materialTitle: cvText(cv, titleColId),

  notes: notesColId ? cvText(cv, notesColId) : "",
  status: pickStatus(cv),

  // ✅ supplier becomes the grouping header
  supplier: supplierText || "",
  supplierIds: supplierRel.ids || [],
});
      }
    } while (cursor);

    console.log("getMaterialsForJob: ONLY SUB → rows:", rows.length);
    return rows.length
  ? {
      mode: "Only Sub Task Materials",
      byStatus: groupByStatus(rows), // ✅ existing (do not break app)
      byStatusSupplier: groupByStatusThenSupplier(rows), // ✅ new nested grouping
    }
  : null;
  }

  // CASE B: Include Main Scope Materials
  if (wantMain) {
    if (!mainToken || !parentBoardId) {
      console.log("getMaterialsForJob: MAIN SCOPE but missing mainToken or parentBoardId", { mainToken, parentBoardId });
      return null;
    }

    // 1) Find the parent item on MATERIALS_BOARD_ID whose name starts with "2788"
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
      const d = await monday(qParent, { boardId: parentBoardId, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of (page?.items || [])) {
        const nm = String(it.name || "");
        if (nm.startsWith(mainToken)) {
          parent = it;
          cursor = null;
          break;
        }
      }
    } while (cursor && !parent);

    if (!parent || !Array.isArray(parent.subitems)) {
      console.log("getMaterialsForJob: MAIN SCOPE – parent not found or has no subitems", {
        mainToken,
        foundParent: !!parent,
      });
      return null;
    }

    // 2) From that parent's subitems: include all except names that start with `${mainToken}-`
    const rows = [];
for (const si of parent.subitems) {
  const nm = String(si.name || "");
  if (nm.startsWith(`${mainToken}-`)) continue;

  const cv = Object.fromEntries((si.column_values || []).map(c => [c.id, c]));
  const supplierRel = supplierColId ? cvRelation(cv, supplierColId) : { text: "", ids: [] };

  let supplierText = String(supplierRel.text || "").trim();
  if (!supplierText && Array.isArray(supplierRel.ids) && supplierRel.ids.length) {
    const names = await getItemNamesByIds(supplierRel.ids);
    supplierText = names.join(", ");
  }

  rows.push({
    id: si.id,
    name: nm,
    materialTitle: cvText(cv, titleColId),
    notes: notesColId ? cvText(cv, notesColId) : "",
    status: pickStatus(cv),
    supplier: supplierText || "",
    supplierIds: supplierRel.ids || [],
  });
} // ✅ CLOSE LOOP HERE

console.log("getMaterialsForJob: MAIN SCOPE → subitems rows:", rows.length);
return rows.length
  ? {
      mode: "Include Main Scope Materials",
      byStatus: groupByStatus(rows),
      byStatusSupplier: groupByStatusThenSupplier(rows),
    }
  : null;
}
}
// ---------- debug ----------
app.get("/debug/ping", (_req, res) => res.json({ ok: true, t: Date.now() }));
app.get("/debug/env-safe", (_req, res) => {
  const safe = {
    PORT,
    // contractors
    CONTRACTORS_BOARD_ID,
    CONTRACTORS_EMAIL_COLUMN_ID,
    CONTRACTORS_PIN_TEXT_COLUMN_ID,
CONTRACTORS_LOGIN_NAME_COLUMN_ID,

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
SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID,
SUBITEMS_SUBCONTRACTOR_TEXT_COLUMN_ID,
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
HAZARD_REGISTER_BOARD_ID,
HZ_RISK_COL_ID,
HZ_INITIAL_RISK_COL_ID,
HZ_CONTROLS_COL_ID,
HZ_RESIDUAL_RISK_COL_ID,
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
app.get("/debug/hazard-register/search", async (req, res) => {
  try {
    const qRaw = String(req.query.q || "").trim();
    if (!qRaw) return res.status(400).json({ ok: false, error: "Missing ?q=Battery Tools" });

    const hazardRegisterByName = await getHazardRegisterMapByName();
    const key = norm(qRaw);

    const found = hazardRegisterByName[key] || null;

    return res.json({
      ok: true,
      query: qRaw,
      norm: key,
      found,
      sampleKeys: Object.keys(hazardRegisterByName).slice(0, 25),
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

console.log("HS ENRICHED SAMPLE: (moved into route)"); // (was crashing server)
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
app.get("/debug/hs-subitem/:id", async (req, res) => {
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
    const item = d?.items?.[0];

    const connectColId = String(process.env.HS_HAZARD_REGISTER_CONNECT_COL_ID || "").trim();
    const cv = item?.column_values?.find(c => c.id === connectColId);

    res.json({
      ok: true,
      subitemId: id,
      subitemName: item?.name || "",
      connectColId,
      connectColText: cv?.text || "",
      connectColType: cv?.type || "",
      connectColValue: cv?.value || "",
      allCols: (item?.column_values || []).map(c => ({ id: c.id, type: c.type, text: c.text })),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});
// ---------- health ----------
app.get("/health", (_req, res) => res.json({ ok: true, t: Date.now() }));

// =====================================================================
// POST /hs/signoff
// Creates signed H&S PDF and uploads to SUBITEM file column
// =====================================================================
app.post("/hs/signoff", async (req, res) => {
  try {
    console.log("🔎 HS SIGNOFF BODY:", req.body);

    const {
  subitemId,
  jobNumber = "",
  workerName = "",
  signaturePngBase64 = "",
  additionalHazards: additionalHazardsRaw = [],
} = req.body || {};

// Normalise/clean additional hazards from app
const additionalHazards = Array.isArray(additionalHazardsRaw)
  ? additionalHazardsRaw
      .map((h) => {
        // allow a couple of possible shapes coming from the app
        const hazard =
          String(h?.hazard ?? h?.title ?? h?.name ?? "").trim();
        const control =
          String(h?.control ?? h?.controls ?? "").trim();

        // drop empty rows
        if (!hazard && !control) return null;

        // enforce shape expected by buildSignedHsPdf (hazard/control keys)
        return { hazard, control };
      })
      .filter(Boolean)
  : [];

    if (!subitemId) {
      return res.status(400).json({ ok: false, error: "Missing subitemId" });
    }
    if (!String(workerName).trim()) {
      return res.status(400).json({ ok: false, error: "Missing workerName" });
    }

    const columnId = String(process.env.SUBITEMS_SIGNED_HS_FILE_COL_ID || "").trim();
    if (!columnId) {
      return res.status(500).json({
        ok: false,
        error: "Missing SUBITEMS_SIGNED_HS_FILE_COL_ID in .env",
      });
    }

    const signedAtISO = new Date().toISOString();

// Get the TA PDF once (your existing "by subite" function)
const { buffer: taPdfBuffer } = await fetchTaPdfBufferForSubitem(subitemId);

// ✅ Stamp signature + name + NZ date/time onto page 1
const stampedTaBuffer = await stampWorkerSignoffOnTaPdf({
  taPdfBuffer,
  workerName,
  signedAtISO,
  signaturePngBase64,
});

// ✅ Build a sign-off page that includes any extra hazards the worker adds
const signoffBuffer = await buildSignedHsPdf({
  jobNumber: jobNumber || "",
  workerName,
  signedAtISO,
  signaturePngBase64,
  additionalHazards,
});

// ✅ Append sign-off page to the stamped TA
const pdfBuffer = await appendSignoffPageToExistingPdf(stampedTaBuffer, signoffBuffer);
    const safeJob = String(jobNumber || subitemId).replace(/[^a-z0-9-_]+/gi, "_");
    const safeWorker = String(workerName).replace(/[^a-z0-9-_]+/gi, "_");
    const fileName = `HS_Signoff_${safeJob}_${safeWorker}_${signedAtISO.slice(0, 10)}.pdf`;

    await uploadFileToMondayColumn({
      itemId: Number(subitemId),
      columnId,
      fileName,
      buffer: pdfBuffer,
    });

    // bust cached job details so the new file appears immediately
    _cache.delete(`jobDetails:${String(subitemId)}:${getFileColumnIds().join(",")}`);

    return res.json({ ok: true });
  } catch (err) {
    console.error("POST /hs/signoff error:", err);
    return res.status(500).json({ ok: false, error: err.message });
  }
});

// ---------- auth ----------
app.post("/auth/login", async (req, res) => {
  try {

    // ✅ TEMP DEBUG — confirms request + body reaching Render
    console.log("✅ HIT /auth/login on THIS server", {
      time: new Date().toISOString(),
      contentType: req.headers["content-type"],
      body: req.body,
    });

    const loginName = String(req.body?.loginName || "").trim().toLowerCase();
    const pin = String(req.body?.pin || "").trim();
console.log("ENV CHECK:", {
  CONTRACTORS_LOGIN_NAME_COLUMN_ID: process.env.CONTRACTORS_LOGIN_NAME_COLUMN_ID,
  CONTRACTORS_PIN_TEXT_COLUMN_ID: process.env.CONTRACTORS_PIN_TEXT_COLUMN_ID,
});
    if (!loginName || !pin) {
      return res.status(400).json({ ok: false, error: "Login name and PIN required." });
    }

   console.log("🔐 LOGIN attempt:", {
      loginName,
      pinLen: String(pin).length,
      bodyKeys: Object.keys(req.body || {}),
      receivedLoginName: req.body?.loginName,
      receivedPinType: typeof req.body?.pin,
    });
// Pull contractors and match by login column (paged)
    const q = `
      query($boardId: ID!, $cursor: String) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items {
              id
              name
              column_values(ids: [
                "${CONTRACTORS_LOGIN_NAME_COLUMN_ID}",
                "${CONTRACTORS_PIN_TEXT_COLUMN_ID}"
              ]) {
                id
                text
              }
            }
          }
        }
      }`;

    let cursor = null;
    let contractor = null;

    do {
      const d = await monday(q, { boardId: CONTRACTORS_BOARD_ID, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of (page?.items || [])) {

  if (!req._loginSampleLogged && page?.items?.length) {
    req._loginSampleLogged = true;
    const first = page.items[0];
    const firstCols = Object.fromEntries((first.column_values || []).map(c => [c.id, c]));
    console.log("👀 LOGIN sample contractor row:", {
      firstId: first.id,
      firstName: first.name,
      loginColId: CONTRACTORS_LOGIN_NAME_COLUMN_ID,
      pinColId: CONTRACTORS_PIN_TEXT_COLUMN_ID,
      loginText: firstCols[CONTRACTORS_LOGIN_NAME_COLUMN_ID]?.text,
      pinText: firstCols[CONTRACTORS_PIN_TEXT_COLUMN_ID]?.text,
    });
  }

        const cols = Object.fromEntries((it.column_values || []).map(c => [c.id, c]));

        const storedLogin = String(cols[CONTRACTORS_LOGIN_NAME_COLUMN_ID]?.text || "")
          .trim()
          .toLowerCase();

        const storedPin = cleanPin4(cols[CONTRACTORS_PIN_TEXT_COLUMN_ID]?.text || "");
const pin4 = cleanPin4(pin);

        if (storedLogin === loginName && storedPin === pin4) {
          contractor = it;
          cursor = null; // stop paging
          break;
        }
      }
    } while (cursor && !contractor);

    if (!contractor) {
      return res.status(401).json({ ok: false, error: "Invalid login or PIN." });
    }

    return res.json({
      ok: true,
      contractorId: contractor.id,
      contractorName: contractor.name
    });

  } catch (e) {
    console.error("ERROR /auth/login:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ---------- jobs (cached) ----------
app.get("/jobs/my", async (req, res) => {
  try {
    const contractorId = String(req.query.contractorId || "").trim();
    if (!contractorId) {
      return res.status(400).json({ error: "contractorId required" });
    }
const contractorName = await getContractorNameById(contractorId);
const contractorNameKey = nameKey(contractorName);
    const onDate = String(req.query.on || "");
    const includeWeekends = String(req.query.includeWeekends || "1") !== "0";
    const page = Math.max(1, parseInt(String(req.query.page || "1"), 10));
    const limit = Math.min(100, Math.max(1, parseInt(String(req.query.limit || "50"), 10)));
    const offset = (page - 1) * limit;

    // ✅ cache key must NOT use `email` (not defined). Use contractorId.
    const bust = String(req.query.bust || "");
const cacheKey = `jobs:${contractorId}:${onDate}:${includeWeekends}:${page}:${limit}:${bust}`;
    const hit = cacheGet(cacheKey);
    if (hit) return res.json(hit);

 
    // ✅ You no longer need to "find contractor id" by email.
    // contractorId is already provided by the app login flow / query param.

    // ... carry on with your existing job/subitem loading logic below ...

     // ✅ We now query SUBITEMS_BOARD_ID directly (NO jobs board scan)
    if (!SUBITEMS_BOARD_ID) {
      return res.status(500).json({ error: "SUBITEMS_BOARD_ID missing in .env" });
    }
    if (!SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID) {
      return res.status(500).json({ error: "SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID missing in .env" });
    }

    const subCols = [
      SUBITEMS_TIMELINE_COLUMN_ID,
      SUBITEMS_JOBNUMBER_COLUMN_ID,
      SUBITEMS_DESCRIPTION_COLUMN_ID,
      SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID,        // filter column
      SUBITEMS_SUBCONTRACTOR_TEXT_COLUMN_ID,      // for matching contractor
    ].filter(Boolean);

    const addrCols = [JOBS_ADDRESS_COLUMN_ID].filter(Boolean);

    // 1) Pull ONLY subitems where On Device == "On Device"
const subitemsQ = `
  query($boardId: ID!, $cursor: String, $subCols: [String!]) {
    boards(ids: [$boardId]) {
      items_page(
        limit: 100,
        cursor: $cursor,
      ) {
        cursor
        items {
          id
          name
          parent_item { id }
          column_values(ids: $subCols) {
            id
            text
            value
          }
        }
      }
    }
  }
`;
    // Helper: fetch parent job addresses for ONLY the parents we actually need
    async function fetchParentAddresses(parentIds = []) {
      const ids = Array.from(new Set((parentIds || []).map(String).filter(Boolean)));
      if (!ids.length || !addrCols.length) return {};

      const qParents = `
        query($ids:[ID!], $addrCols:[String!]) {
          items(ids: $ids) {
            id
            column_values(ids:$addrCols) { id text }
          }
        }`;

      const d = await monday(qParents, { ids, addrCols });
      const map = {};
      for (const it of (d?.items || [])) {
        map[String(it.id)] = it?.column_values?.[0]?.text || "";
      }
      return map;
    }

    const isWeekend = (iso) => {
      if (!iso) return false;
      const dt = new Date(`${iso}T12:00:00Z`);
      const dow = dt.getUTCDay();
      return dow === 0 || dow === 6;
    };

   let cursor = null;
let totalPossible = 0;
let collected = 0;
const results = [];

do {
  const d = await monday(subitemsQ, {
    boardId: SUBITEMS_BOARD_ID,
    cursor,
    subCols,
  });

  const itemsPage = d?.boards?.[0]?.items_page;
  cursor = itemsPage?.cursor || null;

  const items = itemsPage?.items || [];
  if (!items.length) continue;

  // Already filtered by query_params above
const onDeviceItems = items.filter((it) => {
  const cv = (it.column_values || []).find(
    (c) => c.id === SUBITEMS_ON_DEVICE_STATUS_COLUMN_ID
  );
  return String(cv?.text || "").trim() === "On Device";
});
if (!onDeviceItems.length) continue;

  const pending = [];

  for (const s of onDeviceItems) {
    const sCols = Object.fromEntries((s.column_values || []).map((cv) => [cv.id, cv]));

    const subTxt = String(sCols[SUBITEMS_SUBCONTRACTOR_TEXT_COLUMN_ID]?.text || "").trim();

    if (!contractorNameKey) continue;
    if (nameKey(subTxt) !== contractorNameKey) continue;

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
      const parentId = String(s?.parent_item?.id || "");

      pending.push({ 

        parentId,
        parentJobId: parentId,
        parentJobName: s?.parent_item?.name || "",
        subitemId: s.id,
        subitemName: s.name,
        jobNumber: sCols[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || "",
        description: sCols[SUBITEMS_DESCRIPTION_COLUMN_ID]?.text || "",
        timeline: { startDate, endDate },
      });

      collected++;
    }

    if (collected >= limit) {
      cursor = null;
      break;
    }
  }

  const neededParentIds = pending.map((p) => p.parentId).filter(Boolean);
  const addressByParentId = await fetchParentAddresses(neededParentIds);

  for (const p of pending) {
    results.push({
      ...p,
      address: addressByParentId[p.parentId] || "",
    });
  }
} while (cursor);

const out = { items: results, total: totalPossible, page, limit };
cacheSet(cacheKey, out);
return res.json(out);

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

// ✅ ADD THIS NEW FAST ROUTE DIRECTLY BELOW
app.get("/jobs/:subitemId/details-fast", async (req, res) => {
  try {
    const subitemId = String(req.params.subitemId).trim();

    const q = `
      query($id:[ID!], $colIds:[String!]!) {
        items(ids:$id){
          id
          name
          column_values(ids:$colIds){ id text value }
        }
      }`;

    const colIds = [
      SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID,
      SUBITEMS_JOBNUMBER_COLUMN_ID,
      SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID,
      TIME_ALLOWANCE_COLUMN_ID,
    ].filter(Boolean);

    const d = await monday(q, { id: [subitemId], colIds });
    const item = d?.items?.[0];

    if (!item) {
      return res.json({
        scope: "",
        timeAllowance: "",
        jobNumber: "",
        matScopeStatus: "",
      });
    }

    const cvMap = Object.fromEntries(
      (item.column_values || []).map(cv => [cv.id, cv])
    );

    const scope = String(
      cvMap[SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID]?.text || ""
    ).trim();

    const timeAllowance = String(
      cvMap[TIME_ALLOWANCE_COLUMN_ID]?.text || ""
    ).trim();

    const matScopeStatus = String(
      cvMap[SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID]?.text || ""
    ).trim();

    let jobNumber = String(
      cvMap[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || ""
    ).trim();

    if (!jobNumber) {
      const m = String(item.name || "").match(/\b\d{4}(?:-\d+)?\b/);
      jobNumber = m ? m[0] : "";
    }

    return res.json({ scope, timeAllowance, jobNumber, matScopeStatus });

  } catch (e) {
    return res.status(500).json({ error: e?.message || "Server error" });
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
      SUBITEMS_JOBNUMBER_COLUMN_ID,
      SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID,
      TIME_ALLOWANCE_COLUMN_ID,
    ].filter(Boolean);

    const dSub = await monday(qSub, { id: [subitemId], colIds });
    const item = dSub?.items?.[0];
    if (!item) return res.json({ scope: "", timeAllowance: "", hs: null, materials: null });

    const cvMap = Object.fromEntries((item.column_values || []).map((cv) => [cv.id, cv]));

    // robust text reader (uses cv.text, then value.text)
    function cvBestText(cv) {
      if (!cv) return "";
      const t = String(cv.text || "").trim();
      if (t) return t;
      try {
        const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
        const vt = String(v?.text || "").trim();
        return vt || "";
      } catch {
        return "";
      }
    }

    const scope = cvBestText(cvMap[SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID]);
    const timeAllowance = cvBestText(cvMap[TIME_ALLOWANCE_COLUMN_ID]);

    // Job number: prefer configured column, else extract from subitem name (e.g. "2762-5" or "2762")
    let jobNumRaw = (cvMap[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || "").trim();
    if (!jobNumRaw) {
      const m = String(item.name || "").match(/\b\d{4}(?:-\d+)?\b/);
      jobNumRaw = m ? m[0] : "";
    }

    const { subToken, mainToken } = splitJobTokens(jobNumRaw);
    const matScopeStatus = (cvMap[SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID]?.text || "").trim();

    // 2) H&S (pull filled fields + hazards subitems)
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
      const wantPrefix = subToken || mainToken;

      if (wantPrefix) {
        do {
          const dHS = await monday(qHS, { boardId: HS_BOARD_ID, cursor, _bust: Date.now() });
          const page = dHS?.boards?.[0]?.items_page;
          cursor = page?.cursor || null;

          for (const it of page?.items || []) {
            const nm = String(it.name || "").trim();
            if (!nm.startsWith(wantPrefix)) continue;

            const cvs = Object.fromEntries((it.column_values || []).map((cv) => [cv.id, cv]));
            const url = cvUrl(cvs, HS_PDF_URL_COLUMN_ID);

            const fields = (it.column_values || [])
              .filter((cv) => !HS_FIELDS_EXCLUDE.has(cv.id))
              .map((cv) => {
                const label = HS_FIELD_LABELS[cv.id] || cv.id;
                const value = String(cv.text || "").trim();
                return value ? { label, value } : null;
              })
              .filter(Boolean);


const hazardRegisterByName = await getHazardRegisterMapByName(); // norm(name) -> details

// This is the column on the H&S subitems that contains the hazard title (e.g. "Battery Tools")
const HAZARD_TITLE_COL_ID = "text_mkvkjfvh";

const hazards = (it.subitems || []).map((si) => {
  // Build a cv map for this H&S hazard subitem
  const siCv = Object.fromEntries((si.column_values || []).map((c) => [c.id, c]));

  // ✅ Pull the actual hazard title from the column, NOT the subitem name ("1", "2", etc)
  const hazardTitle = (cvText(siCv, HAZARD_TITLE_COL_ID) || "").trim() || String(si.name || "").trim();

  // Lookup in Hazard Register (board 1889935521)
  const key = norm(hazardTitle);
  const reg = hazardRegisterByName[key] || null;
return {
  id: si.id,

  // ✅ add these back for the app UI
  name: hazardTitle,
  title: hazardTitle,
  hazard: hazardTitle,

  sections: [
    { label: "Hazard", value: hazardTitle },
    { label: "Risks", value: reg?.risks || "" },
    { label: "Initial Risk", value: reg?.initialRisk || "" },
    { label: "Controls", value: reg?.controls || "" },
    { label: "Residual Risk", value: reg?.postRisk || "" },
  ],
  hazardRegisterItemId: reg?.id || "",
};
});

// Drop hazards that are effectively blank (no title + no useful data)
const hazardsClean = hazards.filter((h) => {
  const hazardTitle =
    (h.sections || []).find((s) => s.label === "Hazard")?.value ||
    h.title ||
    h.name ||
    h.hazard ||
    "";

  const hasAnyDetail = (h.sections || []).some((s) => String(s?.value || "").trim().length > 0);

  return String(hazardTitle).trim().length > 0 && hasAnyDetail;
});
console.log("HS hazards count:", hazardsClean.length);
console.log("HS hazard sample:", hazardsClean[0]);

// IMPORTANT: return the cleaned hazards
// (If you already reference `hazards` below, change that reference to `hazardsClean`)

            hs = {
              job: wantPrefix,
              url: url || "",
              fields,
              hazards: hazardsClean,
            };

            cursor = null;
            break;
          }
        } while (cursor && !hs);
      }
    }

    // 3) Materials
    const materials = await getMaterialsForJob(jobNumRaw, matScopeStatus);

    return res.json({
      scope,
      timeAllowance,
      hs,
      materials,
    });
  } catch (e) {
    console.error("ERROR /jobs/:subitemId/details2:", e);
    return res.status(500).json({ error: e?.message || "Server error" });
  }
});
// ---------- H&S details (header + hazard subitems) ----------
app.get("/hs/by-subitem/:subitemId", async (req, res) => {
  try {
    const subitemId = String(req.params.subitemId).trim();
    if (!subitemId) return res.status(400).json({ error: "subitemId required" });

    // 1) Load job subitem to extract job number (same logic as details2)
    const qSub = `
      query($id: [ID!], $colIds: [String!]!) {
        items(ids: $id) {
          id
          name
          column_values(ids: $colIds) { id text value }
        }
      }`;

    const subCols = [SUBITEMS_JOBNUMBER_COLUMN_ID].filter(Boolean);
    const dSub = await monday(qSub, { id: [subitemId], colIds: subCols });
    const jobSub = dSub?.items?.[0];
    if (!jobSub) return res.json({ jobNumber: "", hsItem: null, hazards: [] });

    let jobNumRaw = "";
    if (SUBITEMS_JOBNUMBER_COLUMN_ID) {
      const cv = (jobSub.column_values || []).find(c => c.id === SUBITEMS_JOBNUMBER_COLUMN_ID);
      jobNumRaw = String(cv?.text || "").trim();
    }
    if (!jobNumRaw) {
      const m = String(jobSub.name || "").match(/\b\d{4}(?:-\d+)?\b/);
      jobNumRaw = m ? m[0] : "";
    }

    const { subToken, mainToken } = splitJobTokens(jobNumRaw);
    const wantPrefix = subToken || mainToken;

    if (!HS_BOARD_ID || !wantPrefix) {
      return res.json({ jobNumber: jobNumRaw, hsItem: null, hazards: [] });
    }

    // 2) Find the matching H&S item on HS_BOARD_ID by name prefix
    const qHSFind = `
      query($boardId: ID!, $cursor: String) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items { id name }
          }
        }
      }`;

    let cursor = null;
    let hsItemId = null;

    do {
      const d = await monday(qHSFind, { boardId: HS_BOARD_ID, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of (page?.items || [])) {
        const nm = String(it.name || "").trim();
        if (!nm.startsWith(wantPrefix)) continue;
        hsItemId = String(it.id);
        cursor = null;
        break;
      }
    } while (cursor && !hsItemId);

    if (!hsItemId) {
      return res.json({ jobNumber: jobNumRaw, hsItem: null, hazards: [] });
    }

    // 3) Pull header columns + subitems columns
    const headerIds = String(HS_HEADER_COLUMN_IDS || "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    const hazardIds = String(HS_HAZARD_COLUMN_IDS || "")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);

    const qHSFull = `
      query($id: [ID!], $headerIds: [String!], $hazardIds: [String!]) {
        items(ids: $id) {
          id
          name
          column_values(ids: $headerIds) { id text type value }
          subitems {
            id
            name
            column_values(ids: $hazardIds) { id text type value }
          }
        }
      }`;

    const dFull = await monday(qHSFull, { id: [hsItemId], headerIds, hazardIds });
    const hs = dFull?.items?.[0];

    if (!hs) {
      return res.json({ jobNumber: jobNumRaw, hsItem: null, hazards: [] });
    }

    // 4) Filter to only non-empty header fields
    const headerMap = {};
    for (const cv of (hs.column_values || [])) {
      if (!cvHasData(cv)) continue;
      headerMap[cv.id] = {
        id: cv.id,
        text: String(cv.text || "").trim(),
        value: cv.value ?? null,
        type: cv.type || "",
      };
    }

    // 5) Filter hazards (subitems) to only the columns with data
    const hazards = (hs.subitems || []).map(si => {
      const cols = {};
      for (const cv of (si.column_values || [])) {
        if (!cvHasData(cv)) continue;
        cols[cv.id] = {
          id: cv.id,
          text: String(cv.text || "").trim(),
          value: cv.value ?? null,
          type: cv.type || "",
        };
      }
      return {
        id: si.id,
        name: si.name,
        cols,
      };
    }).filter(h => Object.keys(h.cols).length > 0); // keep only hazards with something to show

    return res.json({
      jobNumber: jobNumRaw,
      hsItem: { id: hs.id, name: hs.name, header: headerMap },
      hazards,
      taLink: HS_PDF_URL_COLUMN_ID ? cvUrl(Object.fromEntries((hs.column_values || []).map(c => [c.id, c])), HS_PDF_URL_COLUMN_ID) : "",
    });
  } catch (e) {
    console.error("ERROR /hs/by-subitem/:subitemId", e);
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

// ---- Auto-notify assigned contractors via connect column (no email column) ----
try {
  // 1) Read contractor IDs from the subitem’s contractor connect column
  let contractorIds = [];
  if (SUBITEMS_CONTRACTOR_COLUMN_ID) {
    const qAssigned = `
      query($id:[ID!]) {
        items(ids:$id) {
          column_values(ids:["${SUBITEMS_CONTRACTOR_COLUMN_ID}"]) { id text value }
        }
      }`;
    const dAssigned = await monday(qAssigned, { id: [itemId] });
    const cv = dAssigned?.items?.[0]?.column_values?.[0];
    contractorIds = parseConnectIds(cv?.value);
  }

  // 2) Resolve contractor IDs -> emails (from Contractors board)
const assignedContractorIds = contractorIds;

  // 3) Pull job number for nicer title (best effort)
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
      const m = String(nm).match(/\b\d{4}(?:-\d+)?\b/);
      jobNumber = m ? m[0] : "";
    }
  }

  // 4) Fire push
  await notifyJobUpdate(itemId, jobNumber, assignedContractorIds);
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

// MONDAY WEBHOOK: handle Monday challenge + rich push text
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

    const b = req.body || {};
    const subitemId = String(
      b.item_id ||
      b.pulseId ||
      b.pulse_id ||
      b.event?.pulseId ||
      b.event?.pulse_id ||
      ""
    ).trim();

    console.log("🔔 /monday/webhook", {
      keys: Object.keys(b || {}),
      board_id: b.board_id,
      item_id: b.item_id,
      subitemId,
      column_id: b.column_id,
    });

    if (!subitemId) {
      return res.status(200).send("ok");
    }

    // --- Map column_id → friendly label for the message
    const CHANGE_LABELS = {
      [SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID]: "Scope",
      [SUBITEMS_TIMELINE_COLUMN_ID]: "Timeline",
      [TIME_ALLOWANCE_COLUMN_ID]: "Time allowance",
      [SUBITEMS_MATS_SCOPE_STATUS_COLUMN_ID]: "Materials scope",
    };

    const changedLabel = CHANGE_LABELS[b.column_id] || "Job details";

    // --- Helper: best-effort text from a column_value
    function cvBestText(cv) {
      if (!cv) return "";
      const t = String(cv.text || "").trim();
      if (t) return t;
      try {
        const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
        const vt = String(v?.text || "").trim();
        return vt || "";
      } catch {
        return "";
      }
    }

    // --- Fetch assigned emails + job number + job name for this item
    async function getAssignedContractorIdsJobNumberAndName(itemId) {
  let contractorIds = [];
  let jobNumber = "";
  let jobName = "";

  const colIds = [
    SUBITEMS_CONTRACTOR_COLUMN_ID,
    SUBITEMS_JOBNUMBER_COLUMN_ID,
  ].filter(Boolean);

  const q = `
    query($id:[ID!], $colIds:[String!]) {
      items(ids:$id){
        id
        name
        board { id name }
        column_values(ids:$colIds){ id text value }
      }
    }`;

  const d = await monday(q, { id: [itemId], colIds });
  const item = d?.items?.[0];

  if (!item) {
    console.log("📌 webhook job lookup – item not found", { itemId });
    return { contractorIds: [], jobNumber: "", jobName: "" };
  }

  jobName = item.name || "";
  const cvMap = Object.fromEntries((item.column_values || []).map((cv) => [cv.id, cv]));

  // Assigned contractors from connect column (IDs)
  if (SUBITEMS_CONTRACTOR_COLUMN_ID) {
    contractorIds = parseConnectIds(cvMap[SUBITEMS_CONTRACTOR_COLUMN_ID]?.value);
  }

  // Job number from configured column (if present)
  if (SUBITEMS_JOBNUMBER_COLUMN_ID) {
    jobNumber = cvBestText(cvMap[SUBITEMS_JOBNUMBER_COLUMN_ID]) || "";
  }

  // Fallback: try to pull 4-digit or 4-digit-dash-sub from the item name
  if (!jobNumber && jobName) {
    const m = String(jobName).match(/\b\d{4}(?:-\d+)?\b/);
    if (m) jobNumber = m[0];
  }

  console.log("📌 webhook job lookup", {
    itemId,
    boardId: item.board?.id,
    boardName: item.board?.name,
    jobNumber,
    jobName,
    contractorIds,
  });

  return { contractorIds, jobNumber, jobName };
}

    const { contractorIds, jobNumber, jobName } =
  await getAssignedContractorIdsJobNumberAndName(subitemId);

    // --- Build nice title + body
    let title = "Job Updated";
    if (jobNumber && jobName) {
      title = `Job ${jobNumber} – ${jobName}`;
    } else if (jobNumber) {
      title = `Job ${jobNumber} Updated`;
    } else if (jobName) {
      title = jobName;
    }

    const body = jobNumber
      ? `${changedLabel} updated on Job ${jobNumber}.`
      : `${changedLabel} updated on this job.`;

    // --- Collect FCM tokens for all assigned contractorIds
const tokens = [];
for (const raw of contractorIds || []) {
  const cid = String(raw || "").trim();
  if (!cid) continue;
  const set = TOKENS.get(cid);
  if (!set) continue;
  for (const t of set) tokens.push(t);
}
const uniqueTokens = Array.from(new Set(tokens));

    if (!uniqueTokens.length) {
      console.log("🔕 No tokens for contractorIds", contractorIds);
return res.json({
  ok: true,
  notified: 0,
  reason: "no_tokens",
  jobNumber,
  item_id: subitemId,
  contractorIds,
});
}

    // --- Send push
    const result = await sendToTokens(uniqueTokens, {
      notification: { title, body },
      data: {
        type: "job_update",
        subitemId: String(subitemId),
        jobNumber: String(jobNumber || ""),
        jobName: jobName || "",
        change: changedLabel,
      },
    });

    return res.json({
      ok: true,
      notified: (contractorIds || []).length,
      jobNumber,
      item_id: subitemId,
      result,
    });
  } catch (err) {
    console.error("ERROR /monday/webhook:", err?.message || err);
    return res.status(200).send("ok");
  }
});

// ------------------ ENV DEBUG ROUTE ------------------
app.get("/debug/env", (req, res) => {
  const keys = Object.keys(process.env).sort();
  const out = {};

  for (const k of keys) {
    const v = process.env[k];
    out[k] = v && v.length ? v : "(EMPTY)";
  }

  // Highlight likely problems
  const problems = [];

  if (!process.env.SUBITEMS_JOBNUMBER_COLUMN_ID)
    problems.push("❌ SUBITEMS_JOBNUMBER_COLUMN_ID is EMPTY — job linking will FAIL.");

  if (!process.env.SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID)
    problems.push("❌ SUBITEMS_SCOPE_LONGTEXT_COLUMN_ID missing — Scope will NOT show.");

  if (!process.env.HS_PDF_URL_COLUMN_ID)
    problems.push("❌ HS_PDF_URL_COLUMN_ID missing — H&S Docs will NOT show.");

  if (!process.env.MATERIALS_BOARD_ID)
    problems.push("❌ MATERIALS_BOARD_ID missing — Materials cannot load.");

  return res.json({
    ok: true,
    env: out,
    problems,
    timestamp: Date.now(),
  });
});

// ---------- server ----------
const server = app.listen(Number(PORT), () => console.log("API running on :" + PORT));
server.keepAliveTimeout = 65000;
server.headersTimeout = 66000;