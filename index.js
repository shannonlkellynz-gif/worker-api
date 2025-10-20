// index.js — Monday.com proxy server (CommonJS; updated with auth + timesheets)
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const FormData = require("form-data");

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));

const MONDAY_API = "https://api.monday.com/v2";
const MONDAY_FILE_API = "https://api.monday.com/v2/file";

const {
  PORT = "4000",
  MONDAY_TOKEN,
  // Contractors board (top-level)
  CONTRACTORS_BOARD_ID,
  CONTRACTORS_EMAIL_COLUMN_ID,
  // NEW: App PIN text column id on Contractors board
  CONTRACTORS_PIN_TEXT_COLUMN_ID,

  // Jobs board (top-level) + address column on parent item
  JOBS_BOARD_ID,
  JOBS_ADDRESS_COLUMN_ID,

  // Subitem columns
  SUBITEMS_CONTRACTOR_COLUMN_ID,
  SUBITEMS_TIMELINE_COLUMN_ID,
  SUBITEMS_JOBNUMBER_COLUMN_ID,
  SUBITEMS_DESCRIPTION_COLUMN_ID,
  SUBITEMS_EMAIL_COLUMN_ID,

  // Comma-separated subitem file columns to expose (e.g. file_mkq27jjz,file_mkvr42v,file_mkty6ysc)
  SUBITEMS_FILE_COLUMN_IDS,

  // Timesheet board + columns
  TIMESHEETS_BOARD_ID,
  TS_DATE_COLUMN_ID,
  TS_NAME_COLUMN_ID,
  TS_START_NUM_COLUMN_ID,
  TS_FINISH_NUM_COLUMN_ID,
  // Prefer text Yes/No for lunch
  TS_LUNCH_TEXT_COLUMN_ID, // e.g., text_mkvrjn0s  (value "Yes" or "No")
  // Optional legacy dropdown id (not recommended)
  TS_LUNCH_BOOL_DROPDOWN_ID, // if used, labels must be {1: Yes, 0: No}
  TS_JOBNUMBER_TEXT_COLUMN_ID,
  TS_TOTAL_HOURS_NUM_COLUMN_ID,
  TS_NOTES_LONGTEXT_COLUMN_ID,
  TS_JOB_COMPLETE_TEXT_COLUMN_ID, // e.g., text_mkvrwc2e  (value "Yes"/"No")
  TS_PHOTOS_FILE_COLUMN_ID, // (not used on create currently)
} = process.env;

// ---------------- Core Monday helper ----------------
async function monday(query, variables = {}, isFile = false, form) {
  if (isFile && form) {
    const r = await fetch(MONDAY_FILE_API, {
      method: "POST",
      headers: { Authorization: `Bearer ${MONDAY_TOKEN}` },
      body: form,
    });
    const j = await r.json();
    if (j.errors) throw new Error(JSON.stringify(j.errors));
    return j;
  }
  const r = await fetch(MONDAY_API, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${MONDAY_TOKEN}`,
    },
    body: JSON.stringify({ query, variables }),
  });
  const j = await r.json();
  if (j.errors) throw new Error(JSON.stringify(j.errors));
  return j.data;
}

app.get("/health", (_req, res) => res.json({ ok: true }));

// ---------------- Helpers ----------------
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
async function resolveAssets(assetIds = []) {
  const unique = Array.from(new Set(assetIds.map(String))).filter(Boolean);
  if (unique.length === 0) return {};
  const q = `
    query($ids: [ID!]!) {
      assets(ids: $ids) {
        id
        url
        public_url
        name
        file_extension
      }
    }`;
  const d = await monday(q, { ids: unique });
  const out = {};
  for (const a of d?.assets || []) {
    out[String(a.id)] = {
      url: a.url || null,
      public_url: a.public_url || null,
      name: a.name || "",
      ext: a.file_extension || "",
    };
  }
  return out;
}
const pad2 = (n) => (n < 10 ? `0${n}` : String(n));
const to4 = (nOrStr) => {
  const s = String(nOrStr ?? "").replace(/\D/g, "");
  if (!s) return "";
  return s.padStart(4, "0").slice(0, 4);
};

// ---------------- AUTH ----------------
// POST /auth/login — verify email + 4-digit PIN against Contractors board
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
            items {
              id
              name
              column_values { id text value }
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

      for (const it of page?.items || []) {
        const match = (it.column_values || []).some(
          (cv) =>
            cv.id === CONTRACTORS_EMAIL_COLUMN_ID &&
            String(cv.text || "").trim().toLowerCase() === email
        );
        if (match) {
          contractor = it;
          break;
        }
      }
    } while (!contractor && cursor);

    if (!contractor) {
      return res.status(401).json({ ok: false, error: "Invalid email or PIN." });
    }

    // Preferred text column for PIN
    const cvs = contractor.column_values || [];
    let storedText = "";
    if (CONTRACTORS_PIN_TEXT_COLUMN_ID) {
      const pinCv = cvs.find((cv) => cv.id === CONTRACTORS_PIN_TEXT_COLUMN_ID);
      storedText = String(pinCv?.text || "").trim();
    }
    // Fallback: any 4-digit text on row
    if (!/^\d{4}$/.test(storedText)) {
      const guess = cvs
        .map((cv) => String(cv?.text || "").trim())
        .find((t) => /^\d{4}$/.test(t));
      if (guess) storedText = guess;
    }
    let normalizedStored = storedText.replace(/\D/g, "");
    if (normalizedStored.length > 0 && normalizedStored.length < 4) {
      normalizedStored = normalizedStored.padStart(4, "0");
    }

    if (normalizedStored !== pinRaw) {
      return res.status(401).json({ ok: false, error: "Invalid email or PIN." });
    }

    return res.json({ ok: true, name: contractor.name || "" });
  } catch (e) {
    console.error("ERROR /auth/login:", e);
    return res.status(500).json({ ok: false, error: "Server error." });
  }
});

// ---------------- JOBS ----------------

// GET /jobs/my?email=... [&on=YYYY-MM-DD&includeWeekends=0]
app.get("/jobs/my", async (req, res) => {
  try {
    const email = String(req.query.email || "").trim().toLowerCase();
    if (!email) return res.status(400).json({ error: "email query required" });

    const onDate = String(req.query.on || "").trim(); // optional YYYY-MM-DD
    const includeWeekends = String(req.query.includeWeekends || "1") !== "0";

    const isWeekend = (iso) => {
      const dt = new Date(`${iso}T12:00:00Z`);
      const dow = dt.getUTCDay();
      return dow === 0 || dow === 6;
    };

    // 1) Fetch contractors (paged)
    const contractorsQ = `
      query($boardId:ID!, $cursor:String) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items { id name column_values { id text value } }
          }
        }
      }`;
    let cCursor = null;
    let contractors = [];
    do {
      const d = await monday(contractorsQ, {
        boardId: CONTRACTORS_BOARD_ID,
        cursor: cCursor,
      });
      const page = d.boards[0].items_page;
      contractors = contractors.concat(page.items);
      cCursor = page.cursor || null;
    } while (cCursor);

    const contractor = contractors.find((it) =>
      it.column_values?.some(
        (cv) =>
          cv.id === CONTRACTORS_EMAIL_COLUMN_ID &&
          String(cv.text || "").trim().toLowerCase() === email
      )
    );
    if (!contractor) return res.json({ items: [] });
    const contractorId = String(contractor.id);

    // 2) Fetch jobs + subitems (paged)
    const jobsQ = `
      query($boardId:ID!, $cursor:String) {
        boards(ids: [$boardId]) {
          items_page(limit: 50, cursor: $cursor) {
            cursor
            items {
              id
              name
              column_values { id text value }
              subitems {
                id
                name
                column_values { id text value }
              }
            }
          }
        }
      }`;
    let jCursor = null;
    const results = [];
    do {
      const d = await monday(jobsQ, { boardId: JOBS_BOARD_ID, cursor: jCursor });
      const page = d.boards[0].items_page;
      jCursor = page.cursor || null;

      for (const job of page.items) {
        const jobCols = Object.fromEntries((job.column_values || []).map((cv) => [cv.id, cv]));
        const address = jobCols[JOBS_ADDRESS_COLUMN_ID]?.text || "";

        for (const s of job.subitems || []) {
          const sCols = Object.fromEntries((s.column_values || []).map((cv) => [cv.id, cv]));
          // match by linked contractor or subitem email
          const linkedIds = parseConnectIds(sCols[SUBITEMS_CONTRACTOR_COLUMN_ID]?.value);
          const matchByLink = linkedIds.includes(contractorId);
          const subEmailColId = SUBITEMS_EMAIL_COLUMN_ID;
          const subEmailText = subEmailColId ? (sCols[subEmailColId]?.text || "").trim().toLowerCase() : "";
          const matchByEmail = subEmailColId ? subEmailText === email : false;
          if (!(matchByLink || matchByEmail)) continue;

          const jobNumber = sCols[SUBITEMS_JOBNUMBER_COLUMN_ID]?.text || "";
          const description = sCols[SUBITEMS_DESCRIPTION_COLUMN_ID]?.text || "";

          let startDate = "", endDate = "";
          const tlVal = sCols[SUBITEMS_TIMELINE_COLUMN_ID]?.value;
          if (tlVal) {
            try {
              const tl = JSON.parse(tlVal);
              startDate = tl.from || "";
              endDate = tl.to || tl.from || "";
            } catch {}
          }

          const row = {
            parentJobId: job.id,
            parentJobName: job.name,
            address,
            subitemId: s.id,
            subitemName: s.name,
            jobNumber,
            description,
            timeline: { startDate, endDate },
          };

          // If /jobs/my?on=YYYY-MM-DD and excluding weekends, filter here
          if (onDate) {
            if (!includeWeekends && isWeekend(onDate)) {
              // skip weekend dates entirely
            } else {
              const sISO = startDate || "";
              const eISO = endDate || sISO;
              if (onDate >= sISO && onDate <= eISO) {
                results.push(row);
              }
            }
          } else {
            results.push(row);
          }
        }
      }
    } while (jCursor);

    res.json({ items: results });
  } catch (e) {
    console.error("ERROR /jobs/my:", e);
    res.status(500).json({ error: e.message });
  }
});

// GET /jobs/:subitemId/details — returns filesByColumn + flat files list
app.get("/jobs/:subitemId/details", async (req, res) => {
  const subitemId = String(req.params.subitemId);
  const fileColumnIds = getFileColumnIds();

  try {
    const q = `
      query($id: [ID!]) {
        items(ids: $id) {
          id
          name
          column_values { id text value }
        }
      }`;
    const d = await monday(q, { id: [subitemId] });
    const item = d?.items?.[0];
    if (!item) {
      return res.json({ item: null, files: [], filesByColumn: {}, columnIds: fileColumnIds });
    }

    const filesByColumn = {};
    const flat = [];
    const allAssetIds = [];

    const filtered = (item.column_values || []).filter((cv) => fileColumnIds.includes(cv.id));

    for (const cv of filtered) {
      let files = [];
      if (cv?.value) {
        try {
          const v = typeof cv.value === "string" ? JSON.parse(cv.value) : cv.value;
          if (v && Array.isArray(v.files)) {
            files = v.files.map((f) => {
              const assetId = f?.assetId ? String(f.assetId) : null;
              if (assetId) allAssetIds.push(assetId);
              const obj = { columnId: cv.id, name: String(f?.name ?? "file"), assetId, url: f?.url || null };
              flat.push(obj);
              return { name: obj.name, assetId: obj.assetId, url: obj.url };
            });
          }
        } catch (err) {
          console.warn("Parse file column failed:", cv.id, err?.message || err);
        }
      }
      filesByColumn[cv.id] = files;
    }

    // Resolve asset URLs where missing
    let assets = {};
    if (allAssetIds.length > 0) {
      try {
        assets = await resolveAssets(allAssetIds);
      } catch (err) {
        console.warn("resolveAssets error:", err?.message || err);
      }
    }

    const enrichedFlat = flat.map((f) => {
      if (!f.url && f.assetId && assets[f.assetId]) {
        f.url = assets[f.assetId].public_url || assets[f.assetId].url || null;
      }
      return f;
    });

    res.json({
      item: { id: item.id, name: item.name },
      files: enrichedFlat,
      filesByColumn,
      columnIds: fileColumnIds,
    });
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

// GET /files/:assetId — simple redirect to Monday file URL
app.get("/files/:assetId", async (req, res) => {
  try {
    const assetId = String(req.params.assetId).trim();
    const q = `
      query($ids: [ID!]!) {
        assets(ids: $ids) {
          id
          url
          public_url
          name
          file_extension
        }
      }`;
    const d = await monday(q, { ids: [assetId] });
    const a = d?.assets?.[0];
    if (!a || !(a.public_url || a.url)) {
      return res.status(404).send("No URL available for this file.");
    }
    return res.redirect(a.public_url || a.url);
  } catch (e) {
    console.error("FILE PROXY fatal error:", e?.message || e);
    res.status(500).send("Could not resolve file.");
  }
});

// POST /upload — add a base64 image to a file column on a subitem
app.post("/upload", async (req, res) => {
  try {
    const { jobId, columnId = "files", fileName = "photo.jpg", base64 } = req.body;
    if (!base64) return res.status(400).json({ error: "base64 required" });

    const buf = Buffer.from(base64, "base64");
    const form = new FormData();
    const gql = `mutation ($file: File!) {
      add_file_to_column(item_id:${Number(jobId)}, column_id:"${columnId}", file:$file) { id }
    }`;
    form.append("query", gql);
    form.append("variables[file]", buf, { filename: fileName, contentType: "image/jpeg" });

    const r = await monday("", {}, true, form);
    res.json(r);
  } catch (e) {
    console.error("ERROR /upload:", e);
    res.status(500).json({ error: e.message });
  }
});

// ---------------- TIMESHEETS ----------------

// Helper: find a group id by (partial) title, fallback to first group.
async function findGroupId(boardId, titleIncludes /* string, case-insensitive */) {
  const q = `
    query($id: [ID!]) {
      boards(ids: $id) {
        groups { id title }
      }
    }`;
  const d = await monday(q, { id: [boardId] });
  const groups = d?.boards?.[0]?.groups || [];
  const match = groups.find((g) => String(g.title || "").toLowerCase().includes(String(titleIncludes).toLowerCase()));
  return match ? match.id : (groups[0]?.id || null);
}

// POST /timesheets — create item in "To be Approved" (or fallback group)
app.post("/timesheets", async (req, res) => {
  try {
    const {
      email,
      workerName,
      jobNumber,
      date, // ISO yyyy-mm-dd
      startNum,
      endNum,
      tookLunch,
      totalHours,
      jobComplete,
      notes,
    } = req.body || {};

    if (!TIMESHEETS_BOARD_ID) {
      return res.status(400).json({ error: "TIMESHEETS_BOARD_ID not set" });
    }

    // Resolve group
    const groupId = await findGroupId(TIMESHEETS_BOARD_ID, "to be approved");

    // Build column values object
    const col = {};

    if (TS_NAME_COLUMN_ID && workerName) col[TS_NAME_COLUMN_ID] = String(workerName);
    if (TS_JOBNUMBER_TEXT_COLUMN_ID && jobNumber) col[TS_JOBNUMBER_TEXT_COLUMN_ID] = String(jobNumber);

    if (TS_DATE_COLUMN_ID && date) col[TS_DATE_COLUMN_ID] = { date: String(date) };

    if (TS_START_NUM_COLUMN_ID && (startNum || startNum === 0)) col[TS_START_NUM_COLUMN_ID] = String(startNum);
    if (TS_FINISH_NUM_COLUMN_ID && (endNum || endNum === 0)) col[TS_FINISH_NUM_COLUMN_ID] = String(endNum);

    // Lunch (prefer text Yes/No)
    if (TS_LUNCH_TEXT_COLUMN_ID) {
      col[TS_LUNCH_TEXT_COLUMN_ID] = tookLunch ? "Yes" : "No";
    } else if (TS_LUNCH_BOOL_DROPDOWN_ID) {
      // Dropdown expects label indexes. We'll use 1 for Yes, 0 for No.
      col[TS_LUNCH_BOOL_DROPDOWN_ID] = { labels: [tookLunch ? "Yes" : "No"] };
    }

    if (TS_TOTAL_HOURS_NUM_COLUMN_ID && (totalHours || totalHours === 0)) col[TS_TOTAL_HOURS_NUM_COLUMN_ID] = String(totalHours);

    if (TS_NOTES_LONGTEXT_COLUMN_ID && notes != null) col[TS_NOTES_LONGTEXT_COLUMN_ID] = String(notes);

    if (TS_JOB_COMPLETE_TEXT_COLUMN_ID) col[TS_JOB_COMPLETE_TEXT_COLUMN_ID] = jobComplete ? "Yes" : "No";

    const itemName = `${jobNumber || "Timesheet"} — ${date || ""}`;

    const mutation = `
      mutation CreateTS($boardId: ID!, $groupId: String, $itemName: String!, $columnVals: JSON!) {
        create_item(board_id: $boardId, group_id: $groupId, item_name: $itemName, column_values: $columnVals) { id }
      }`;
    const variables = {
      boardId: Number(TIMESHEETS_BOARD_ID),
      groupId,
      itemName,
      columnVals: JSON.stringify(col),
    };
    const r = await monday(mutation, variables);
    res.json({ ok: true, id: r?.create_item?.id || null });
  } catch (e) {
    console.error("ERROR /timesheets:", e);
    res.status(500).json({ error: e.message });
  }
});

// GET /timesheets?name=John%20Smith&jobNumber=2688-2&limit=50
app.get("/timesheets", async (req, res) => {
  try {
    const name = String(req.query.name || "").trim();
    const jobNumberFilter = String(req.query.jobNumber || "").trim();
    const limit = Math.max(1, Math.min(200, Number(req.query.limit || 50)));

    const q = `
      query($boardId:ID!, $cursor:String) {
        boards(ids: [$boardId]) {
          items_page(limit: 100, cursor: $cursor) {
            cursor
            items {
              id
              name
              group { id title }
              column_values { id text value }
            }
          }
        }
      }`;
    let cursor = null;
    const all = [];

    do {
      const d = await monday(q, { boardId: TIMESHEETS_BOARD_ID, cursor });
      const page = d?.boards?.[0]?.items_page;
      cursor = page?.cursor || null;

      for (const it of page?.items || []) {
        const cvs = Object.fromEntries((it.column_values || []).map((cv) => [cv.id, cv]));

        const workerNameText = TS_NAME_COLUMN_ID ? (cvs[TS_NAME_COLUMN_ID]?.text || "") : "";
        if (name && workerNameText && workerNameText.trim() !== name) continue;

        const jobNumberText = TS_JOBNUMBER_TEXT_COLUMN_ID ? (cvs[TS_JOBNUMBER_TEXT_COLUMN_ID]?.text || "") : "";
        if (jobNumberFilter && jobNumberText.trim() !== jobNumberFilter) continue;

        // date ISO
        let dateISO = "";
        if (TS_DATE_COLUMN_ID) {
          const v = cvs[TS_DATE_COLUMN_ID]?.value;
          if (v) {
            try {
              const parsed = typeof v === "string" ? JSON.parse(v) : v;
              dateISO = parsed?.date || "";
            } catch {}
          }
        }

        // start/end as 4-digit strings
        const start4 = to4(cvs[TS_START_NUM_COLUMN_ID]?.text || "");
        const end4 = to4(cvs[TS_FINISH_NUM_COLUMN_ID]?.text || "");

        // total hours
        const totalHours = Number((cvs[TS_TOTAL_HOURS_NUM_COLUMN_ID]?.text || "").replace(",", ".")) || 0;

        // notes
        const notes = cvs[TS_NOTES_LONGTEXT_COLUMN_ID]?.text || "";

        // status by group title
        const groupTitle = String(it.group?.title || "").toLowerCase();
        const approved = groupTitle.includes("approved - upcoming payroll") || groupTitle.includes("payroll processed");
        const status = approved ? "approved" : "pending";

        all.push({
          id: it.id,
          itemName: it.name,
          dateISO,
          start4,
          end4,
          totalHours,
          jobNumber: jobNumberText,
          workerName: workerNameText,
          notes,
          status,
        });
      }
    } while (cursor);

    // sort newest first by dateISO then end time
    all.sort((a, b) => {
      if (a.dateISO > b.dateISO) return -1;
      if (a.dateISO < b.dateISO) return 1;
      return Number(b.end4) - Number(a.end4);
    });

    res.json({ items: all.slice(0, limit) });
  } catch (e) {
    console.error("ERROR GET /timesheets:", e);
    res.status(500).json({ error: e.message });
  }
});

app.listen(Number(PORT), () => {
  console.log("API running on :" + PORT);
});