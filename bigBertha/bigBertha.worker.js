// Helper to read and sanitize CSV header to SQL-safe identifiers
function readCsvHeaderSync(filePath) {
  const fd = fs.openSync(filePath, "r");
  try {
    const buf = Buffer.alloc(65536); // 64KB is plenty for a header line
    const bytes = fs.readSync(fd, buf, 0, buf.length, 0);
    const text = buf.slice(0, bytes).toString("utf8");
    const nl = text.indexOf("\n");
    const line = (nl >= 0 ? text.slice(0, nl) : text).replace(/\r$/, "");
    // strip BOM
    const noBom = line.replace(/^\uFEFF/, "");
    // split by commas not inside quotes
    const parts = noBom
      .match(/(?:^|,)("(?:[^"]|"")*"|[^,]*)/g)
      .map((s) => s.replace(/^,/, ""))
      .map((s) => {
        if (s.startsWith('"') && s.endsWith('"')) {
          return s.slice(1, -1).replace(/""/g, '"');
        }
        return s;
      });
    // sanitize to safe SQL identifiers (keep camelCase, remove spaces)
    const cols = parts.map((h, i) => {
      const t = String(h || "").trim();
      const base = t.length ? t : `col_${i + 1}`;
      // allow letters, numbers, underscore; keep other chars out
      return base.replace(/[^A-Za-z0-9_]/g, "_");
    });
    return cols;
  } finally {
    fs.closeSync(fd);
  }
}
function normIdent(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}
const fs = require("node:fs");
const path = require("node:path");
const { logger } = require("../helpers/logger");
const db = require("../db/database");
const { parseDateLike } = require("../helpers/dateNormaliser");
const { normalizeAmountLike } = require("../helpers/amountNormaliser");
const pool = db.getPgPool();
const { from: copyFrom } = require("pg-copy-streams");
const fastCsv = require("fast-csv");
const SCHEMA = process.env.DB_SCHEMA || "public";
const { pipeline } = require("node:stream/promises");

const ABS_PAYMENT_FOR_CUSTOMERS = (process.env.ABS_PAYMENT_FOR_CUSTOMERS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

async function uploadsLocalRoute(req, res) {
  const uploadDir = process.env.LOCAL_UPLOAD_DIR || "/tmp/uploads";
  const fullUploadDir = path.resolve(uploadDir);
  logger.logEvent("info", "Upload debug: resolved upload dir", {
    uploadDir,
    fullUploadDir,
  });

  if (!fs.existsSync(fullUploadDir)) {
    fs.mkdirSync(fullUploadDir, { recursive: true });
  }

  const file = req.files?.file;
  if (!file) {
    return res.status(400).json({ error: "No file uploaded" });
  }

  const tempPath = file.tempFilePath || file.path;
  const targetPath = path.join(fullUploadDir, file.name);
  logger.logEvent("info", "Upload debug: targetPath", { targetPath });

  try {
    fs.renameSync(tempPath, targetPath);
    const existsAfterMove = fs.existsSync(targetPath);
    logger.logEvent("info", "Upload debug: file moved", {
      targetPath,
      existsAfterMove,
    });
  } catch (err) {
    return res.status(500).json({ error: "Failed to move uploaded file" });
  }

  res.json({ message: "File uploaded successfully", path: targetPath });
}

async function ingest(filePath) {
  const exists = fs.existsSync(filePath);
  logger.logEvent("info", "Ingest debug: checking file existence", {
    filePath,
    exists,
  });

  if (!exists) {
    throw new Error(`File does not exist: ${filePath}`);
  }
  // Ingest logic here...
}

async function processCsvJob({ jobId, filePath, customerId, ptrsId }) {
  const client = await pool.connect();
  let processed = 0;
  const absForThisCustomer = ABS_PAYMENT_FOR_CUSTOMERS.includes(customerId);
  try {
    await client.query("BEGIN");
    // Ensure RLS tenant context is set for this transaction/connection
    await client.query(
      "SELECT set_config('app.current_customer_id', $1, true)",
      [customerId]
    );
    const { rows: rlsChk } = await client.query(
      "SELECT current_setting('app.current_customer_id', true) AS cid"
    );
    logger.logEvent("info", "RLS set for worker tx", {
      meta: { customerIdSet: rlsChk?.[0]?.cid },
    });
    await client.query(
      "UPDATE " +
        SCHEMA +
        '.tbl_ingest_job SET status=\'running\', "startedAt"=now(), "updatedAt"=now() WHERE id=$1',
      [jobId]
    );

    // 1) Derive columns from the CSV header, then COPY with HEADER (no column list)
    const hdrCols = readCsvHeaderSync(filePath).map((c) =>
      c.replace(/^f\./i, "")
    );
    logger.logEvent("info", "Ingest debug: derived header columns", {
      meta: { hdrCols },
    });

    // Build normalized lookup (case/format insensitive)
    const normToActual = Object.create(null);
    for (const c of hdrCols) normToActual[normIdent(c)] = c;

    // Candidate aliases (normalized) for each required logical field
    // Candidate aliases (normalized) for required fields (must exist)
    const REQ_CANDS = {
      payerEntityName: [
        "payerentityname",
        "payer_entity_name",
        "payername",
        "payer",
        "suppliername",
        "supplier",
      ],
      payeeEntityName: [
        "payeeentityname",
        "payee_entity_name",
        "payeename",
        "payee",
        "vendorname",
        "vendor",
      ],
      paymentAmount: [
        "paymentamount",
        "payment_amount",
        "amount",
        "invoiceamount",
        "totalamount",
        "paymentvalue",
      ],
      paymentDate: [
        "paymentdate",
        "payment_date",
        "date",
        "invoicedate",
        "transactiondate",
      ],
    };

    // Optional columns — use if present
    const OPT_CANDS = {
      payerEntityAbn: [
        "payerentityabn",
        "payerabn",
        "payer_abn",
        "supplierabn",
        "abn",
      ],
      payerEntityAcnArbn: [
        "payerentityacnarbn",
        "payeracnarbn",
        "payer_acn_arbn",
        "supplieracnarbn",
        "acn",
        "arbn",
      ],
      payeeEntityAbn: ["payeeentityabn", "payeeabn", "payee_abn", "abn"],
      payeeEntityAcnArbn: [
        "payeeentityacnarbn",
        "payeeacnarbn",
        "payee_acn_arbn",
        "acn",
        "arbn",
      ],
      // additional optional date-like fields we may receive/mirror
      invoiceReceiptDate: [
        "invoicereceiptdate",
        "invoice_receipt_date",
        "documentdate",
        "document_date",
        "docdate",
        "doc_date",
      ],
      invoiceIssueDate: [
        "invoiceissuedate",
        "invoice_issue_date",
        "issuedate",
        "issue_date",
      ],
      invoiceDueDate: [
        "invoiceduedate",
        "invoice_due_date",
        "duedate",
        "due_date",
      ],
      supplyDate: ["supplydate", "supply_date", "servicedate", "service_date"],
      noticeForPaymentIssueDate: [
        "noticeforpaymentissuedate",
        "notice_for_payment_issue_date",
        "noticeissuedate",
        "notice_issue_date",
      ],
      postingDate: ["postingdate", "posting_date"],
      clearingDate: ["clearingdate", "clearing_date"],
    };

    const resolvedReq = {};
    const missing = [];
    for (const [key, cands] of Object.entries(REQ_CANDS)) {
      let hitName = null;
      for (const cand of cands) {
        if (normToActual[cand]) {
          hitName = normToActual[cand];
          break;
        }
      }
      if (hitName) resolvedReq[key] = hitName;
      else missing.push(key);
    }
    if (missing.length) {
      throw new Error(`Missing required CSV columns: ${missing.join(", ")}`);
    }

    const resolvedOpt = {};
    for (const [key, cands] of Object.entries(OPT_CANDS)) {
      let hitName = null;
      for (const cand of cands) {
        if (normToActual[cand]) {
          hitName = normToActual[cand];
          break;
        }
      }
      if (hitName) resolvedOpt[key] = hitName; // else leave undefined
    }

    logger.logEvent("info", "Ingest debug: resolved header mapping", {
      meta: { required: resolvedReq, optional: resolvedOpt },
    });

    // Determine which CSV headers correspond to date-like fields (present in this file)
    const dateHeaderNames = [];
    if (resolvedReq.paymentDate) dateHeaderNames.push(resolvedReq.paymentDate);
    const dateLikeOptKeys = [
      "invoiceReceiptDate",
      "invoiceIssueDate",
      "invoiceDueDate",
      "supplyDate",
      "noticeForPaymentIssueDate",
      "postingDate",
      "clearingDate",
    ];
    for (const k of dateLikeOptKeys) {
      if (resolvedOpt[k]) dateHeaderNames.push(resolvedOpt[k]);
    }

    if (!hdrCols || hdrCols.length === 0) {
      throw new Error("CSV appears to have an empty or unreadable header");
    }
    const colDefs = hdrCols.map((c) => `"${c}" text`).join(",");
    await client.query(`CREATE TEMP TABLE _file_in (${colDefs})`);

    const copyInSql = `COPY _file_in FROM STDIN WITH (FORMAT csv, HEADER true)`;
    const copyIn = client.query(copyFrom(copyInSql));

    // Stream-parse the CSV, normalize any date-like headers, and stream out with original headers.
    const readStream = fs.createReadStream(filePath);
    const csvParser = fastCsv
      .parse({ headers: true, ignoreEmpty: true })
      .transform((row) => {
        for (const h of dateHeaderNames) {
          if (h && Object.prototype.hasOwnProperty.call(row, h)) {
            const raw = row[h];
            const parsed = parseDateLike(raw);
            // Keep original if parser fails; INSERT path will route invalids to error table.
            row[h] = parsed ? parsed : raw;
          }
        }
        // Normalize payment amount (strip thousands separators, currency, spaces; keep sign)
        if (
          resolvedReq.paymentAmount &&
          Object.prototype.hasOwnProperty.call(row, resolvedReq.paymentAmount)
        ) {
          const rawAmt = row[resolvedReq.paymentAmount];
          const cleaned = normalizeAmountLike(rawAmt);
          // Use cleaned when available; otherwise keep original so validation can flag it
          if (cleaned != null) {
            row[resolvedReq.paymentAmount] = cleaned;
          }
        }
        return row;
      });

    const csvFormatter = fastCsv.format({ headers: hdrCols });

    await pipeline(readStream, csvParser, csvFormatter, copyIn);

    // 2) Record processed count and load into raw staging with job metadata
    const { rows: cntRows } = await client.query(
      `SELECT COUNT(*)::bigint AS n FROM _file_in`
    );
    processed = Number(cntRows[0].n || 0);

    // Discover available columns in the staging table and build a safe insert
    const { rows: rawCols } = await client.query(
      `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
      [SCHEMA, "tbl_tcp_import_raw"]
    );
    const available = new Set(rawCols.map((r) => r.column_name));

    // Required meta columns in staging
    const metaRequired = ["jobId", "customerId", "ptrsId", "rowNumber"];
    const missingMeta = metaRequired.filter((c) => !available.has(c));
    if (missingMeta.length) {
      throw new Error(
        `Staging table ${SCHEMA}.tbl_tcp_import_raw missing required columns: ${missingMeta.join(", ")}`
      );
    }

    const insertCols = [
      '"jobId"',
      '"customerId"',
      '"ptrsId"',
      '"rowNumber"',
      '"createdAt"',
      '"updatedAt"',
    ];
    const selectExprs = [
      "$1",
      "$2",
      "$3",
      "ROW_NUMBER() OVER ()",
      "now()",
      "now()",
    ];

    // Optional audit columns if the staging table has them
    if (available.has("createdBy")) {
      insertCols.push('"createdBy"');
      selectExprs.push("'system'");
    }
    if (available.has("updatedBy")) {
      insertCols.push('"updatedBy"');
      selectExprs.push("'system'");
    }

    // Ensure each inserted row gets a primary key id (server-side, per-row)
    if (available.has("id")) {
      insertCols.unshift(`"id"`);
      // use md5(random||clock||jobId) -> 10 chars; avoids JS loops
      selectExprs.unshift(
        "substr(md5(random()::text || clock_timestamp()::text || $1), 1, 10)"
      );
    }

    function maybeAdd(colName, expr) {
      if (available.has(colName)) {
        insertCols.push(`"${colName}"`);
        selectExprs.push(expr);
      }
    }

    // Map canonical logical fields to staging columns if present
    maybeAdd("payerEntityName", `f."${resolvedReq.payerEntityName}"`);
    maybeAdd(
      "payerEntityAbn",
      resolvedOpt.payerEntityAbn ? `f."${resolvedOpt.payerEntityAbn}"` : "NULL"
    );
    maybeAdd(
      "payerEntityAcnArbn",
      resolvedOpt.payerEntityAcnArbn
        ? `f."${resolvedOpt.payerEntityAcnArbn}"`
        : "NULL"
    );
    maybeAdd("payeeEntityName", `f."${resolvedReq.payeeEntityName}"`);
    maybeAdd(
      "payeeEntityAbn",
      resolvedOpt.payeeEntityAbn ? `f."${resolvedOpt.payeeEntityAbn}"` : "NULL"
    );
    maybeAdd(
      "payeeEntityAcnArbn",
      resolvedOpt.payeeEntityAcnArbn
        ? `f."${resolvedOpt.payeeEntityAcnArbn}"`
        : "NULL"
    );
    maybeAdd("paymentAmount", `f."${resolvedReq.paymentAmount}"`);
    maybeAdd("paymentDate", `f."${resolvedReq.paymentDate}"`);

    const insertSql = `
      INSERT INTO ${SCHEMA}."tbl_tcp_import_raw"
        (${insertCols.join(", ")})
      SELECT ${selectExprs.join(", ")}
      FROM _file_in f
    `;

    await client.query(insertSql, [jobId, customerId, ptrsId]);

    // Persist rowsProcessed after COPY/load
    await client.query(
      `UPDATE ${SCHEMA}.tbl_ingest_job
         SET "rowsProcessed" = $2, "updatedAt" = now()
       WHERE id = $1`,
      [jobId, processed]
    );

    // Create normalized temp table using only columns that exist in staging
    function colOrNull(name, wrap = (x) => x) {
      if (available.has(name)) return wrap(`"${name}"`);
      return "NULL";
    }

    const normSql = `
      CREATE TEMP TABLE _norm AS
      SELECT
        "jobId" AS jobId,
        "customerId" AS customerId,
        "ptrsId" AS ptrsId,
        ${colOrNull("payerEntityName", (c) => `trim(${c})`)} AS payerEntityName,
        ${colOrNull("payerEntityAbn", (c) => `regexp_replace(COALESCE(${c},''), '[^0-9]', '', 'g')`)} AS payerEntityAbn,
        ${colOrNull("payerEntityAcnArbn", (c) => `regexp_replace(COALESCE(${c},''), '[^0-9]', '', 'g')`)} AS payerEntityAcnArbn,
        ${colOrNull("payeeEntityName", (c) => `trim(${c})`)} AS payeeEntityName,
        ${colOrNull("payeeEntityAbn", (c) => `regexp_replace(COALESCE(${c},''), '[^0-9]', '', 'g')`)} AS payeeEntityAbn,
        ${colOrNull("payeeEntityAcnArbn", (c) => `regexp_replace(COALESCE(${c},''), '[^0-9]', '', 'g')`)} AS payeeEntityAcnArbn,
        ${colOrNull("paymentAmount", (c) => `NULLIF(trim(${c}), '')`)} AS amount_txt,
        ${colOrNull("paymentDate", (c) => `NULLIF(trim(${c}), '')`)} AS date_txt  -- may already be ISO via parseDateLike
      FROM ${SCHEMA}."tbl_tcp_import_raw"
      WHERE "jobId" = $1
    `;

    await client.query(normSql, [jobId]);

    // Valid: numeric amount and a parsable date
    const insertValidSql = `INSERT INTO ${SCHEMA}."tbl_tcp" (
         "id",
         "payerEntityName","payerEntityAbn","payerEntityAcnArbn",
         "payeeEntityName","payeeEntityAbn","payeeEntityAcnArbn",
         "paymentAmount","paymentDate",
         "source","createdBy","updatedBy","customerId","ptrsId",
         "createdAt","updatedAt"
       )
       SELECT
         substr(md5(random()::text || clock_timestamp()::text), 1, 10) AS id,
         payerEntityName,
         CASE WHEN payerEntityAbn ~ '^[0-9]{11}$' THEN payerEntityAbn::bigint ELSE NULL END,
         CASE WHEN payerEntityAcnArbn ~ '^[0-9]+$' THEN payerEntityAcnArbn::bigint ELSE NULL END,
         payeeEntityName,
         CASE WHEN payeeEntityAbn ~ '^[0-9]{11}$' THEN payeeEntityAbn::bigint ELSE NULL END,
         CASE WHEN payeeEntityAcnArbn ~ '^[0-9]+$' THEN payeeEntityAcnArbn::bigint ELSE NULL END,
         CASE WHEN $3 THEN abs(NULLIF(amount_txt,'')::numeric) ELSE NULLIF(amount_txt,'')::numeric END,
         CASE WHEN date_txt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN date_txt::timestamp ELSE NULL END,
         'csv_upload','system','system',$1,$2,now(),now()
       FROM _norm
       WHERE payerEntityName IS NOT NULL AND payerEntityName <> ''
         AND payeeEntityName IS NOT NULL AND payeeEntityName <> ''
         AND amount_txt ~ '^-?[0-9]+(\\.[0-9]+)?$'
         AND (date_txt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' OR date_txt IS NULL)`;

    const { rowCount: rowsValid } = await client.query(insertValidSql, [
      customerId,
      ptrsId,
      absForThisCustomer,
    ]);

    // Errors: everything else goes to error table
    const insertErrorSql = `INSERT INTO ${SCHEMA}."tbl_tcp_error" (
         "id",
         "payerEntityName","payerEntityAbn","payerEntityAcnArbn",
         "payeeEntityName","payeeEntityAbn","payeeEntityAcnArbn",
         "paymentAmount","paymentDate","errorReason",
         "createdBy","updatedBy","customerId","ptrsId",
         "createdAt","updatedAt"
       )
       SELECT
         substr(md5(random()::text || clock_timestamp()::text), 1, 10) AS id,
         payerEntityName,
         CASE WHEN payerEntityAbn ~ '^[0-9]{11}$' THEN payerEntityAbn::bigint ELSE NULL END,
         CASE WHEN payerEntityAcnArbn ~ '^[0-9]+$' THEN payerEntityAcnArbn::bigint ELSE NULL END,
         payeeEntityName,
         CASE WHEN payeeEntityAbn ~ '^[0-9]{11}$' THEN payeeEntityAbn::bigint ELSE NULL END,
         CASE WHEN payeeEntityAcnArbn ~ '^[0-9]+$' THEN payeeEntityAcnArbn::bigint ELSE NULL END,
         CASE WHEN amount_txt ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (CASE WHEN $3 THEN abs(amount_txt::numeric) ELSE amount_txt::numeric END) ELSE NULL END,
         CASE WHEN date_txt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN date_txt::timestamp ELSE NULL END,
         jsonb_build_object(
           'type', CASE
             WHEN payerEntityName IS NULL OR payerEntityName = '' THEN 'MISSING_PAYER_NAME'
             WHEN payeeEntityName IS NULL OR payeeEntityName = '' THEN 'MISSING_PAYEE_NAME'
             WHEN amount_txt IS NULL OR amount_txt = '' THEN 'MISSING_AMOUNT'
             WHEN amount_txt !~ '^-?[0-9]+(\\.[0-9]+)?$' THEN 'INVALID_AMOUNT'
             WHEN date_txt IS NOT NULL AND date_txt !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 'INVALID_DATE'
             ELSE 'VALIDATION_FAILED'
           END,
           'message', CASE
             WHEN payerEntityName IS NULL OR payerEntityName = '' THEN 'Missing payerEntityName'
             WHEN payeeEntityName IS NULL OR payeeEntityName = '' THEN 'Missing payeeEntityName'
             WHEN amount_txt IS NULL OR amount_txt = '' THEN 'Missing amount'
             WHEN amount_txt !~ '^-?[0-9]+(\\.[0-9]+)?$' THEN 'Invalid amount'
             WHEN date_txt IS NOT NULL AND date_txt !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN 'Invalid date'
             ELSE 'Validation failed'
           END,
           'raw', jsonb_build_object(
             'payerEntityName', COALESCE(payerEntityName,''),
             'payeeEntityName', COALESCE(payeeEntityName,''),
             'amount', COALESCE(amount_txt,''),
             'date', COALESCE(date_txt,''),
             'payerEntityAbn', COALESCE(payerEntityAbn,''),
             'payerEntityAcnArbn', COALESCE(payerEntityAcnArbn,''),
             'payeeEntityAbn', COALESCE(payeeEntityAbn,''),
             'payeeEntityAcnArbn', COALESCE(payeeEntityAcnArbn,'')
           )
         ),
         'system','system',$1,$2,now(),now()
       FROM _norm
       WHERE NOT (
         payerEntityName IS NOT NULL AND payerEntityName <> '' AND
         payeeEntityName IS NOT NULL AND payeeEntityName <> '' AND
         amount_txt ~ '^-?[0-9]+(\\.[0-9]+)?$' AND
         (date_txt ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' OR date_txt IS NULL)
       )`;

    const { rowCount: rowsErrored } = await client.query(insertErrorSql, [
      customerId,
      ptrsId,
      absForThisCustomer,
    ]);

    await client.query(
      "UPDATE " +
        SCHEMA +
        '.tbl_ingest_job SET "rowsValid"=$2, "rowsErrored"=$3, "updatedAt"=now() WHERE id=$1',
      [jobId, rowsValid, rowsErrored]
    );

    await client.query(
      "UPDATE " +
        SCHEMA +
        '.tbl_ingest_job SET status=\'complete\', "rowsProcessed"=$2, "finishedAt"=now(), "updatedAt"=now() WHERE id=$1',
      [jobId, processed]
    );

    await client.query("COMMIT");
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    try {
      await client.query(
        "UPDATE " +
          SCHEMA +
          '.tbl_ingest_job SET status=\'failed\', "lastError"=$2, "finishedAt"=now(), "updatedAt"=now() WHERE id=$1',
        [
          jobId,
          error && error.message
            ? error.message.substring(0, 500)
            : String(error).substring(0, 500),
        ]
      );
    } catch (_) {}
    logger.logEvent("error", "[worker] failed", {
      meta: { error: error.message },
    });
  } finally {
    client.release();
  }
}

module.exports = { uploadsLocalRoute, ingest, processCsvJob };
