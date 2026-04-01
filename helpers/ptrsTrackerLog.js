const fs = require("fs");
const path = require("path");
const os = require("os");

function _traceNowIso() {
  return new Date().toISOString();
}

function hrMsSince(startNs) {
  const end = process.hrtime.bigint();
  return Number(end - startNs) / 1e6;
}

function _safeFilePart(v) {
  return String(v || "")
    .replace(/[^a-zA-Z0-9._-]/g, "_")
    .slice(0, 80);
}

/**
 * Opt-in trace writer for performance diagnostics.
 * Writes JSON Lines to a file (one event per line).
 *
 * Enable with:
 *  PTRS_TRACE=1
 * Optional:
 *  PTRS_TRACE_DIR=/some/dir   (defaults to OS tmp dir)
 *
 * @param {Object} opts
 * @param {string} opts.customerId
 * @param {string} opts.ptrsId
 * @param {string|null} [opts.actorId]
 * @param {function|null} [opts.logInfo] - optional logging function (msg, meta)
 * @param {function|null} [opts.meta] - optional meta wrapper function
 * @returns {object|null}
 */
function createPtrsTrace({
  customerId,
  ptrsId,
  actorId = null,
  logInfo = null,
  meta = null,
}) {
  const enabled =
    String(process.env.PTRS_TRACE || "").trim() === "1" ||
    String(process.env.PTRS_TRACE || "")
      .trim()
      .toLowerCase() === "true";

  if (!enabled) return null;

  let dir = String(process.env.PTRS_TRACE_DIR || "").trim() || os.tmpdir();

  // Allow relative paths in .env (resolve from repo root / process cwd)
  const isAbs = path.isAbsolute(dir) || /^[a-zA-Z]:[\\/]/.test(dir); // Windows drive
  if (!isAbs) {
    dir = path.resolve(process.cwd(), dir);
  }

  try {
    fs.mkdirSync(dir, { recursive: true });
  } catch (_) {
    return null;
  }

  const traceId = `${Date.now()}_${Math.random().toString(16).slice(2)}`;

  // One file per day (append JSONL). Use UTC date so it’s stable.
  const day = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
  const filename = `ptrs_trace_${day}.jsonl`;
  const filePath = path.join(dir, filename);

  let stream;
  try {
    stream = fs.createWriteStream(filePath, { flags: "a" });
  } catch (_) {
    return null;
  }

  let disabled = false;

  stream.on("error", () => {
    disabled = true;
    try {
      stream.destroy();
    } catch (_) {}
  });

  const base = {
    traceId,
    customerId,
    ptrsId,
    actorId: actorId || null,
    pid: process.pid,
  };

  const write = (event, metaObj = {}) => {
    if (disabled) return;
    try {
      stream.write(
        JSON.stringify({
          ts: _traceNowIso(),
          event,
          ...base,
          ...metaObj,
        }) + "\n",
      );
    } catch (_) {
      // Never allow trace writing to break the job.
    }
  };

  const close = async () => {
    if (!stream || disabled) return;
    try {
      await new Promise((resolve) => stream.end(resolve));
    } catch (_) {}
  };

  // Optional log so callers can find the file without hunting.
  if (typeof logInfo === "function") {
    try {
      const payload = { customerId, ptrsId, traceId, filePath };
      logInfo(
        "PTRS trace enabled (JSONL file)",
        typeof meta === "function" ? meta(payload) : payload,
      );
    } catch (_) {}
  }

  if (!disabled) write("trace_open", { filePath, day });

  return { traceId, filePath, write, close };
}

module.exports = {
  createPtrsTrace,
  hrMsSince,
};
