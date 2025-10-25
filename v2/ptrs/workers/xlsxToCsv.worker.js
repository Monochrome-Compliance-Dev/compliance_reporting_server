// XLSX → CSV conversion isolated in a worker
const { parentPort } = require("worker_threads");

let XLSX = null;
try {
  XLSX = require("xlsx");
} catch (e) {}

function convert(buffer) {
  if (!XLSX) {
    const err = new Error("XLSX support not installed. Please `npm i xlsx`.");
    err.code = "XLSX_MODULE_MISSING";
    throw err;
  }
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheetName = wb.SheetNames[0];
  if (!sheetName) {
    const err = new Error("Excel file contains no sheets.");
    err.code = "NO_SHEETS";
    throw err;
  }
  const ws = wb.Sheets[sheetName];
  return XLSX.utils.sheet_to_csv(ws, { FS: ",", strip: true });
}

parentPort.on("message", (msg) => {
  try {
    const { buffer } = msg || {};
    if (!buffer) throw new Error("Missing buffer");
    const csv = convert(Buffer.from(buffer));
    parentPort.postMessage({ ok: true, csv });
  } catch (err) {
    parentPort.postMessage({
      ok: false,
      error: { message: err.message, code: err.code || "XLSX_CONVERT_FAILED" },
    });
  }
});
