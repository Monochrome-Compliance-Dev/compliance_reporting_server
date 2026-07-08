const { logger } = require("../helpers/logger");
const NodeClam = require("clamscan");
const { Readable } = require("stream");

const antivirusScannerPreference =
  process.env.ANTIVIRUS_SCANNER_PREFERENCE || "clamdscan";
const clamdSocketPath =
  process.env.CLAMD_SOCKET_PATH || "/opt/homebrew/var/run/clamav/clamd.sock";

const ClamScan = new NodeClam().init({
  removeInfected: true,
  quarantineInfected: false,
  scanLog: null,
  debugMode: false,
  fileList: null,
  scanRecursively: false,
  clamscan: {
    path: process.env.CLAMSCAN_PATH || "/opt/homebrew/bin/clamscan",
    db: null,
    scanArchives: true,
    active: true,
  },
  clamdscan: {
    socket: clamdSocketPath,
    timeout: 120000,
    multiscan: false,
    active: true,
  },
  preference: antivirusScannerPreference,
});

async function scanFile(filePath, originalname) {
  const scanStartedAt = Date.now();
  logger.logEvent("info", "scanFile invoked", {
    action: "AntivirusScan",
    fileName: originalname,
    filePath,
    scannerMode: antivirusScannerPreference,
    clamdSocketPath,
  });

  try {
    const clamscan = await ClamScan;
    const { isInfected } = await clamscan.isInfected(filePath);
    const elapsedMs = Date.now() - scanStartedAt;
    logger.logEvent("info", "Scan result", {
      action: "AntivirusScan",
      fileName: originalname,
      result: isInfected ? "infected" : "clean",
      elapsedMs,
      scannerMode: antivirusScannerPreference,
    });

    if (isInfected) {
      logger.logEvent("warn", "File rejected by antivirus", {
        action: "AntivirusScan",
        fileName: originalname,
        elapsedMs,
      });
      throw { status: 400, message: "File failed antivirus scan." };
    }

    logger.logEvent("info", "File passed scan", {
      action: "AntivirusScan",
      fileName: originalname,
      elapsedMs,
      scannerMode: antivirusScannerPreference,
    });
  } catch (err) {
    logger.logEvent("error", "ClamAV scan error", {
      action: "AntivirusScan",
      fileName: originalname,
      elapsedMs: Date.now() - scanStartedAt,
      error: err.message,
      scannerMode: antivirusScannerPreference,
    });
    throw { status: 400, message: "Antivirus scan failed." };
  }
}

async function scanFileBuffer(buffer, name) {
  const scanStartedAt = Date.now();
  logger.logEvent("info", "scanFileBuffer invoked", {
    action: "AntivirusScanBuffer",
    fileName: name,
  });

  try {
    const clamscan = await ClamScan;
    const stream = Readable.from(buffer); // convert buffer to stream
    const { isInfected } = await clamscan.scanStream(stream);
    const elapsedMs = Date.now() - scanStartedAt;

    logger.logEvent("info", "Buffer scan result", {
      action: "AntivirusScanBuffer",
      fileName: name,
      result: isInfected ? "infected" : "clean",
      elapsedMs,
    });
    if (isInfected) {
      logger.logEvent("warn", "Buffer rejected by antivirus", {
        action: "AntivirusScanBuffer",
        fileName: name,
        elapsedMs,
      });
      throw { status: 400, message: "File failed antivirus scan." };
    }
    logger.logEvent("info", "Buffer passed scan", {
      action: "AntivirusScanBuffer",
      fileName: name,
      elapsedMs,
    });
  } catch (err) {
    logger.logEvent("error", "ClamAV buffer scan error", {
      action: "AntivirusScanBuffer",
      fileName: name,
      elapsedMs: Date.now() - scanStartedAt,
      error: err.message,
    });
    throw { status: 400, message: "Antivirus scan failed." };
  }
}

module.exports = { scanFile, scanFileBuffer };
