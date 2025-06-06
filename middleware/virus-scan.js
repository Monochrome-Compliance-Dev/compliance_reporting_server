const { logger } = require("../helpers/logger");
const NodeClam = require("clamscan");
let clamAvailable = true;
const { Readable } = require("stream");

let ClamScan = null;

(async () => {
  try {
    ClamScan = await new NodeClam().init({
      removeInfected: true,
      quarantineInfected: false,
      scanLog: null,
      debugMode: false,
      fileList: null,
      scanRecursively: false,
      clamscan: {
        path: "/opt/homebrew/bin/clamscan", // adjust path if needed
        db: null,
        scanArchives: true,
        active: true,
      },
      preference: "clamscan",
    });
  } catch (error) {
    clamAvailable = false;
    logger.logEvent("warn", "ClamAV not available during async init", {
      action: "AntivirusInit",
      error: error.message,
    });
  }
})();

async function scanFile(filePath, ext, originalname) {
  if (!clamAvailable) {
    logger.logEvent("warn", "Skipping scan, ClamAV not available", {
      action: "AntivirusScan",
      fileName: originalname,
    });
    return;
  }

  logger.logEvent("info", "scanFile invoked", {
    action: "AntivirusScan",
    fileName: originalname,
    filePath,
  });

  try {
    let clamscan;
    try {
      clamscan = await ClamScan;
    } catch (error) {
      logger.logEvent("warn", "ClamAV unavailable at runtime", {
        action: "AntivirusInit",
        error: error.message,
      });
      return;
    }

    const { isInfected } = await clamscan.isInfected(filePath);
    logger.logEvent("info", "Scan result", {
      action: "AntivirusScan",
      fileName: originalname,
      result: isInfected ? "infected" : "clean",
    });

    if (isInfected) {
      logger.logEvent("warn", "File rejected by antivirus", {
        action: "AntivirusScan",
        fileName: originalname,
      });
      throw new Error("File failed antivirus scan.");
    }

    logger.logEvent("info", "File passed scan", {
      action: "AntivirusScan",
      fileName: originalname,
    });
  } catch (err) {
    logger.logEvent("error", "ClamAV scan error", {
      action: "AntivirusScan",
      fileName: originalname,
      error: err.message,
    });
    throw new Error("Antivirus scan failed.");
  }
}

async function scanFileBuffer(buffer, name) {
  if (!clamAvailable) {
    logger.logEvent("warn", "Skipping scan, ClamAV not available", {
      action: "AntivirusScan",
      fileName: name,
    });
    return;
  }

  logger.logEvent("info", "scanFileBuffer invoked", {
    action: "AntivirusScanBuffer",
    fileName: name,
  });

  try {
    let clamscan;
    try {
      clamscan = await ClamScan;
    } catch (error) {
      logger.logEvent("warn", "ClamAV unavailable at runtime", {
        action: "AntivirusInit",
        error: error.message,
      });
      return;
    }

    const stream = Readable.from(buffer); // convert buffer to stream
    const { isInfected } = await clamscan.scanStream(stream);

    logger.logEvent("info", "Buffer scan result", {
      action: "AntivirusScanBuffer",
      fileName: name,
      result: isInfected ? "infected" : "clean",
    });
    if (isInfected) {
      logger.logEvent("warn", "Buffer rejected by antivirus", {
        action: "AntivirusScanBuffer",
        fileName: name,
      });
      throw new Error("File failed antivirus scan.");
    }
    logger.logEvent("info", "Buffer passed scan", {
      action: "AntivirusScanBuffer",
      fileName: name,
    });
  } catch (err) {
    logger.logEvent("error", "ClamAV buffer scan error", {
      action: "AntivirusScanBuffer",
      fileName: name,
      error: err.message,
    });
    throw new Error("Antivirus scan failed.");
  }
}

module.exports = {
  scanFile,
  scanFileBuffer,
  isClamAvailable: () => clamAvailable,
};
