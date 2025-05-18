const NodeClam = require("clamscan");
const { Readable } = require("stream");

const ClamScan = new NodeClam().init({
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

async function scanFile(filePath, ext, originalname) {
  console.log(`[virus-scan] scanFile called for: ${originalname} (${ext})`);
  console.log(`[virus-scan] filePath received: ${filePath}`);

  try {
    const clamscan = await ClamScan;
    const { isInfected } = await clamscan.isInfected(filePath);
    console.log(
      `[virus-scan] Scan result for ${originalname}: ${isInfected ? "infected" : "clean"}`
    );

    if (isInfected) {
      console.warn(`[virus-scan] File rejected: ${originalname}`);
      throw new Error("File failed antivirus scan.");
    }

    console.info(`[virus-scan] File passed scan: ${originalname}`);
  } catch (err) {
    console.error(`[virus-scan] ClamAV error for ${originalname}:`, err);
    throw new Error("Antivirus scan failed.");
  }
}

async function scanFileBuffer(buffer, name) {
  console.log(`[virus-scan] scanFileBuffer called for: ${name}`);

  try {
    const clamscan = await ClamScan;
    const stream = Readable.from(buffer); // convert buffer to stream
    const { isInfected } = await clamscan.scanStream(stream);

    console.log(
      `[virus-scan] Scan result for ${name}: ${isInfected ? "infected" : "clean"}`
    );
    if (isInfected) {
      console.warn(`[virus-scan] File rejected: ${name}`);
      throw new Error("File failed antivirus scan.");
    }

    console.info(`[virus-scan] File passed scan: ${name}`);
  } catch (err) {
    console.error(`[virus-scan] ClamAV error for ${name}:`, err);
    throw new Error("Antivirus scan failed.");
  }
}

module.exports = { scanFile, scanFileBuffer };
