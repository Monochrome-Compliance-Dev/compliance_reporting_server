require("dotenv").config();
const transformXeroData = require("./transformXeroData");
const xeroService = require("../../xero/xero.service");
const tcpService = require("../../tcp/tcp.service");
const { logger } = require("../../helpers/logger");
const db = require("../../db/database");

async function replay() {
  const clientId = "rahfwOLxLN"; // replace with actual clientId
  const reportId = "qSoYMYMyS9"; // replace with actual reportId
  const createdBy = "j3HJwUR_pi"; // replace with actual userId

  try {
    const xeroData = await xeroService.getAllXeroData(clientId, reportId, db);

    const transformed = await transformXeroData(xeroData);

    const result = await tcpService.saveTransformedDataToTcp(
      transformed,
      reportId,
      clientId,
      createdBy
    );
    logger.logEvent("info", "TCP data reloaded from existing Xero records", {
      result,
    });
    console.log("✅ TCP save complete.");
  } catch (err) {
    logger.logEvent("error", "TCP replay failed", { error: err.message });
    console.error("❌ TCP replay failed:", err.message);
  }
}

replay();
