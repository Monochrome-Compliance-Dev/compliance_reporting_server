const { stageRun } = require("../services/stage.service.js");
const { logger } = require("@/utils/logger.js");

async function stageRunController(req, res) {
  try {
    const { runId } = req.params;
    const { customerId } = req.user || {};
    const { profileId } = req.body || {};
    const result = await stageRun({ customerId, runId, profileId });
    return res.json(result);
  } catch (err) {
    logger.error("StageRun failed", {
      meta: { err: err.message, stack: err.stack },
    });
    res.status(500).json({ error: err.message });
  }
}
module.exports = { stageRunController };
