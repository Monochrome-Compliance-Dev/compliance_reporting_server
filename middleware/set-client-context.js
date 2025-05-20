const { logger } = require("../helpers/logger");
const db = require("../helpers/db");

module.exports = async function setClientContext(req, res, next) {
  try {
    const clientId = req.auth?.clientId;
    if (!clientId) {
      return res.status(400).json({ message: "clientId missing from request" });
    }

    await db.sequelize.query(`SET @current_client_id = '${clientId}'`);
    logger.logEvent("info", "Client context set", {
      action: "SetClientContext",
      clientId,
    });
    next();
  } catch (err) {
    logger.logEvent("error", "Error setting client context", {
      action: "SetClientContext",
      error: err.message,
    });
    res.status(500).json({ message: "Failed to set client context" });
  }
};
