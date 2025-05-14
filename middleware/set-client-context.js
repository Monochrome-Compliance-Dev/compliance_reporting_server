const db = require("../helpers/db");

module.exports = async function setClientContext(req, res, next) {
  try {
    const clientId = req.auth?.clientId;
    if (!clientId) {
      return res.status(400).json({ message: "clientId missing from request" });
    }

    await db.sequelize.query(`SET @current_client_id = '${clientId}'`);
    next();
  } catch (err) {
    console.error("Error setting client context:", err);
    res.status(500).json({ message: "Failed to set client context" });
  }
};
