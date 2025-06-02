// middleware/injectClientId.js
const { sequelize } = require("../db/database"); // update with your Sequelize instance

module.exports = async (req, res, next) => {
  const clientId = req.auth?.clientId;
  if (!clientId) {
    console.error("Middleware error: clientId missing in req.auth");
    return res.status(400).json({ error: "clientId missing" });
  }

  const safeClientId = clientId.replace(/[^a-zA-Z0-9-_]/g, "");
  const transaction = await sequelize.transaction();

  try {
    await sequelize.query(
      `SET LOCAL app.current_client_id = '${safeClientId}'`,
      {
        transaction,
      }
    );
    req.dbTransaction = transaction;
    req.body.clientId = safeClientId;
    next();
  } catch (error) {
    console.error("Middleware RLS error:", error.message);
    await transaction.rollback();
    res.status(500).json({ error: "Error setting RLS parameter" });
  }
};
