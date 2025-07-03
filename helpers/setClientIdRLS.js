// utils/db/setClientIdRLS.js
const { sequelize } = require("../db/database"); // adjust path if necessary

async function beginTransactionWithClientContext(clientId) {
  if (!clientId) {
    throw new Error("Client ID is required to set RLS context.");
  }

  const safeClientId = clientId.replace(/[^a-zA-Z0-9-_]/g, "");
  const t = await sequelize.transaction();

  try {
    await sequelize.query(
      `SET LOCAL app.current_client_id = '${safeClientId}'`,
      { transaction: t }
    );
  } catch (error) {
    console.warn("SET LOCAL failed — continuing without enforced RLS.");
  }

  return t;
}

module.exports = { beginTransactionWithClientContext };
