// middleware/setClientIdRLS.js
const { sequelize } = require("../db/database"); // adjust to your Sequelize instance

const setClientIdRLS = async (req, res, next) => {
  try {
    const clientId = req.auth?.clientId; // updated to use req.auth
    if (!clientId) {
      console.warn("No clientId found for RLS - skipping.");
      return next();
    }

    // Set the current client ID in Postgres for RLS
    await sequelize.query(`SET app.current_client_id = '${clientId}';`);
    next();
  } catch (error) {
    console.error("Error setting RLS clientId:", error);
    res.status(500).json({ message: "Error setting RLS" });
  }
};

module.exports = setClientIdRLS;
