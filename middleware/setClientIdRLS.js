// middleware/setClientIdRLS.js
const { sequelize } = require("../db/database"); // adjust to your Sequelize instance

const setClientIdRLS = async (req, res, next) => {
  try {
    const clientId = req.auth?.clientId; // updated to use req.auth
    if (!clientId) {
      console.warn("No clientId found for RLS - skipping.");
      return next();
    }

    // Set the current client ID in Postgres for RLS using SET LOCAL for safety within transactions
    await sequelize.query(`SET LOCAL app.current_client_id = '${clientId}';`);
    next();
  } catch (error) {
    // Handle error gracefully if app.current_client_id parameter is missing
    if (error.message.includes("unrecognized configuration parameter")) {
      console.warn(
        "RLS parameter app.current_client_id not found in database, skipping SET LOCAL."
      );
      return next();
    }
    console.error("Error setting RLS clientId:", error);
    res.status(500).json({ message: "Error setting RLS" });
  }
};

module.exports = setClientIdRLS;
