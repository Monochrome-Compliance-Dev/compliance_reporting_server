const config = require("./config");
const { Sequelize } = require("sequelize");
const { logger } = require("./logger");

const DB_HOST = process.env.DB_HOST || config.db.host;
const DB_PORT = process.env.DB_PORT || config.db.port;
const DB_USER = process.env.DB_USER || config.db.user;
const DB_PASSWORD = process.env.DB_PASSWORD || config.db.password;
const DB_NAME = process.env.DB_NAME || config.db.name;

const sequelize = new Sequelize(DB_NAME, DB_USER, DB_PASSWORD, {
  dialect: "postgres",
  host: DB_HOST,
  port: DB_PORT,
  pool: {
    max: 100,
    min: 0,
    acquire: 30000,
    idle: 10000,
  },
  logging: console.log,
});

const db = {
  sequelize,
};

async function initialize() {
  let retries = 5;
  while (retries) {
    try {
      await sequelize.authenticate();
      logger.logEvent("info", "Database connection established", {
        action: "DatabaseInit",
      });
      break;
    } catch (err) {
      retries -= 1;
      logger.logEvent("error", "Database connection failed. Retrying...", {
        action: "DatabaseInit",
        error: err.message,
        stack: err.stack,
      });
      if (!retries) throw err;
      await new Promise((res) => setTimeout(res, 5000));
    }
  }

  // Init models
  db.User = require("../users/user.model")(sequelize);
  db.RefreshToken = require("../users/refresh-token.model")(sequelize);
  db.Client = require("../clients/client.model")(sequelize);
  db.Report = require("../reports/report.model")(sequelize);
  db.Tcp = require("../tcp/tcp.model")(sequelize);
  db.Entity = require("../entities/entity.model")(sequelize);
  db.Booking = require("../booking/booking.model")(sequelize);
  db.Tracking = require("../tracking/tracking.model")(sequelize);
  db.Audit = require("../audit/audit.model")(sequelize);
  db.AdminContent = require("../admin/admin.model")(sequelize);

  // Relationships
  db.User.hasMany(db.RefreshToken, { onDelete: "CASCADE" });
  db.RefreshToken.belongsTo(db.User);
  db.User.belongsTo(db.Client);
  db.Client.hasMany(db.User);
  db.Client.hasMany(db.Report, { onDelete: "CASCADE" });
  db.Client.hasMany(db.Tcp, { onDelete: "CASCADE" });
  db.Report.belongsTo(db.Client);
  db.Tcp.belongsTo(db.Report, { onDelete: "CASCADE" });
  db.Report.hasMany(db.Tcp, { onDelete: "CASCADE" });
  db.Tcp.hasMany(db.Audit, { onDelete: "CASCADE" });
  db.Audit.belongsTo(db.Tcp, { onDelete: "CASCADE" });
  db.Client.hasMany(db.Audit, { onDelete: "CASCADE" });
  db.Audit.belongsTo(db.Client, { onDelete: "CASCADE" });

  // Sync models
  await sequelize.sync();
}

module.exports = db;
initialize();
