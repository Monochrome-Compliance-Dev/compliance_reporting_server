const { Sequelize } = require("sequelize");
const { logger } = require("../helpers/logger");
const fs = require("fs");
const path = require("path");

const DB_HOST = process.env.DB_HOST;
const DB_PORT = process.env.DB_PORT || 5432;
const DB_USER = process.env.DB_USER;
const DB_PASSWORD = process.env.DB_PASSWORD;
const DB_NAME = process.env.DB_NAME;
const DB_SSL = process.env.DB_SSL;

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
  schema: process.env.DB_SCHEMA || "public", // Use environment variable or default to 'public'
  dialectOptions:
    DB_SSL === "true"
      ? {
          ssl: {
            require: true,
            rejectUnauthorized: false,
          },
        }
      : {},
});

const db = {
  sequelize,
  Sequelize,
};

async function initialise() {
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

  // Dynamically load models from their folders
  const modelDirs = [
    "../users",
    "../clients",
    "../reports",
    "../tcp",
    "../entities",
    "../booking",
    "../tracking",
    "../admin",
    "../xero",
  ];

  modelDirs.forEach((dir) => {
    const modelPath = path.join(__dirname, dir);
    const files = fs.readdirSync(modelPath);
    files.forEach((file) => {
      if (file.endsWith(".model.js")) {
        const model = require(path.join(modelPath, file))(sequelize);
        const name = model.name.charAt(0).toUpperCase() + model.name.slice(1);
        db[name] = model;
      }
    });
  });

  // Setup model relationships
  if (db.User && db.RefreshToken) {
    db.User.hasMany(db.RefreshToken, { onDelete: "CASCADE" });
    db.RefreshToken.belongsTo(db.User);
  }
  if (db.User && db.Client) {
    db.User.belongsTo(db.Client);
    db.Client.hasMany(db.User);
  }
  if (db.Client && db.Report) {
    db.Client.hasMany(db.Report, { onDelete: "CASCADE" });
    db.Report.belongsTo(db.Client);
  }
  if (db.Client && db.Tcp) {
    db.Client.hasMany(db.Tcp, { onDelete: "CASCADE" });
  }
  if (db.Report && db.Tcp) {
    db.Report.hasMany(db.Tcp, { onDelete: "CASCADE" });
    db.Tcp.belongsTo(db.Report, { onDelete: "CASCADE" });
  }

  // Xero Token relationship
  if (db.Client && db.XeroToken) {
    db.Client.hasMany(db.XeroToken, { onDelete: "CASCADE" });
    db.XeroToken.belongsTo(db.Client);
  }

  // Xero Invoice relationship
  if (db.Client && db.XeroInvoice) {
    db.Client.hasMany(db.XeroInvoice, { onDelete: "CASCADE" });
    db.XeroInvoice.belongsTo(db.Client);
  }

  // Xero Payment relationship
  if (db.Client && db.XeroPayment) {
    db.Client.hasMany(db.XeroPayment, { onDelete: "CASCADE" });
    db.XeroPayment.belongsTo(db.Client);
  }

  // Xero Contact relationship
  if (db.Client && db.XeroContact) {
    db.Client.hasMany(db.XeroContact, { onDelete: "CASCADE" });
    db.XeroContact.belongsTo(db.Client);
  }

  // Xero Organisation relationship
  if (db.Client && db.XeroOrganisation) {
    db.Client.hasMany(db.XeroOrganisation, { onDelete: "CASCADE" });
    db.XeroOrganisation.belongsTo(db.Client);
  }

  // TCP csv upload error relationship
  if (db.Client && db.TcpError) {
    db.Client.hasMany(db.TcpError, { onDelete: "CASCADE" });
    db.TcpError.belongsTo(db.Client);
  }
  if (db.Report && db.TcpError) {
    db.Report.hasMany(db.TcpError, { onDelete: "CASCADE" });
    db.TcpError.belongsTo(db.Report);
  }

  // Sync models
  await sequelize.sync();

  // Initialise RLS policies (if using them)
  await initialiseRLS();
}

async function initialiseRLS() {
  const rlsFile = path.join(__dirname, "setup_rls.sql");
  if (fs.existsSync(rlsFile)) {
    const sql = fs.readFileSync(rlsFile, "utf8");
    try {
      await sequelize.query(sql);
      logger.logEvent("info", "RLS policies initialised", {
        action: "DatabaseInit",
      });
    } catch (error) {
      logger.logEvent("error", "RLS policy initialisation failed", {
        action: "DatabaseInit",
        error: error.message,
      });
    }
  } else {
    logger.logEvent("warn", "No RLS setup file found", {
      action: "DatabaseInit",
    });
  }
}

module.exports = db;
initialise();
