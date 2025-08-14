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
    max: 20,
    min: 0,
    acquire: 30000,
    idle: 10000,
  },
  // logging: console.log,
  logging: false,
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
    "../ptrs",
    "../tcp",
    "../entities",
    "../booking",
    "../tracking",
    "../admin",
    "../xero",
    "../audit", // added to load AuditEvent model
    "../esg", // added to load ESGIndicator and ESGMetric models
    "../files", // added to load File model
    "../ms", // added to load MSGrievance, MSSupplierRisk and MSTraining models
    "../partners",
    "../invoices",
    "../products",
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
    db.User.hasMany(db.RefreshToken, {
      foreignKey: "userId",
      onDelete: "CASCADE",
    });
    db.RefreshToken.belongsTo(db.User, { foreignKey: "userId" });
  }
  if (db.User && db.Client) {
    db.User.belongsTo(db.Client, { foreignKey: "clientId" });
    db.Client.hasMany(db.User, { foreignKey: "clientId" });
  }
  if (db.Client && db.Ptrs) {
    db.Client.hasMany(db.Ptrs, { foreignKey: "clientId", onDelete: "CASCADE" });
    db.Ptrs.belongsTo(db.Client, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
  }
  if (db.Client && db.Tcp) {
    db.Client.hasMany(db.Tcp, { foreignKey: "clientId", onDelete: "CASCADE" });
    db.Tcp.belongsTo(db.Client, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
  }
  if (db.Ptrs && db.Tcp) {
    db.Ptrs.hasMany(db.Tcp, { foreignKey: "ptrsId", onDelete: "CASCADE" });
    db.Tcp.belongsTo(db.Ptrs, { foreignKey: "ptrsId", onDelete: "CASCADE" });
  }

  // Xero Token relationship
  if (db.Client && db.XeroToken) {
    db.Client.hasMany(db.XeroToken, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.XeroToken.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // Xero Invoice relationship
  if (db.Client && db.XeroInvoice) {
    db.Client.hasMany(db.XeroInvoice, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.XeroInvoice.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // Xero Payment relationship
  if (db.Client && db.XeroPayment) {
    db.Client.hasMany(db.XeroPayment, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.XeroPayment.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // Xero Contact relationship
  if (db.Client && db.XeroContact) {
    db.Client.hasMany(db.XeroContact, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.XeroContact.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // Xero Organisation relationship
  if (db.Client && db.XeroOrganisation) {
    db.Client.hasMany(db.XeroOrganisation, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.XeroOrganisation.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // TCP csv upload error relationship
  if (db.Client && db.TcpError) {
    db.Client.hasMany(db.TcpError, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.TcpError.belongsTo(db.Client, { foreignKey: "clientId" });
  }
  if (db.Ptrs && db.TcpError) {
    db.Ptrs.hasMany(db.TcpError, { foreignKey: "ptrsId", onDelete: "CASCADE" });
    db.TcpError.belongsTo(db.Ptrs, { foreignKey: "ptrsId" });
  }

  // ESG Indicator and Metric relationships
  if (db.ESGIndicator && db.ESGMetric) {
    db.ESGIndicator.hasMany(db.ESGMetric, { foreignKey: "indicatorId" });
    db.ESGMetric.belongsTo(db.ESGIndicator, { foreignKey: "indicatorId" });
  }

  // ESG Metric has many Files
  if (db.ESGMetric && db.File) {
    db.ESGMetric.hasMany(db.File, {
      foreignKey: "metricId",
      onDelete: "CASCADE",
    });
    db.File.belongsTo(db.ESGMetric, {
      foreignKey: "metricId",
      onDelete: "CASCADE",
    });
  }

  // ESG Indicator has many Files
  if (db.ESGIndicator && db.File) {
    db.ESGIndicator.hasMany(db.File, {
      foreignKey: "indicatorId",
      onDelete: "CASCADE",
    });
    db.File.belongsTo(db.ESGIndicator, {
      foreignKey: "indicatorId",
      onDelete: "CASCADE",
    });
  }

  // ESG Metric belongs to Unit
  if (db.ESGMetric && db.Unit) {
    db.Unit.hasMany(db.ESGMetric, { foreignKey: "unitId" });
    db.ESGMetric.belongsTo(db.Unit, { foreignKey: "unitId" });
  }

  // Modern Slavery relationships
  if (db.Client && db.MSSupplierRisk) {
    db.Client.hasMany(db.MSSupplierRisk, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.MSSupplierRisk.belongsTo(db.Client, { foreignKey: "clientId" });
  }
  if (db.User && db.MSSupplierRisk) {
    db.User.hasMany(db.MSSupplierRisk, { foreignKey: "createdBy" });
    db.MSSupplierRisk.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Client && db.MSTraining) {
    db.Client.hasMany(db.MSTraining, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.MSTraining.belongsTo(db.Client, { foreignKey: "clientId" });
  }
  if (db.User && db.MSTraining) {
    db.User.hasMany(db.MSTraining, { foreignKey: "createdBy" });
    db.MSTraining.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Client && db.MSGrievance) {
    db.Client.hasMany(db.MSGrievance, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.MSGrievance.belongsTo(db.Client, { foreignKey: "clientId" });
  }
  if (db.User && db.MSGrievance) {
    db.User.hasMany(db.MSGrievance, { foreignKey: "createdBy" });
    db.MSGrievance.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Client && db.MSInterviewResponse) {
    db.Client.hasMany(db.MSInterviewResponse, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.MSInterviewResponse.belongsTo(db.Client, { foreignKey: "clientId" });
  }
  if (db.User && db.MSInterviewResponse) {
    db.User.hasMany(db.MSInterviewResponse, { foreignKey: "createdBy" });
    db.MSInterviewResponse.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  // MSReportingPeriod relationships

  // Invoice and InvoiceLine relationship
  if (db.Invoice && db.InvoiceLine) {
    db.Invoice.hasMany(db.InvoiceLine, {
      foreignKey: "invoiceId",
      onDelete: "CASCADE",
    });
    db.InvoiceLine.belongsTo(db.Invoice, { foreignKey: "invoiceId" });
  }

  // Invoice belongs to Client
  if (db.Invoice && db.Client) {
    db.Client.hasMany(db.Invoice, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.Invoice.belongsTo(db.Client, { foreignKey: "clientId" });
  }

  // Invoice belongs to Partner
  if (db.Invoice && db.Partner) {
    db.Partner.hasMany(db.Invoice, {
      foreignKey: "partnerId",
      onDelete: "CASCADE",
    });
    db.Invoice.belongsTo(db.Partner, { foreignKey: "partnerId" });
  }

  // InvoiceLine belongs to Product
  if (db.InvoiceLine && db.Product) {
    db.Product.hasMany(db.InvoiceLine, { foreignKey: "productId" });
    db.InvoiceLine.belongsTo(db.Product, { foreignKey: "productId" });
  }

  // TODO: Replace sequelize.sync() with proper migrations (e.g. umzug / sequelize-cli)
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
      // logger.logEvent("info", "RLS policies initialised", {
      //   action: "DatabaseInit",
      // });
    } catch (error) {
      logger.logEvent("error", "RLS policy initialisation failed", {
        action: "DatabaseInit",
        error: error.message,
      });
    }
  } else {
    // logger.logEvent("warn", "No RLS setup file found", {
    //   action: "DatabaseInit",
    // });
  }
}

module.exports = db;
if (process.env.DISABLE_DB !== "true") {
  initialise().catch((err) => {
    logger.logEvent("error", "Failed DB init", {
      action: "DatabaseInit",
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  });
} else {
  logger.logEvent("info", "DB init skipped (DISABLE_DB=true)");
}

process.on("SIGTERM", async () => {
  logger.logEvent("info", "Shutting down DB connections...", {
    action: "DatabaseShutdown",
  });
  await sequelize.close();
  process.exit(0);
});
