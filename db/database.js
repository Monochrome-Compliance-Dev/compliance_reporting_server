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
    "../customers",
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
    "../clients",
    "../engagements",
    "../resources",
    "../assignments",
    "../budget_items",
    "../timesheets",
  ];

  modelDirs.forEach((dir) => {
    const modelPath = path.join(__dirname, dir);
    const files = fs.readdirSync(modelPath);
    files.forEach((file) => {
      if (file.endsWith(".model.js")) {
        const model = require(path.join(modelPath, file))(sequelize);
        const toPascal = (s) =>
          s
            .split("_")
            .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
            .join("");
        const name = toPascal(model.name);
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
  if (db.User && db.Customer) {
    db.User.belongsTo(db.Customer, { foreignKey: "customerId" });
    db.Customer.hasMany(db.User, { foreignKey: "customerId" });
  }
  if (db.Customer && db.Ptrs) {
    db.Customer.hasMany(db.Ptrs, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.Ptrs.belongsTo(db.Customer, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
  }
  if (db.Customer && db.Tcp) {
    db.Customer.hasMany(db.Tcp, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.Tcp.belongsTo(db.Customer, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
  }
  if (db.Ptrs && db.Tcp) {
    db.Ptrs.hasMany(db.Tcp, { foreignKey: "ptrsId", onDelete: "CASCADE" });
    db.Tcp.belongsTo(db.Ptrs, { foreignKey: "ptrsId", onDelete: "CASCADE" });
  }

  // Xero Token relationship
  if (db.Customer && db.XeroToken) {
    db.Customer.hasMany(db.XeroToken, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.XeroToken.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // Xero Invoice relationship
  if (db.Customer && db.XeroInvoice) {
    db.Customer.hasMany(db.XeroInvoice, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.XeroInvoice.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // Xero Payment relationship
  if (db.Customer && db.XeroPayment) {
    db.Customer.hasMany(db.XeroPayment, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.XeroPayment.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // Xero Contact relationship
  if (db.Customer && db.XeroContact) {
    db.Customer.hasMany(db.XeroContact, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.XeroContact.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // Xero Organisation relationship
  if (db.Customer && db.XeroOrganisation) {
    db.Customer.hasMany(db.XeroOrganisation, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.XeroOrganisation.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // TCP csv upload error relationship
  if (db.Customer && db.TcpError) {
    db.Customer.hasMany(db.TcpError, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.TcpError.belongsTo(db.Customer, { foreignKey: "customerId" });
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
  if (db.Customer && db.MSSupplierRisk) {
    db.Customer.hasMany(db.MSSupplierRisk, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.MSSupplierRisk.belongsTo(db.Customer, { foreignKey: "customerId" });
  }
  if (db.User && db.MSSupplierRisk) {
    db.User.hasMany(db.MSSupplierRisk, { foreignKey: "createdBy" });
    db.MSSupplierRisk.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Customer && db.MSTraining) {
    db.Customer.hasMany(db.MSTraining, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.MSTraining.belongsTo(db.Customer, { foreignKey: "customerId" });
  }
  if (db.User && db.MSTraining) {
    db.User.hasMany(db.MSTraining, { foreignKey: "createdBy" });
    db.MSTraining.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Customer && db.MSGrievance) {
    db.Customer.hasMany(db.MSGrievance, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.MSGrievance.belongsTo(db.Customer, { foreignKey: "customerId" });
  }
  if (db.User && db.MSGrievance) {
    db.User.hasMany(db.MSGrievance, { foreignKey: "createdBy" });
    db.MSGrievance.belongsTo(db.User, { foreignKey: "createdBy" });
  }

  if (db.Customer && db.MSInterviewResponse) {
    db.Customer.hasMany(db.MSInterviewResponse, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.MSInterviewResponse.belongsTo(db.Customer, { foreignKey: "customerId" });
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

  // Invoice belongs to Customer
  if (db.Invoice && db.Customer) {
    db.Customer.hasMany(db.Invoice, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.Invoice.belongsTo(db.Customer, { foreignKey: "customerId" });
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

  // --- Pulse (Monochrome Compliance) relationships ---
  // Engagement ↔ Client
  if (db.Engagement && db.Client) {
    db.Client.hasMany(db.Engagement, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    db.Engagement.belongsTo(db.Client, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
  }

  // Engagement ↔ BudgetItem
  if (db.Engagement && db.BudgetItem) {
    db.Engagement.hasMany(db.BudgetItem, {
      foreignKey: "engagementId",
      onDelete: "CASCADE",
    });
    db.BudgetItem.belongsTo(db.Engagement, {
      foreignKey: "engagementId",
      onDelete: "CASCADE",
    });
  }

  // Engagement ↔ Assignment ↔ Resource
  if (db.Engagement && db.Assignment) {
    db.Engagement.hasMany(db.Assignment, {
      foreignKey: "engagementId",
      onDelete: "CASCADE",
    });
    db.Assignment.belongsTo(db.Engagement, {
      foreignKey: "engagementId",
      onDelete: "CASCADE",
    });
  }
  if (db.Resource && db.Assignment) {
    db.Resource.hasMany(db.Assignment, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
    db.Assignment.belongsTo(db.Resource, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
  }

  // Resource ↔ Timesheet
  if (db.Resource && db.Timesheet) {
    db.Resource.hasMany(db.Timesheet, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
    db.Timesheet.belongsTo(db.Resource, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
  }

  // Timesheet ↔ TimesheetRow
  if (db.Timesheet && db.TimesheetRow) {
    db.Timesheet.hasMany(db.TimesheetRow, {
      foreignKey: "timesheetId",
      onDelete: "CASCADE",
    });
    db.TimesheetRow.belongsTo(db.Timesheet, {
      foreignKey: "timesheetId",
      onDelete: "CASCADE",
    });
  }

  // Optional links from TimesheetRow to Engagement and BudgetItem for reporting
  if (db.TimesheetRow && db.Engagement) {
    db.Engagement.hasMany(db.TimesheetRow, {
      foreignKey: "engagementId",
      onDelete: "SET NULL",
    });
    db.TimesheetRow.belongsTo(db.Engagement, { foreignKey: "engagementId" });
  }
  if (db.TimesheetRow && db.BudgetItem) {
    db.BudgetItem.hasMany(db.TimesheetRow, {
      foreignKey: "budgetItemId",
      onDelete: "SET NULL",
    });
    db.TimesheetRow.belongsTo(db.BudgetItem, { foreignKey: "budgetItemId" });
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
