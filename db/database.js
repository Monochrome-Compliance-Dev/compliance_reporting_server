const { Sequelize } = require("sequelize");
const { logger } = require("@/helpers/logger");
const fs = require("fs");
const path = require("path");

// v2 PTRS model loader (New World)
const { initPtrsV2Models } = require("@/v2/ptrs/models/ptrs_model_loader");
const { Pool } = require("pg");

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

// --- Raw pg Pool (for streaming/COPY use cases like Big Bertha) ---
let _pgPool = null;
function getPgPool() {
  if (_pgPool) return _pgPool;
  const ssl =
    process.env.DB_SSL === "true" ? { rejectUnauthorized: false } : false;
  _pgPool = new Pool({
    host: process.env.DB_HOST || process.env.PGHOST || "localhost",
    user: process.env.DB_USER || process.env.PGUSER || "postgres",
    password: process.env.DB_PASSWORD || process.env.PGPASSWORD || "",
    database: process.env.DB_NAME || process.env.PGDATABASE || "postgres",
    port: Number(process.env.DB_PORT || process.env.PGPORT || 5432),
    ssl,
    max: 10,
  });
  return _pgPool;
}

// expose getter on db export
db.getPgPool = getPgPool;

// Recursively collect all *.model.js files under a directory (skips node_modules/.git/coverage)
function walkModelFiles(dir) {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const results = [];
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (
        entry.name === "node_modules" ||
        entry.name === ".git" ||
        entry.name === "coverage"
      )
        continue;
      results.push(...walkModelFiles(full));
    } else if (entry.isFile() && entry.name.endsWith(".model.js")) {
      results.push(full);
    }
  }
  return results;
}

const toPascal = (s) =>
  s
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");

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
    "../bigBertha",
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
    "../stripe",
    "../pulse",
    "../v2/profiles", // added to load CustomerProfile model
  ];

  modelDirs.forEach((dir) => {
    const modelPath = path.join(__dirname, dir);

    // Skip gracefully if the directory does not exist
    if (!fs.existsSync(modelPath)) {
      logger.logEvent("warn", "Model directory missing – skipping", {
        action: "DatabaseInit",
        dir: modelPath,
      });
      return;
    }

    const files = walkModelFiles(modelPath);
    if (files.length === 0) {
      logger.logEvent("warn", "No model files found in directory", {
        action: "DatabaseInit",
        dir: modelPath,
      });
    }

    files.forEach((fullPath) => {
      try {
        const factory = require(fullPath);
        if (typeof factory !== "function") {
          logger.logEvent(
            "warn",
            "Model file does not export a factory – skipping",
            {
              action: "DatabaseInit",
              file: fullPath,
            },
          );
          return;
        }
        const model = factory(sequelize);
        const name = toPascal(model.name);
        db[name] = model;
      } catch (err) {
        logger.logEvent("error", "Failed to load model file", {
          action: "DatabaseInit",
          file: fullPath,
          error: err.message,
        });
      }
    });
  });

  // --- Load PTRS v2 New World models (non-*.model.js, via explicit loader) ---
  try {
    const v2Models = initPtrsV2Models(sequelize);
    for (const [name, model] of Object.entries(v2Models)) {
      // Keep naming consistent with existing convention
      db[toPascal(model.name)] = model;
    }

    // Ensure PTRS v2 import exception model is available (table: ptrs_import_exception)
    try {
      const definePtrsImportException = require("@/v2/ptrs/models/ptrs_import_exception");
      if (typeof definePtrsImportException === "function") {
        const m = definePtrsImportException(sequelize);
        const key = toPascal(m.name);
        if (!db[key]) db[key] = m;
      }
    } catch (e) {
      logger.logEvent("warn", "Failed to load PTRS v2 import exception model", {
        action: "DatabaseInit",
        error: e?.message,
      });
    }

    logger.logEvent("info", "PTRS v2 models initialised", {
      action: "DatabaseInit",
      count: Object.keys(v2Models).length,
    });
  } catch (err) {
    logger.logEvent("error", "Failed to initialise PTRS v2 models", {
      action: "DatabaseInit",
      error: err.message,
      stack: err.stack,
    });
  }

  // --- Load PTRS v2 reference models (non-*.model.js, explicit requires) ---
  // These live under v2/ptrs/models and are intentionally not part of the generic *.model.js loader.
  // Only define them if they are not already present.
  try {
    const definePtrsEmployeeRef = require("@/v2/ptrs/models/ptrs_employee_ref");
    const definePtrsIntraCompanyRef = require("@/v2/ptrs/models/ptrs_intra_company_ref");
    const definePtrsGovEntityRef = require("@/v2/ptrs/models/ptrs_gov_entity_ref");
    const definePtrsExclusionKeywordCustomerRef = require("@/v2/ptrs/models/ptrs_exclusion_keyword_customer_ref");
    const definePtrsEntityRef = require("@/v2/ptrs/models/ptrs_entity_ref");

    const ensureModel = (factory) => {
      if (typeof factory !== "function") return null;
      const model = factory(sequelize);
      const key = toPascal(model.name);
      if (!db[key]) db[key] = model;
      return model;
    };

    ensureModel(definePtrsEmployeeRef);
    ensureModel(definePtrsIntraCompanyRef);
    ensureModel(definePtrsGovEntityRef);
    ensureModel(definePtrsExclusionKeywordCustomerRef);
    ensureModel(definePtrsEntityRef);

    logger.logEvent("info", "PTRS v2 reference models initialised", {
      action: "DatabaseInit",
      models: [
        "PtrsEmployeeRef",
        "PtrsIntraCompanyRef",
        "PtrsGovEntityRef",
        "PtrsExclusionKeywordCustomerRef",
        "PtrsEntityRef",
      ],
    });
  } catch (err) {
    logger.logEvent("error", "Failed to initialise PTRS v2 reference models", {
      action: "DatabaseInit",
      error: err.message,
      stack: err.stack,
    });
  }

  // --- Load PTRS v2 Xero cache models (non-*.model.js, via explicit requires) ---
  // These live under v2/ptrs/xero/models and are intentionally not part of the generic *.model.js loader.
  // Only define them if they are not already present (avoids clobbering any existing Xero models loaded elsewhere).
  try {
    const defineXeroContact = require("@/v2/ptrs/xero/models/xeroContact.model");
    const defineXeroInvoice = require("@/v2/ptrs/xero/models/xeroInvoice.model");
    const defineXeroPayment = require("@/v2/ptrs/xero/models/xeroPayment.model");
    const defineXeroBankTransaction = require("@/v2/ptrs/xero/models/xeroBankTransactions.model");
    const defineXeroOrganisation = require("@/v2/ptrs/xero/models/xeroOrganisation.model");

    const ensureModel = (factory) => {
      if (typeof factory !== "function") return null;
      const model = factory(sequelize);
      const key = toPascal(model.name);
      if (!db[key]) {
        db[key] = model;
      }
      return model;
    };

    ensureModel(defineXeroContact);
    ensureModel(defineXeroInvoice);
    ensureModel(defineXeroPayment);
    ensureModel(defineXeroBankTransaction);
    ensureModel(defineXeroOrganisation);

    logger.logEvent("info", "PTRS v2 Xero cache models initialised", {
      action: "DatabaseInit",
      models: [
        "PtrsXeroContact",
        "PtrsXeroInvoice",
        "PtrsXeroPayment",
        "PtrsXeroBankTransaction",
        "PtrsXeroOrganisation",
      ],
    });
  } catch (err) {
    logger.logEvent("error", "Failed to initialise PTRS v2 Xero cache models", {
      action: "DatabaseInit",
      error: err.message,
      stack: err.stack,
    });
  }

  // Setup model relationships
  // --- PTRS v2 relationships (New World) ---
  // Profile ↔ Canonical Field Map
  if (db.PtrsProfile && db.PtrsFieldMap) {
    db.PtrsProfile.hasMany(db.PtrsFieldMap, {
      foreignKey: "profileId",
      sourceKey: "id",
      onDelete: "CASCADE",
    });
    db.PtrsFieldMap.belongsTo(db.PtrsProfile, {
      foreignKey: "profileId",
      targetKey: "id",
    });
  }
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

  // Customer Access (which users can act for which customers)
  if (db.User && db.CustomerAccess) {
    db.User.hasMany(db.CustomerAccess, {
      foreignKey: "userId",
      onDelete: "CASCADE",
    });
    db.CustomerAccess.belongsTo(db.User, { foreignKey: "userId" });
  }
  if (db.Customer && db.CustomerAccess) {
    db.Customer.hasMany(db.CustomerAccess, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.CustomerAccess.belongsTo(db.Customer, { foreignKey: "customerId" });
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

  // PTRS v2 Xero cache relationships (avoid collision with legacy v1 Xero models)
  if (db.Customer && db.PtrsXeroInvoice) {
    db.Customer.hasMany(db.PtrsXeroInvoice, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.PtrsXeroInvoice.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  if (db.Customer && db.PtrsXeroPayment) {
    db.Customer.hasMany(db.PtrsXeroPayment, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.PtrsXeroPayment.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  if (db.Customer && db.PtrsXeroContact) {
    db.Customer.hasMany(db.PtrsXeroContact, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.PtrsXeroContact.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  if (db.Customer && db.PtrsXeroBankTransaction) {
    db.Customer.hasMany(db.PtrsXeroBankTransaction, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.PtrsXeroBankTransaction.belongsTo(db.Customer, {
      foreignKey: "customerId",
    });
  }

  // PTRS v2 Xero Organisation cache relationship
  if (db.Customer && db.PtrsXeroOrganisation) {
    db.Customer.hasMany(db.PtrsXeroOrganisation, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.PtrsXeroOrganisation.belongsTo(db.Customer, {
      foreignKey: "customerId",
    });
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

  // Big Bertha staging (raw import rows)
  if (db.Customer && db.ImportRaw) {
    db.Customer.hasMany(db.ImportRaw, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.ImportRaw.belongsTo(db.Customer, { foreignKey: "customerId" });
  }
  if (db.Ptrs && db.ImportRaw) {
    db.Ptrs.hasMany(db.ImportRaw, {
      foreignKey: "ptrsId",
      onDelete: "CASCADE",
    });
    db.ImportRaw.belongsTo(db.Ptrs, {
      foreignKey: "ptrsId",
      onDelete: "CASCADE",
    });
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
  // If/when Trackable model exists, also wire it (no-op if missing)
  if (db.Trackable && db.Budget) {
    db.Trackable.hasMany(db.Budget, {
      foreignKey: "trackableId",
      onDelete: "SET NULL",
    });
    db.Budget.belongsTo(db.Trackable, { foreignKey: "trackableId" });
  }

  // Budget ↔ Sections
  if (db.Budget && db.BudgetSection) {
    db.Budget.hasMany(db.BudgetSection, {
      foreignKey: "budgetId",
      onDelete: "CASCADE",
    });
    db.BudgetSection.belongsTo(db.Budget, { foreignKey: "budgetId" });
  }

  // Budget ↔ Items (root-level items)
  if (db.Budget && db.BudgetItem) {
    db.Budget.hasMany(db.BudgetItem, {
      foreignKey: "budgetId",
      onDelete: "CASCADE",
    });
    db.BudgetItem.belongsTo(db.Budget, { foreignKey: "budgetId" });
  }

  // Section ↔ Items
  if (db.BudgetSection && db.BudgetItem) {
    db.BudgetSection.hasMany(db.BudgetItem, {
      foreignKey: "sectionId",
      onDelete: "CASCADE",
    });
    db.BudgetItem.belongsTo(db.BudgetSection, { foreignKey: "sectionId" });
  }

  // BudgetItem ↔ Assignment (canonical link)
  if (db.BudgetItem && db.Assignment) {
    db.BudgetItem.hasMany(db.Assignment, {
      foreignKey: "budgetItemId",
      onDelete: "CASCADE",
    });
    db.Assignment.belongsTo(db.BudgetItem, {
      foreignKey: "budgetItemId",
      as: "line",
    });
  }

  // Resource ↔ Assignment
  if (db.Resource && db.Assignment) {
    db.Resource.hasMany(db.Assignment, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
    db.Assignment.belongsTo(db.Resource, { foreignKey: "resourceId" });
  }

  // Contribution relationships (New World)
  if (db.BudgetItem && db.Contribution) {
    db.BudgetItem.hasMany(db.Contribution, {
      foreignKey: "budgetItemId",
      onDelete: "CASCADE",
    });
    db.Contribution.belongsTo(db.BudgetItem, { foreignKey: "budgetItemId" });
  }
  if (db.Resource && db.Contribution) {
    db.Resource.hasMany(db.Contribution, {
      foreignKey: "resourceId",
      onDelete: "CASCADE",
    });
    db.Contribution.belongsTo(db.Resource, { foreignKey: "resourceId" });
  }
  if (db.Assignment && db.Contribution) {
    db.Assignment.hasMany(db.Contribution, {
      foreignKey: "assignmentId",
      onDelete: "SET NULL",
    });
    db.Contribution.belongsTo(db.Assignment, { foreignKey: "assignmentId" });
  }

  // --- Stripe / Billing relationships ---
  if (db.Customer && db.StripeUser) {
    db.Customer.hasMany(db.StripeUser, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.StripeUser.belongsTo(db.Customer, { foreignKey: "customerId" });
  }
  if (db.User && db.StripeUser) {
    // One Stripe linkage per (customer, user) by DB unique index, but expose as hasMany for simplicity
    db.User.hasMany(db.StripeUser, {
      foreignKey: "userId",
      onDelete: "CASCADE",
    });
    db.StripeUser.belongsTo(db.User, { foreignKey: "userId" });
  }
  // Customer ↔ FeatureEntitlement (tenant-scoped feature flags)
  if (db.Customer && db.FeatureEntitlement) {
    db.Customer.hasMany(db.FeatureEntitlement, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });
    db.FeatureEntitlement.belongsTo(db.Customer, { foreignKey: "customerId" });
  }

  // IMPORTANT:
  // Do NOT use alter:true globally – it breaks Postgres enums when defaults exist.
  // We rely on explicit model additions instead.
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
  try {
    const pool = db.getPgPool && db.getPgPool();
    if (pool) await pool.end();
  } catch (e) {}
  process.exit(0);
});
