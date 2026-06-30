const { Op } = require("sequelize");
const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const { SCHEMA_STATUSES } = require("../constants/schemaDefinition.constants");
const {
  validateSchemaDefinition,
} = require("../validators/schemaDefinition.validator");

function toPlain(row) {
  if (!row) return null;
  return row.get ? row.get({ plain: true }) : row;
}

function normaliseSchemaDefinition(row) {
  const plain = toPlain(row);
  if (!plain) return null;

  return {
    id: plain.id,
    schemaKey: plain.schemaKey,
    name: plain.name,
    datasetType: plain.datasetType,
    version: plain.version,
    status: plain.status,
    description: plain.description || null,
    definition: plain.definition,
    createdBy: plain.createdBy || null,
    updatedBy: plain.updatedBy || null,
    createdAt: plain.createdAt,
    updatedAt: plain.updatedAt,
  };
}

function getSourceSchemaDefinitionModel() {
  if (!db.SourceSchemaDefinition) {
    throw new Error("SourceSchemaDefinition model is not registered on db");
  }
  return db.SourceSchemaDefinition;
}

function getSequelize() {
  if (!db.sequelize) {
    throw new Error("sequelize instance is not registered on db");
  }
  return db.sequelize;
}

function createHttpError(message, statusCode = 400) {
  const err = new Error(message);
  err.statusCode = statusCode;
  return err;
}

function assertValidDefinition(definition) {
  const errors = validateSchemaDefinition(definition);
  if (errors.length) {
    throw createHttpError(errors.join(" "), 400);
  }
}

function normaliseKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase();
}

function prepareDefinitionPayload(definition, overrides = {}) {
  const prepared = {
    ...definition,
    ...overrides,
  };

  prepared.schemaKey = normaliseKey(prepared.schemaKey);
  prepared.datasetType = normaliseKey(prepared.datasetType);
  prepared.version = Number(prepared.version);
  prepared.fields = Array.isArray(prepared.fields) ? prepared.fields : [];

  return prepared;
}

function buildPersistencePayload(definition, userId) {
  return {
    schemaKey: definition.schemaKey,
    name: definition.name,
    datasetType: definition.datasetType,
    version: definition.version,
    status: definition.status,
    description: definition.description || null,
    definition,
    createdBy: userId || null,
    updatedBy: userId || null,
  };
}

async function listSchemaDefinitions({ datasetType, status, schemaKey } = {}) {
  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const where = {};

  if (datasetType) where.datasetType = normaliseKey(datasetType);
  if (status) where.status = status;
  if (schemaKey) where.schemaKey = normaliseKey(schemaKey);

  try {
    const rows = await SourceSchemaDefinition.findAll({
      where,
      order: [
        ["schemaKey", "ASC"],
        ["version", "DESC"],
      ],
    });

    return rows.map(normaliseSchemaDefinition);
  } catch (err) {
    logger?.error?.("Failed to list Schema Definitions", {
      action: "SchemaEngineListSchemaDefinitions",
      datasetType,
      status,
      schemaKey,
      error: err.message,
    });
    throw err;
  }
}

async function getSchemaDefinition({ id } = {}) {
  if (!id) throw createHttpError("id is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();

  try {
    const row = await SourceSchemaDefinition.findByPk(id);
    return normaliseSchemaDefinition(row);
  } catch (err) {
    logger?.error?.("Failed to get Schema Definition", {
      action: "SchemaEngineGetSchemaDefinition",
      id,
      error: err.message,
    });
    throw err;
  }
}

async function getActiveSchemaForDatasetType(datasetType) {
  if (!datasetType) throw createHttpError("datasetType is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const normalisedDatasetType = normaliseKey(datasetType);

  try {
    const row = await SourceSchemaDefinition.findOne({
      where: {
        datasetType: normalisedDatasetType,
        status: SCHEMA_STATUSES.APPROVED,
      },
      order: [["version", "DESC"]],
    });

    return normaliseSchemaDefinition(row);
  } catch (err) {
    logger?.error?.("Failed to get active Schema Definition for dataset type", {
      action: "SchemaEngineGetActiveSchemaForDatasetType",
      datasetType: normalisedDatasetType,
      error: err.message,
    });
    throw err;
  }
}

async function createSchemaDefinition({ definition, userId } = {}) {
  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const sequelize = getSequelize();

  const prepared = prepareDefinitionPayload(definition || {}, {
    version: definition?.version || 1,
    status: SCHEMA_STATUSES.DRAFT,
  });

  assertValidDefinition(prepared);

  const transaction = await sequelize.transaction();

  try {
    const existing = await SourceSchemaDefinition.findOne({
      where: {
        schemaKey: prepared.schemaKey,
        version: prepared.version,
      },
      transaction,
    });

    if (existing) {
      throw createHttpError(
        `Schema Definition '${prepared.schemaKey}' version ${prepared.version} already exists.`,
        409,
      );
    }

    const row = await SourceSchemaDefinition.create(
      buildPersistencePayload(prepared, userId),
      { transaction },
    );

    await transaction.commit();
    return normaliseSchemaDefinition(row);
  } catch (err) {
    await rollbackQuietly(transaction);
    logger?.error?.("Failed to create Schema Definition", {
      action: "SchemaEngineCreateSchemaDefinition",
      schemaKey: prepared.schemaKey,
      version: prepared.version,
      error: err.message,
    });
    throw err;
  }
}

async function updateSchemaDefinition({ id, definition, userId } = {}) {
  if (!id) throw createHttpError("id is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const sequelize = getSequelize();
  const transaction = await sequelize.transaction();

  try {
    const row = await SourceSchemaDefinition.findByPk(id, { transaction });
    if (!row) return null;

    if (row.status !== SCHEMA_STATUSES.DRAFT) {
      throw createHttpError(
        "Only Draft Schema Definitions can be updated.",
        409,
      );
    }

    const prepared = prepareDefinitionPayload(definition || {}, {
      schemaKey: row.schemaKey,
      version: row.version,
      status: SCHEMA_STATUSES.DRAFT,
    });

    assertValidDefinition(prepared);

    await row.update(
      {
        name: prepared.name,
        datasetType: prepared.datasetType,
        description: prepared.description || null,
        definition: prepared,
        updatedBy: userId || null,
      },
      { transaction },
    );

    await transaction.commit();
    return normaliseSchemaDefinition(row);
  } catch (err) {
    await rollbackQuietly(transaction);
    logger?.error?.("Failed to update Schema Definition", {
      action: "SchemaEngineUpdateSchemaDefinition",
      id,
      error: err.message,
    });
    throw err;
  }
}

async function approveSchemaDefinition({ id, userId } = {}) {
  if (!id) throw createHttpError("id is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const sequelize = getSequelize();
  const transaction = await sequelize.transaction();

  try {
    const row = await SourceSchemaDefinition.findByPk(id, { transaction });
    if (!row) return null;

    if (row.status !== SCHEMA_STATUSES.DRAFT) {
      throw createHttpError(
        "Only Draft Schema Definitions can be approved.",
        409,
      );
    }

    const definition = prepareDefinitionPayload(row.definition, {
      schemaKey: row.schemaKey,
      version: row.version,
      status: SCHEMA_STATUSES.APPROVED,
    });

    assertValidDefinition(definition);

    await row.update(
      {
        status: SCHEMA_STATUSES.APPROVED,
        definition,
        updatedBy: userId || null,
      },
      { transaction },
    );

    await transaction.commit();
    return normaliseSchemaDefinition(row);
  } catch (err) {
    await rollbackQuietly(transaction);
    logger?.error?.("Failed to approve Schema Definition", {
      action: "SchemaEngineApproveSchemaDefinition",
      id,
      error: err.message,
    });
    throw err;
  }
}

async function createNewSchemaDefinitionVersion({ id, userId } = {}) {
  if (!id) throw createHttpError("id is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const sequelize = getSequelize();
  const transaction = await sequelize.transaction();

  try {
    const source = await SourceSchemaDefinition.findByPk(id, { transaction });
    if (!source) return null;

    if (source.status !== SCHEMA_STATUSES.APPROVED) {
      throw createHttpError(
        "Only Approved Schema Definitions can be used to create a new version.",
        409,
      );
    }

    const existingDraft = await SourceSchemaDefinition.findOne({
      where: {
        schemaKey: source.schemaKey,
        status: SCHEMA_STATUSES.DRAFT,
        id: { [Op.ne]: source.id },
      },
      transaction,
    });

    if (existingDraft) {
      throw createHttpError(
        `A Draft version already exists for Schema Definition '${source.schemaKey}'.`,
        409,
      );
    }

    const latest = await SourceSchemaDefinition.max("version", {
      where: { schemaKey: source.schemaKey },
      transaction,
    });
    const nextVersion = Number(latest || source.version || 0) + 1;

    const definition = prepareDefinitionPayload(source.definition, {
      schemaKey: source.schemaKey,
      version: nextVersion,
      status: SCHEMA_STATUSES.DRAFT,
    });

    assertValidDefinition(definition);

    const row = await SourceSchemaDefinition.create(
      buildPersistencePayload(definition, userId),
      { transaction },
    );

    await transaction.commit();
    return normaliseSchemaDefinition(row);
  } catch (err) {
    await rollbackQuietly(transaction);
    logger?.error?.("Failed to create new Schema Definition version", {
      action: "SchemaEngineCreateNewSchemaDefinitionVersion",
      id,
      error: err.message,
    });
    throw err;
  }
}

async function deprecateSchemaDefinition({ id, userId } = {}) {
  if (!id) throw createHttpError("id is required", 400);

  const SourceSchemaDefinition = getSourceSchemaDefinitionModel();
  const sequelize = getSequelize();
  const transaction = await sequelize.transaction();

  try {
    const row = await SourceSchemaDefinition.findByPk(id, { transaction });
    if (!row) return null;

    if (row.status === SCHEMA_STATUSES.DEPRECATED) {
      await transaction.commit();
      return normaliseSchemaDefinition(row);
    }

    const definition = prepareDefinitionPayload(row.definition, {
      schemaKey: row.schemaKey,
      version: row.version,
      status: SCHEMA_STATUSES.DEPRECATED,
    });

    assertValidDefinition(definition);

    await row.update(
      {
        status: SCHEMA_STATUSES.DEPRECATED,
        definition,
        updatedBy: userId || null,
      },
      { transaction },
    );

    await transaction.commit();
    return normaliseSchemaDefinition(row);
  } catch (err) {
    await rollbackQuietly(transaction);
    logger?.error?.("Failed to deprecate Schema Definition", {
      action: "SchemaEngineDeprecateSchemaDefinition",
      id,
      error: err.message,
    });
    throw err;
  }
}

function rollbackQuietly(transaction) {
  if (!transaction || transaction.finished) return Promise.resolve();
  return transaction.rollback().catch(() => {});
}

module.exports = {
  listSchemaDefinitions,
  getSchemaDefinition,
  getActiveSchemaForDatasetType,
  createSchemaDefinition,
  updateSchemaDefinition,
  approveSchemaDefinition,
  createNewSchemaDefinitionVersion,
  deprecateSchemaDefinition,
};
