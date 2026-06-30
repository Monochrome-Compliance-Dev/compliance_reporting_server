const schemaDefinitionService = require("../services/schemaDefinition.service");
const { success, notFound } = require("@/helpers/controllerHelpers");

function getUserId(req) {
  return req.user?.id || req.user?.userId || null;
}

async function listSchemaDefinitions(req, res, next) {
  try {
    const schemaDefinitions =
      await schemaDefinitionService.listSchemaDefinitions({
        datasetType: req.query.datasetType,
        status: req.query.status,
        schemaKey: req.query.schemaKey,
      });

    return success(res, { schemaDefinitions });
  } catch (err) {
    return next(err);
  }
}

async function getSchemaDefinition(req, res, next) {
  try {
    const schemaDefinition = await schemaDefinitionService.getSchemaDefinition({
      id: req.params.id,
    });

    if (!schemaDefinition) {
      return notFound(res, "Schema Definition not found.");
    }

    return success(res, { schemaDefinition });
  } catch (err) {
    return next(err);
  }
}

async function createSchemaDefinition(req, res, next) {
  try {
    const schemaDefinition =
      await schemaDefinitionService.createSchemaDefinition({
        definition: req.body,
        userId: getUserId(req),
      });

    return success(res, { schemaDefinition }, 201);
  } catch (err) {
    return next(err);
  }
}

async function updateSchemaDefinition(req, res, next) {
  try {
    const schemaDefinition =
      await schemaDefinitionService.updateSchemaDefinition({
        id: req.params.id,
        definition: req.body,
        userId: getUserId(req),
      });

    if (!schemaDefinition) {
      return notFound(res, "Schema Definition not found.");
    }

    return success(res, { schemaDefinition });
  } catch (err) {
    return next(err);
  }
}

async function approveSchemaDefinition(req, res, next) {
  try {
    const schemaDefinition =
      await schemaDefinitionService.approveSchemaDefinition({
        id: req.params.id,
        userId: getUserId(req),
      });

    if (!schemaDefinition) {
      return notFound(res, "Schema Definition not found.");
    }

    return success(res, { schemaDefinition });
  } catch (err) {
    return next(err);
  }
}

async function createNewSchemaDefinitionVersion(req, res, next) {
  try {
    const schemaDefinition =
      await schemaDefinitionService.createNewSchemaDefinitionVersion({
        id: req.params.id,
        userId: getUserId(req),
      });

    if (!schemaDefinition) {
      return notFound(res, "Schema Definition not found.");
    }

    return success(res, { schemaDefinition }, 201);
  } catch (err) {
    return next(err);
  }
}

async function deprecateSchemaDefinition(req, res, next) {
  try {
    const schemaDefinition =
      await schemaDefinitionService.deprecateSchemaDefinition({
        id: req.params.id,
        userId: getUserId(req),
      });

    if (!schemaDefinition) {
      return notFound(res, "Schema Definition not found.");
    }

    return success(res, { schemaDefinition });
  } catch (err) {
    return next(err);
  }
}

module.exports = {
  listSchemaDefinitions,
  getSchemaDefinition,
  createSchemaDefinition,
  updateSchemaDefinition,
  approveSchemaDefinition,
  createNewSchemaDefinitionVersion,
  deprecateSchemaDefinition,
};
