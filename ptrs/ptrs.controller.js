const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const ptrsService = require("./ptrs.service");
const { ptrsSchema } = require("./ptrs.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/ptrs/:id", authorise(), getById);
router.post("/", authorise(), validateRequest(ptrsSchema), create);
router.put("/:id", authorise(), validateRequest(ptrsSchema), update);
router.patch("/:id", authorise(), patch);
router.delete("/:id", authorise(), _delete);

module.exports = router;

async function getAll(req, res, next) {
  const timestamp = new Date().toISOString();
  try {
    const clientId = req.auth?.clientId;
    const userId = req.auth?.id;
    logger.logEvent("info", "Fetching all ptrs", {
      action: "GetAllPtrs",
      userId,
      clientId,
      timestamp,
    });
    const ptrs = await ptrsService.getAll({ clientId });
    logger.logEvent("info", "Fetched all ptrs", {
      action: "GetAllPtrs",
      userId,
      clientId,
      count: Array.isArray(ptrs) ? ptrs.length : undefined,
      timestamp,
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error fetching all ptrs", {
      action: "GetAllPtrs",
      userId: req.auth?.id,
      clientId: req.auth?.clientId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function getById(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Fetching ptrs by ID", {
    action: "GetPtrsById",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    const ptrs = await ptrsService.getById({ id, clientId });
    if (ptrs) {
      logger.logEvent("info", "Fetched ptrs by ID", {
        action: "GetPtrsById",
        id,
        clientId,
        userId,
        timestamp,
      });
      res.json({ status: "success", data: ptrs });
    } else {
      logger.logEvent("warn", "Ptrs not found", {
        action: "GetPtrsById",
        id,
        clientId,
        userId,
        timestamp,
      });
      res.sendStatus(404);
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching ptrs by ID", {
      action: "GetPtrsById",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function create(req, res, next) {
  const timestamp = new Date().toISOString();
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Creating ptrs", {
    action: "CreatePtrs",
    clientId,
    userId,
    timestamp,
  });
  try {
    const ptrs = await ptrsService.create({ data: req.body, clientId });
    logger.logEvent("info", "Ptrs created", {
      action: "CreatePtrs",
      id: ptrs.id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error creating ptrs", {
      action: "CreatePtrs",
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function update(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Updating ptrs", {
    action: "UpdatePtrs",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    const ptrs = await ptrsService.update({ id, data: req.body, clientId });
    logger.logEvent("info", "Ptrs updated", {
      action: "UpdatePtrs",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error updating ptrs", {
      action: "UpdatePtrs",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function patch(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Patching ptrs", {
    action: "PatchPtrs",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    if (!clientId) {
      logger.logEvent("warn", "No clientId found for RLS - skipping", {
        action: "PatchPtrs",
        id,
        userId,
        timestamp,
      });
      return res.status(400).json({ message: "Client ID missing" });
    }
    const ptrs = await ptrsService.patch({ id, data: req.body, clientId });
    logger.logEvent("info", "Ptrs patched", {
      action: "PatchPtrs",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.json({ status: "success", data: ptrs });
  } catch (error) {
    logger.logEvent("error", "Error patching ptrs", {
      action: "PatchPtrs",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}

async function _delete(req, res, next) {
  const timestamp = new Date().toISOString();
  const id = req.params.id;
  const clientId = req.auth?.clientId;
  const userId = req.auth?.id;
  logger.logEvent("info", "Deleting ptrs", {
    action: "DeletePtrs",
    id,
    clientId,
    userId,
    timestamp,
  });
  try {
    if (!clientId) {
      logger.logEvent("warn", "No clientId found for RLS - skipping", {
        action: "DeletePtrs",
        id,
        userId,
        timestamp,
      });
      return res.status(400).json({ message: "Client ID missing" });
    }
    await ptrsService.delete({ id, clientId });
    logger.logEvent("info", "Ptrs deleted", {
      action: "DeletePtrs",
      id,
      clientId,
      userId,
      timestamp,
    });
    res.status(204).json({ status: "success" });
  } catch (error) {
    logger.logEvent("error", "Error deleting ptrs", {
      action: "DeletePtrs",
      id,
      clientId,
      userId,
      error: error.message,
      timestamp: new Date().toISOString(),
    });
    next(error);
  }
}
