const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const clientService = require("./client.service");
const { logger } = require("../helpers/logger");
const { clientSchema, clientUpdateSchema } = require("./client.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", validateRequest(clientSchema), create);
router.put(
  "/:id",
  authorise(["Admin", "Boss"]),
  validateRequest(clientUpdateSchema),
  update
);
router.delete("/:id", authorise(["Admin", "Boss"]), _delete);

module.exports = router;

function getAll(req, res, next) {
  logger.logEvent("info", "Retrieved all clients", {
    action: "GetAllClients",
    userId: req?.user?.id || "unknown",
  });
  clientService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  logger.logEvent("info", "Retrieved client by ID", {
    action: "GetClient",
    clientId: req.params.id,
    userId: req?.user?.id || "unknown",
  });
  clientService
    .getById(req.params.id)
    .then((client) => (client ? res.json(client) : res.sendStatus(404)))
    .catch(next);
}

function create(req, res, next) {
  const clientId = req.auth.clientId; // Explicitly grab from auth
  logger.logEvent("info", "Creating client", {
    action: "CreateClient",
    userId: req?.user?.id || "anonymous",
    ip: req.ip,
    userAgent: req.headers["user-agent"],
    payload: req.body,
    clientId,
  });
  clientService
    .create({ ...req.body, clientId }) // Explicitly pass clientId
    .then((client) => {
      logger.logEvent("info", "Client created via API", {
        action: "CreateClient",
        clientId: client.id,
      });
      res.json(client);
    })
    .catch((error) => {
      logger.logEvent("error", "Error creating client", {
        action: "CreateClient",
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  const clientId = req.auth.clientId;
  logger.logEvent("info", "Updating client", {
    action: "UpdateClient",
    clientId: req.params.id,
    userId: req?.user?.id || "unknown",
    payload: req.body,
  });
  clientService
    .update(req.params.id, { ...req.body, clientId }) // Explicitly pass clientId
    .then((client) => {
      logger.logEvent("info", "Client updated via API", {
        action: "UpdateClient",
        clientId: req.params.id,
        paymentConfirmed: req.body?.paymentConfirmed,
      });
      res.json(client);
    })
    .catch(next);
}

function _delete(req, res, next) {
  logger.logEvent("info", "Deleting client", {
    action: "DeleteClient",
    clientId: req.params.id,
    userId: req?.user?.id || "unknown",
  });
  clientService
    .delete(req.params.id)
    .then(() => {
      logger.logEvent("warn", "Client deleted via API", {
        action: "DeleteClient",
        clientId: req.params.id,
      });
      res.json({ message: "Client deleted successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error deleting client", {
        action: "DeleteClient",
        error: error.message,
      });
      next(error); // Pass the error to the global error handler
    });
}
