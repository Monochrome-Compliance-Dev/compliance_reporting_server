const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const clientService = require("./client.service");
const logger = require("../helpers/logger");
const { clientSchema, clientUpdateSchema } = require("./client.validator");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getById);
router.post("/", validateRequest(clientSchema), create);
// router.post("/", authorise(), validateRequest(clientSchema), create);
router.put("/:id", authorise(), validateRequest(clientUpdateSchema), update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  logger.info(
    `[ClientController] Retrieved all clients by user ${req?.user?.id || "unknown"}`
  );
  clientService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  logger.info(
    `[ClientController] Retrieved client by ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  clientService
    .getById(req.params.id)
    .then((client) => (client ? res.json(client) : res.sendStatus(404)))
    .catch(next);
}

function create(req, res, next) {
  console.log("Creating client with data:", req.body); // Log the request body
  logger.info(
    `[ClientController] Created new client by user ${req?.user?.id || "unknown"}`
  );
  clientService
    .create(req.body)
    .then((client) => {
      logger.info(`Client created via API: ${client.id}`);
      res.json(client);
    })
    .catch((error) => {
      console.error("Error creating client:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  logger.info(
    `[ClientController] Updated client ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  clientService
    .update(req.params.id, req.body)
    .then((client) => {
      logger.info(`Client updated via API: ${req.params.id}`);
      res.json(client);
    })
    .catch(next);
}

function _delete(req, res, next) {
  logger.info(
    `[ClientController] Deleted client ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  clientService
    .delete(req.params.id)
    .then(() => {
      logger.info(`Client deleted via API: ${req.params.id}`);
      res.json({ message: "Client deleted successfully" });
    })
    .catch((error) => {
      console.error("Error deleting client:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
