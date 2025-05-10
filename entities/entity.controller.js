const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const entityService = require("./entity.service");
const upload = require("../middleware/upload");
const { sendAttachmentEmail } = require("../helpers/send-email");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/entity/:id", authorise(), getEntityByReportId);
router.get("/:id", authorise(), getById);
router.post("/", create);
router.post("/send-email", upload.single("attachment"), sendPdfEmail);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  entityService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  entityService
    .getAllByReportId(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function getEntityByReportId(req, res, next) {
  entityService
    .getEntityByReportId(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function getById(req, res, next) {
  entityService
    .getById(req.params.id)
    .then((entity) => (entity ? res.json(entity) : res.sendSentityus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    entityName: Joi.string().required(),
    entityABN: Joi.string().allow(null, ""),
    startEntity: Joi.string().required(),
    section7: Joi.string().required(),
    cce: Joi.string().required(),
    charity: Joi.string().required(),
    connectionToAustralia: Joi.string().required(),
    revenue: Joi.string().required(),
    controlled: Joi.string().required(),
    stoppedReason: Joi.string().required(),
    completed: Joi.boolean().required(),
    timestamp: Joi.date().required(),
    createdBy: Joi.number().required(),
    updatedBy: Joi.number().allow(null),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  entityService
    .create(req.body)
    .then((entity) => res.json(entity))
    .catch((error) => {
      console.error("Error creating entity:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function sendPdfEmail(req, res, next) {
  // console.log("Sending email with attachment...");
  // console.log("Headers:", req.headers);
  // console.log("Content-Type:", req.headers["content-type"]); // Log the Content-Type header
  // console.log("Form data (req.body):", req.body);
  // console.log("Uploaded file (req.file):", req.file);

  if (!req.file) {
    return res.status(400).json({ message: "Attachment is required" });
  }

  sendAttachmentEmail(req, res)
    .then(() => res.json({ message: "Email sent successfully" }))
    .catch((error) => {
      console.error("Error sending email:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function update(req, res, next) {
  entityService
    .update(req.params.id, req.body)
    .then((entity) => res.json(entity))
    .catch(next);
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    entityName: Joi.string().required(),
    entityABN: Joi.string().allow(null, ""),
    startEntity: Joi.string().allow(null, ""),
    section7: Joi.string().allow(null, ""),
    cce: Joi.string().allow(null, ""),
    charity: Joi.string().allow(null, ""),
    connectionToAustralia: Joi.string().allow(null, ""),
    revenue: Joi.string().allow(null, ""),
    controlled: Joi.string().allow(null, ""),
    stoppedReason: Joi.string().allow(null, ""),
    completed: Joi.boolean().allow(false),
    timestamp: Joi.date().allow(null),
    // createdBy: Joi.number().required(),
    updatedBy: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function _delete(req, res, next) {
  entityService
    .delete(req.params.id)
    .then(() => res.json({ message: "Entity deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting entity:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
