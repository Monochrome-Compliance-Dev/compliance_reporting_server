const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const adminService = require("./admin.service");

// routes
router.get("/", authorise(), getAll);
router.get("/record/:clientId", authorise(), getAllById);
router.get("/client/:clientId", getAdminsByClientId);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  adminService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllById(req, res, next) {
  console.log("Client ID:", req.params); // Log the client ID for debugging
  adminService
    .getAllById(req.params.clientId)
    .then((admins) => {
      if (!admins || admins.length === 0) {
        console.log("No admins found for clientId:", req.params.clientId);
        return res.status(404).json({ message: "No admins found" });
      }
      res.json(admins);
    })
    .catch((error) => {
      console.error("Error fetching admin:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function getAdminsByClientId(req, res, next) {
  const clientId = req.params.clientId;

  adminService
    .getAllById(clientId)
    .then((admins) => {
      if (!admins || admins.length === 0) {
        console.log("No admins found for clientId:", clientId);
        return res.status(404).json({ message: "No admins found" });
      }
      res.json(admins);
    })
    .catch((error) => {
      console.error("Error fetching admin:", error);
      next(error); // Pass the error to the global error handler
    });
}

function getById(req, res, next) {
  adminService
    .getById(req.params.id)
    .then((admin) => (admin ? res.json(admin) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    ReportingPeriodStartDate: Joi.string().required(),
    ReportingPeriodEndDate: Joi.string().required(),
    adminName: Joi.string().required(),
    createdBy: Joi.number().required(),
    adminStatus: Joi.string().required(),
    clientId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  adminService
    .create(req.body)
    .then((admin) => res.json(admin))
    .catch((error) => {
      console.error("Error creating admin:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    adminName: Joi.string().required(),
    abn: Joi.string().required(),
    acn: Joi.string().required(),
    addressline1: Joi.string().required(),
    city: Joi.string().required(),
    state: Joi.string().required(),
    postcode: Joi.string().required(),
    country: Joi.string().required(),
    postaladdressline1: Joi.string().required(),
    postalcity: Joi.string().required(),
    postalstate: Joi.string().required(),
    postalpostcode: Joi.string().required(),
    postalcountry: Joi.string().required(),
    industryCode: Joi.string().required(),
    contactFirst: Joi.string().required(),
    contactLast: Joi.string().required(),
    contactPosition: Joi.string().required(),
    contactEmail: Joi.string().required(),
    contactPhone: Joi.string().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  adminService
    .update(req.params.id, req.body)
    .then((admin) => res.json(admin))
    .catch(next);
}

function _delete(req, res, next) {
  adminService
    .delete(req.params.id)
    .then(() => res.json({ message: "Admin deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting admin:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
