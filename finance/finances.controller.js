const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const financeService = require("./finance.service");
const { add } = require("winston");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getByReportId);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  financeService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getByReportId(req, res, next) {
  financeService
    .getByReportId(req.params.id)
    .then((finance) => (finance ? res.json(finance) : res.sendStatus(404)))
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    InvoicePracticesAndArrangements: Joi.string(),
    PracticesAndArrangementsForLodgingTenders: Joi.string(),
    PracticesAndArrangementsToAcceptInvoices: Joi.string(),
    TotalValueOfSmallBusinessProcurement: Joi.string(),
    SupplyChainFinanceArrangements: Joi.string(),
    TotalNumberSupplyChainFinanceArrangements: Joi.string(),
    TotalValueSupplyChainFinanceArrangements: Joi.string(),
    BenefitsOfSupplyChainFinanceArrangements: Joi.string(),
    RequirementToUseSupplyChainFinanceArrangements: Joi.string(),
    DetailOfChangeInAccountingPeriod: Joi.string(),
    DetailOfChangeInBusinessName: Joi.string(),
    DetailEntitesBelowReportingThreshold: Joi.string(),

    createdBy: Joi.number().required(),
    updatedBy: Joi.number(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  financeService
    .create(req.body)
    .then((finance) => res.json(finance))
    .catch((error) => {
      console.error("Error creating finance:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    InvoicePracticesAndArrangements: Joi.string(),
    PracticesAndArrangementsForLodgingTenders: Joi.string(),
    PracticesAndArrangementsToAcceptInvoices: Joi.string(),
    TotalValueOfSmallBusinessProcurement: Joi.string(),
    SupplyChainFinanceArrangements: Joi.string(),
    TotalNumberSupplyChainFinanceArrangements: Joi.string(),
    TotalValueSupplyChainFinanceArrangements: Joi.string(),
    BenefitsOfSupplyChainFinanceArrangements: Joi.string(),
    RequirementToUseSupplyChainFinanceArrangements: Joi.string(),
    DetailOfChangeInAccountingPeriod: Joi.string(),
    DetailOfChangeInBusinessName: Joi.string(),
    DetailEntitesBelowReportingThreshold: Joi.string(),

    createdBy: Joi.number().required(),
    updatedBy: Joi.number().required(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  financeService
    .update(req.params.id, req.body)
    .then((finance) => res.json(finance))
    .catch(next);
}

function _delete(req, res, next) {
  financeService
    .delete(req.params.id)
    .then(() => res.json({ message: "Finance deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting finance:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
