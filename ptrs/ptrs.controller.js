const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const ptrsService = require("./ptrs.service");
const { add } = require("winston");

// routes
router.get("/", authorise(), getAll);
router.get("/report/:id", authorise(), getAllByReportId);
router.get("/:id", authorise(), getById);
router.post("/", authorise(), bulkPrep);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  ptrsService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  ptrsService
    .getAllByReportId(req.params.id)
    .then((ptrs) => (ptrs ? res.json(ptrs) : res.sendStatus(404)))
    .catch(next);
}

function getById(req, res, next) {
  ptrsService
    .getById(req.params.id)
    .then((ptrs) => (ptrs ? res.json(ptrs) : res.sendStatus(404)))
    .catch(next);
}

function bulkPrep(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Validate each record using createSchema
          const reqForValidation = { body: record };
          await validateRecord(reqForValidation);

          // Save each record using ptrsService
          return await ptrsService.create(record);
        } catch (error) {
          console.error("Error processing record:", error);
          throw error; // Propagate the error to Promise.all
        }
      });
    });

    // Wait for all records to be saved
    Promise.all(promises)
      .then((results) =>
        res.json({
          success: true,
          message: "All records saved successfully",
          results,
        })
      )
      .catch((error) => {
        console.error("Error saving records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

async function validateRecord(req) {
  const schema = Joi.object({
    payerEntityName: Joi.string().required(),
    payerEntityAbn: Joi.number().allow(null), // Changed to number
    payerEntityAcnArbn: Joi.number().allow(null),
    payeeEntityName: Joi.string().required(),
    payeeEntityAbn: Joi.number().allow(null), // Changed to number
    payeeEntityAcnArbn: Joi.number().allow(null), // Changed to number
    paymentAmount: Joi.number().required(), // Changed to number
    description: Joi.string().allow(null, ""),
    supplyDate: Joi.date().allow(null, ""),
    paymentDate: Joi.date().required(),
    contractPoReferenceNumber: Joi.string().allow(null, ""),
    contractPoPaymentTerms: Joi.string().allow(null, ""),
    noticeForPaymentIssueDate: Joi.date().allow(null, ""),
    noticeForPaymentTerms: Joi.string().allow(null, ""),
    invoiceReferenceNumber: Joi.string().allow(null, ""),
    invoiceIssueDate: Joi.date().allow(null, ""),
    invoiceReceiptDate: Joi.date().allow(null, ""),
    invoicePaymentTerms: Joi.string().allow(null, ""),
    invoiceDueDate: Joi.date().allow(null, ""),
    isTcp: Joi.boolean().allow(null, ""),
    comment: Joi.string().allow(null, ""),
    updatedBy: Joi.number().required(),
    reportId: Joi.number().required(),
  });

  // Validate the request body
  await schema.validateAsync(req.body);
}

function create(req, res, next) {
  ptrsService
    .create(req.body)
    .then((ptrs) => res.json(ptrs))
    .catch((error) => {
      console.error("Error creating ptrs:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    payerEntityName: Joi.string(),
    payerEntityAbn: Joi.number().allow(null), // Changed to number
    payerEntityAcnArbn: Joi.string().allow(null, ""),
    payeeEntityName: Joi.string(),
    payeeEntityAbn: Joi.number().allow(null), // Changed to number
    payeeEntityAcnArbn: Joi.number().allow(null), // Changed to number
    paymentAmount: Joi.number(), // Changed to number
    description: Joi.string().allow(null, ""),
    supplyDate: Joi.date().allow(null, ""),
    paymentDate: Joi.date(),
    contractPoReferenceNumber: Joi.string().allow(null, ""),
    contractPoPaymentTerms: Joi.string().allow(null, ""),
    noticeForPaymentIssueDate: Joi.date().allow(null, ""),
    noticeForPaymentTerms: Joi.string().allow(null, ""),
    invoiceReferenceNumber: Joi.string().allow(null, ""),
    invoiceIssueDate: Joi.date().allow(null, ""),
    invoiceReceiptDate: Joi.date().allow(null, ""),
    invoicePaymentTerms: Joi.string().allow(null, ""),
    invoiceDueDate: Joi.date().allow(null, ""),
    isTcp: Joi.boolean(),
    comment: Joi.string().allow(null, ""),
    updatedBy: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  ptrsService
    .update(req.params.id, req.body)
    .then((ptrs) => res.json(ptrs))
    .catch(next);
}

function _delete(req, res, next) {
  ptrsService
    .delete(req.params.id)
    .then(() => res.json({ message: "Ptrs deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting ptrs:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
