const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const ptrsService = require("./ptrs.service");
const { add } = require("winston");

// routes
router.get("/", authorise(), getAll);
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

function getById(req, res, next) {
  ptrsService
    .getById(req.params.id)
    .then((ptrs) => (ptrs ? res.json(ptrs) : res.sendStatus(404)))
    .catch(next);
}

function bulkPrep(req, res, next) {
  // Need to destructure the request body to get the array of objects
  console.log("Request body:", typeof req.body);
  req.body.map((item) => {
    createSchema({ body: item }, res, next);
    // Call the create function for each item
    // ptrsService
    //   .create(item)
    //   .then((ptrs) => {
    //     console.log("Created ptrs:", ptrs);
    //   })
    //   .catch((error) => {
    //     console.error("Error creating ptrs:", error); // Log the error details
    //     next(error); // Pass the error to the global error handler
    //   });
  });
}

function createSchema(req, res, next) {
  console.log("Request body:", req.body);
  // const schema = Joi.object({
  //   payerEntityName: Joi.string().required(),
  //   payerEntityAbn: Joi.string().allow(null, ""),
  //   payerEntityAcnArbn: Joi.string().allow(null, ""),
  //   payeeEntityName: Joi.string().required(),
  //   payeeEntityAbn: Joi.string().allow(null, ""),
  //   payeeEntityAcnArbn: Joi.string().allow(null, ""),
  //   paymentAmount: Joi.string().required(),
  //   description: Joi.string().allow(null, ""),
  //   supplyDate: Joi.date().allow(null, ""),
  //   paymentDate: Joi.date().required(),
  //   contractPoReferenceNumber: Joi.string().allow(null, ""),
  //   contractPoPaymentTerms: Joi.string().allow(null, ""),
  //   noticeForPaymentIssueDate: Joi.date().allow(null, ""),
  //   noticeForPaymentTerms: Joi.string().allow(null, ""),
  //   invoiceReferenceNumber: Joi.string().allow(null, ""),
  //   invoiceIssueDate: Joi.date().allow(null, ""),
  //   invoiceReceiptDate: Joi.date().allow(null, ""),
  //   invoicePaymentTerms: Joi.string().allow(null, ""),
  //   invoiceDueDate: Joi.date().allow(null, ""),
  //   isTcp: Joi.boolean().required(),
  //   comment: Joi.string().allow(null, ""),
  //   updatedBy: Joi.number().required(),
  // });
  // validateRequest(req, next, schema);
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
    payerEntityAbn: Joi.string().allow(null, ""),
    payerEntityAcnArbn: Joi.string().allow(null, ""),
    payeeEntityName: Joi.string(),
    payeeEntityAbn: Joi.string().allow(null, ""),
    payeeEntityAcnArbn: Joi.string().allow(null, ""),
    paymentAmount: Joi.string(),
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
