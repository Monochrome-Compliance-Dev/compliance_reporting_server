const express = require("express");
const router = express.Router();
const Joi = require("joi");
const authorise = require("../middleware/authorise");
const tcpService = require("./tcp.service");
const setClientContext = require("../middleware/set-client-context");
const validateRequest = require("../middleware/validate-request");
const { tcpSchema } = require("./tcp.validator");

// routes
router.get("/", authorise(), setClientContext, getAll);
router.get("/report/:id", authorise(), setClientContext, getAllByReportId);
router.get("/tcp/:id", authorise(), setClientContext, getTcpByReportId);
router.get("/:id", authorise(), setClientContext, getById);
router.post(
  "/",
  authorise(),
  validateRequest(tcpSchema),
  setClientContext,
  bulkCreate
);
router.put(
  "/",
  authorise(),
  validateRequest(tcpSchema),
  setClientContext,
  bulkUpdate
);
router.put("/partial", authorise(), setClientContext, partialUpdate);
router.put("/sbi/:id", authorise(), setClientContext, sbiUpdate);
router.delete("/:id", authorise(), setClientContext, _delete);

module.exports = router;

function getAll(req, res, next) {
  tcpService
    .getAll(req.auth.clientId)
    .then((entities) => res.json(entities))
    .catch(next);
}

function getAllByReportId(req, res, next) {
  tcpService
    .getAllByReportId(req.params.id, req.auth.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

function getTcpByReportId(req, res, next) {
  tcpService
    .getTcpByReportId(req.params.id, req.auth.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

function sbiUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Validate each record using sbiSchema
          const reqForValidation = { body: record };
          await validateSbiRecord(reqForValidation);

          // Save each record using tcpService
          return await tcpService.sbiUpdate(
            req.params.id,
            record,
            req.auth.clientId
          );
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

async function validateSbiRecord(req) {
  const schema = Joi.object({
    payeeEntityAbn: Joi.number().required(),
  });
  // Validate the request body
  await schema.validateAsync(req.body);
}

function partialUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);
    console.log("Records to update:", records);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Exclude id and createdAt fields from the record
          const { id, createdAt, ...recordToUpdate } = record;

          // Validate each record using bulkUpdateSchema
          const reqForValidation = { body: recordToUpdate };
          await partialUpdateSchema(reqForValidation); // Validate the record

          // Save each record using tcpService
          return await tcpService.update(
            record.id,
            recordToUpdate,
            req.auth.clientId
          );
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
          message: "All records updated successfully",
          results,
        })
      )
      .catch((error) => {
        console.error("Error updating records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

async function partialUpdateSchema(req) {
  const schema = Joi.object({
    partialPayment: Joi.boolean().required(),
    updatedBy: Joi.number().required(),
  });

  // Validate the request body
  await schema.validateAsync(req.body);
}

function getById(req, res, next) {
  tcpService
    .getById(req.params.id, req.auth.clientId)
    .then((tcp) => (tcp ? res.json(tcp) : res.sendStatus(404)))
    .catch(next);
}

function bulkCreate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Validate each record using createSchema
          // const reqForValidation = { body: record };
          // await validateRecord(reqForValidation);

          // Save each record using tcpService
          return await tcpService.create(record, req.auth.clientId);
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

// async function validateRecord(req) {
//   const schema = Joi.object({
//     payerEntityName: Joi.string().required(),
//     payerEntityAbn: Joi.number().allow(null), // Changed to number
//     payerEntityAcnArbn: Joi.number().allow(null),
//     payeeEntityName: Joi.string().required(),
//     payeeEntityAbn: Joi.number().allow(null), // Changed to number
//     payeeEntityAcnArbn: Joi.number().allow(null), // Changed to number
//     paymentAmount: Joi.number().required(), // Changed to number
//     description: Joi.string().allow(null, ""),
//     supplyDate: Joi.date().allow(null, ""),
//     paymentDate: Joi.date().required(),
//     contractPoReferenceNumber: Joi.string().allow(null, ""),
//     contractPoPaymentTerms: Joi.string().allow(null, ""),
//     noticeForPaymentIssueDate: Joi.date().allow(null, ""),
//     noticeForPaymentTerms: Joi.string().allow(null, ""),
//     invoiceReferenceNumber: Joi.string().allow(null, ""),
//     invoiceIssueDate: Joi.date().allow(null, ""),
//     invoiceReceiptDate: Joi.date().allow(null, ""),
//     invoiceAmount: Joi.number().allow(null),
//     invoicePaymentTerms: Joi.string().allow(null, ""),
//     invoiceDueDate: Joi.date().allow(null, ""),
//     isTcp: Joi.boolean().allow(null, ""),
//     tcpExclusion: Joi.string().allow(null, ""),
//     peppolEnabled: Joi.boolean().allow(null, ""),
//     rcti: Joi.boolean().allow(null, ""),
//     creditCardPayment: Joi.boolean().allow(null, ""),
//     creditCardNumber: Joi.string().allow(null, ""),
//     partialPayment: Joi.boolean().allow(null, ""),
//     paymentTerm: Joi.number().allow(null),
//     excludedTcp: Joi.boolean().allow(null, ""),
//     notes: Joi.string().allow(null, ""),
//     isSb: Joi.boolean().allow(null),
//     paymentTime: Joi.number().allow(null),
//     createdBy: Joi.string().required(),
//     updatedBy: Joi.string().allow(null),
//     reportId: Joi.string().required(),
//   });

//   // Validate the request body
//   await schema.validateAsync(req.body);
// }

function create(req, res, next) {
  tcpService
    .create(req.body, req.auth.clientId)
    .then((tcp) => res.json(tcp))
    .catch((error) => {
      console.error("Error creating tcp:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function bulkUpdate(req, res, next) {
  try {
    // Ensure req.body is an object and iterate through its keys
    const records = Object.values(req.body);
    // console.log("Records to update:", records);

    const promises = records.flatMap((item) => {
      // Check if item is an array and process each object individually
      const itemsToProcess = Array.isArray(item) ? item : [item];

      return itemsToProcess.map(async (record) => {
        try {
          // Exclude id and createdAt fields from the record
          const { id, createdAt, ...recordToUpdate } = record;

          // Validate each record using bulkUpdateSchema
          // const reqForValidation = { body: recordToUpdate };
          // await bulkUpdateSchema(reqForValidation); // Validate the record

          // Save each record using tcpService
          return await tcpService.update(id, recordToUpdate, req.auth.clientId);
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
          message: "All records updated successfully",
          results,
        })
      )
      .catch((error) => {
        console.error("Error updating records:", error);
        next(error); // Pass the error to the global error handler
      });
  } catch (error) {
    console.error("Error processing bulk records:", error);
    next(error); // Pass the error to the global error handler
  }
}

// async function bulkUpdateSchema(req) {
//   const schema = Joi.object({
//     payerEntityName: Joi.string().allow(null),
//     payerEntityAbn: Joi.number().allow(null),
//     payerEntityAcnArbn: Joi.number().allow(null),
//     payeeEntityName: Joi.string().allow(null),
//     payeeEntityAbn: Joi.number().allow(null),
//     payeeEntityAcnArbn: Joi.number().allow(null),
//     paymentAmount: Joi.number().required(),
//     description: Joi.string().allow(null, ""),
//     supplyDate: Joi.date().allow(null, ""),
//     paymentDate: Joi.date().required(),
//     contractPoReferenceNumber: Joi.string().allow(null, ""),
//     contractPoPaymentTerms: Joi.string().allow(null, ""),
//     noticeForPaymentIssueDate: Joi.date().allow(null, ""),
//     noticeForPaymentTerms: Joi.string().allow(null, ""),
//     invoiceReferenceNumber: Joi.string().allow(null, ""),
//     invoiceIssueDate: Joi.date().allow(null, ""),
//     invoiceReceiptDate: Joi.date().allow(null, ""),
//     invoiceAmount: Joi.number().allow(null),
//     invoicePaymentTerms: Joi.string().allow(null, ""),
//     invoiceDueDate: Joi.date().allow(null, ""),
//     isTcp: Joi.boolean().allow(null, ""),
//     tcpExclusion: Joi.string().allow(null, ""),
//     peppolEnabled: Joi.boolean().allow(null, ""),
//     rcti: Joi.boolean().allow(null, ""),
//     creditCardPayment: Joi.boolean().allow(null, ""),
//     creditCardNumber: Joi.string().allow(null, ""),
//     partialPayment: Joi.boolean().allow(null, ""),
//     paymentTerm: Joi.number().allow(null),
//     excludedTcp: Joi.boolean().allow(null, ""),
//     notes: Joi.string().allow(null, ""),
//     isSb: Joi.boolean().allow(null),
//     paymentTime: Joi.number().allow(null),
//     createdBy: Joi.string().allow(null),
//     updatedBy: Joi.string().required(),
//     updatedAt: Joi.date().required(),
//     reportId: Joi.string().required(),
//   });

//   // Validate the request body
//   await schema.validateAsync(req.body);
// }

function update(req, res, next) {
  tcpService
    .update(req.params.id, req.body, req.auth.clientId)
    .then((tcp) => res.json(tcp))
    .catch(next);
}

function _delete(req, res, next) {
  tcpService
    .delete(req.params.id, req.auth.clientId)
    .then(() => res.json({ message: "Tcp deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting tcp:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
