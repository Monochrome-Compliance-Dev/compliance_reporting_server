const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const validateRequest = require("../middleware/validate-request");
const invoiceService = require("./invoice.service");
const { generateInvoiceSchema } = require("./invoice.validator");
const {
  logCreateAudit,
  logReadAudit,
  logUpdateAudit,
  logDeleteAudit,
} = require("../audit/auditHelpers");

router.post(
  "/generate",
  authorise(),
  validateRequest(generateInvoiceSchema),
  generateInvoicesForPeriod
);
router.get("/:id", authorise(), getInvoiceById);
router.put("/:id", authorise(), updateInvoice);
router.delete("/:id", authorise(), deleteInvoice);

async function generateInvoicesForPeriod(req, res, next) {
  try {
    const reportingPeriodId = req.body.reportingPeriodId;
    const userId = req.auth.id;
    const clientId = req.auth.clientId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const result = await invoiceService.generateInvoicesForPeriod(
      reportingPeriodId,
      userId,
      clientId
    );

    await logCreateAudit({
      entity: "InvoiceBatch",
      clientId,
      userId,
      req,
      entityId: reportingPeriodId,
      reqBody: { reportingPeriodId },
      action: "Create",
      result,
      details: { invoiceCount: result.length },
      ip,
      device,
    });

    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
}

async function getInvoiceById(req, res, next) {
  try {
    const id = req.params.id;
    const userId = req.auth.id;
    const clientId = req.auth.clientId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const invoice = await invoiceService.getInvoiceById(id, clientId);
    const invoiceData = invoice?.get ? invoice.get({ plain: true }) : invoice;
    await logReadAudit({
      entity: "Invoice",
      clientId,
      userId,
      req,
      result: invoiceData,
      entityId: id,
      action: "Read",
      details: {
        totalAmount: invoiceData?.totalAmount,
        status: invoiceData?.status,
      },
      ip,
      device,
    });
    res.json(invoiceData);
  } catch (err) {
    next(err);
  }
}

async function updateInvoice(req, res, next) {
  try {
    const id = req.params.id;
    const userId = req.auth.id;
    const clientId = req.auth.clientId;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const data = req.body;

    // Fetch the existing invoice before updating
    const before = await invoiceService.getInvoiceById(id, clientId);
    const beforeData = before?.get ? before.get({ plain: true }) : before;

    const result = await invoiceService.updateInvoice(id, clientId, {
      ...data,
      updatedBy: userId,
    });
    const afterData = result?.get ? result.get({ plain: true }) : result;

    await logUpdateAudit({
      entity: "Invoice",
      clientId,
      userId,
      reqBody: data,
      req,
      action: "Update",
      before: beforeData,
      after: afterData,
      entityId: id,
      details: { updatedFields: Object.keys(data) },
      ip,
      device,
    });
    res.json(afterData);
  } catch (err) {
    next(err);
  }
}

async function deleteInvoice(req, res, next) {
  try {
    const id = req.params.id;
    const userId = req.auth.id;
    const clientId = req.auth.clientId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    // Fetch the invoice first, then delete
    const before = await invoiceService.getInvoiceById(id, clientId);
    const beforeData = before?.get ? before.get({ plain: true }) : before;

    await invoiceService.deleteInvoice(id, clientId);
    await logDeleteAudit({
      entity: "Invoice",
      clientId,
      userId,
      req,
      action: "Delete",
      before: beforeData,
      entityId: id,
      details: { deletedInvoiceId: id },
      ip,
      device,
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

module.exports = router;
