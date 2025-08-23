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
router.post(
  "/generate/partner",
  authorise(),
  validateRequest(generateInvoiceSchema),
  generatePartnerInvoicesForPeriod
);
router.get("/", authorise(), getInvoicesByScope);
router.get("/:id", authorise(), getInvoiceById);
router.put("/:id", authorise(), updateInvoice);
router.delete("/:id", authorise(), deleteInvoice);

async function getInvoicesByScope(req, res, next) {
  try {
    const userId = req.auth.id;
    const customerId = req.auth.customerId;
    const partnerId = req.query.partnerId || null;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const result = await invoiceService.getInvoicesByScope({
      customerId,
      partnerId,
    });

    await logReadAudit({
      entity: "InvoiceBatch",
      userId,
      customerId,
      req,
      result,
      entityId: partnerId || customerId,
      action: "Read",
      details: { filter: { customerId, partnerId } },
      ip,
      device,
    });

    res.json(result);
  } catch (err) {
    next(err);
  }
}

async function generateInvoicesForPeriod(req, res, next) {
  try {
    const reportingPeriodId = req.body.reportingPeriodId;
    const userId = req.auth.id;
    const customerId = req.auth.customerId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const result = await invoiceService.generateInvoicesForPeriod(
      reportingPeriodId,
      userId,
      customerId
    );

    const ids = result.map((r) => r.id);

    await logCreateAudit({
      entity: "InvoiceBatch",
      customerId,
      userId,
      req,
      entityId: reportingPeriodId,
      reqBody: { reportingPeriodId },
      action: "Create",
      result,
      details: { invoiceCount: result.length, invoiceIds: ids },
      ip,
      device,
    });

    res.status(201).json(result);
  } catch (err) {
    next(err);
  }
}

async function generatePartnerInvoicesForPeriod(req, res, next) {
  try {
    const reportingPeriodId = req.body.reportingPeriodId;
    const userId = req.auth.id;
    const customerId = req.auth.customerId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const result = await invoiceService.generatePartnerInvoicesForPeriod(
      reportingPeriodId,
      userId,
      customerId
    );

    await logCreateAudit({
      entity: "PartnerInvoiceBatch",
      customerId: null,
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
    const customerId = req.auth.customerId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const invoice = await invoiceService.getInvoiceById(id, customerId);
    const invoiceData = invoice?.get ? invoice.get({ plain: true }) : invoice;
    await logReadAudit({
      entity: "Invoice",
      customerId,
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
    const customerId = req.auth.customerId;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const data = req.body;

    // Fetch the existing invoice before updating
    const before = await invoiceService.getInvoiceById(id, customerId);
    // console.log("before: ", before);
    const beforeData = before?.get ? before.get({ plain: true }) : before;
    // console.log("beforeData: ", beforeData);

    const result = await invoiceService.updateInvoice(id, customerId, {
      ...data,
      updatedBy: userId,
    });
    // console.log("result: ", result);
    const afterData = result?.get ? result.get({ plain: true }) : result;
    // console.log("afterData: ", afterData);

    await logUpdateAudit({
      entity: "Invoice",
      customerId,
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
    const customerId = req.auth.customerId;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    // Fetch the invoice first, then delete
    const before = await invoiceService.getInvoiceById(id, customerId);
    const beforeData = before?.get ? before.get({ plain: true }) : before;

    await invoiceService.deleteInvoice(id, customerId);
    await logDeleteAudit({
      entity: "Invoice",
      customerId,
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
