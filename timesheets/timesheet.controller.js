const auditService = require("../audit/audit.service");
const { logger } = require("../helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});
const timesheetService = require("./timesheet.service");
const {
  timesheetCreateSchema,
  timesheetUpdateSchema,
  timesheetPatchSchema,
  timesheetRowCreateSchema,
  timesheetRowUpdateSchema,
  timesheetRowPatchSchema,
} = require("./timesheet.validator");

router.get("/", requirePulse, getAll);
router.get("/utilisation", requirePulse, getUtilisation);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(timesheetCreateSchema), create);
router.put(
  "/:id",
  requirePulse,
  validateRequest(timesheetUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(timesheetPatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);
// Timesheet rows
router.get("/:id/rows", requirePulse, getRows);
router.post(
  "/:id/rows",
  requirePulse,
  validateRequest(timesheetRowCreateSchema),
  createRow
);
router.put(
  "/rows/:rowId",
  requirePulse,
  validateRequest(timesheetRowUpdateSchema),
  updateRow
);
router.patch(
  "/rows/:rowId",
  requirePulse,
  validateRequest(timesheetRowPatchSchema),
  patchRow
);
router.delete("/rows/:rowId", requirePulse, deleteRow);

module.exports = router;

async function getAll(req, res, next) {
  try {
    const customerId = req.effectiveCustomerId;
    const userId = req.auth?.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const timesheets = await timesheetService.getAll({
      customerId,
      order: [["createdAt", "DESC"]],
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetAllTimesheets",
      entity: "Timesheet",
      details: {
        count: Array.isArray(timesheets) ? timesheets.length : undefined,
      },
    });
    res.json({ status: "success", data: timesheets });
  } catch (error) {
    logger.logEvent("error", "Error fetching all timesheets", {
      action: "GetAllTimesheets",
      userId: req.auth?.id,
      customerId: req.effectiveCustomerId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getById(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const timesheet = await timesheetService.getById({ id, customerId });
    if (timesheet) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetTimesheetById",
        entity: "Timesheet",
        entityId: id,
      });
      res.json({ status: "success", data: timesheet });
    } else {
      res.status(404).json({ status: "error", message: "Timesheet not found" });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching timesheet by ID", {
      action: "GetTimesheetById",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function create(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const timesheet = await timesheetService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateTimesheet",
      entity: "Timesheet",
      entityId: timesheet.id,
      details: { status: timesheet.status },
    });
    res.status(201).json({ status: "success", data: timesheet });
  } catch (error) {
    logger.logEvent("error", "Error creating timesheet", {
      action: "CreateTimesheet",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function update(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const timesheet = await timesheetService.update({
      id,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateTimesheet",
      entity: "Timesheet",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: timesheet });
  } catch (error) {
    logger.logEvent("error", "Error updating timesheet", {
      action: "UpdateTimesheet",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function patch(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    const timesheet = await timesheetService.patch({
      id,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchTimesheet",
      entity: "Timesheet",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: timesheet });
  } catch (error) {
    logger.logEvent("error", "Error patching timesheet", {
      action: "PatchTimesheet",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function _delete(req, res, next) {
  const id = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    if (!customerId)
      return res.status(400).json({ message: "Customer ID missing" });
    await timesheetService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteTimesheet",
      entity: "Timesheet",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting timesheet", {
      action: "DeleteTimesheet",
      id,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

// --- Timesheet rows handlers ---
async function getRows(req, res, next) {
  const timesheetId = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const rows = await timesheetService.listByTimesheet({
      timesheetId,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetTimesheetRows",
      entity: "TimesheetRow",
      details: {
        timesheetId,
        count: Array.isArray(rows) ? rows.length : undefined,
      },
    });
    res.json({ status: "success", data: rows });
  } catch (error) {
    logger.logEvent("error", "Error fetching timesheet rows", {
      action: "GetTimesheetRows",
      timesheetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function createRow(req, res, next) {
  const timesheetId = req.params.id;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await timesheetService.createRow({
      timesheetId,
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateTimesheetRow",
      entity: "TimesheetRow",
      entityId: row.id,
      details: { timesheetId },
    });
    res.status(201).json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error creating timesheet row", {
      action: "CreateTimesheetRow",
      timesheetId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function updateRow(req, res, next) {
  const rowId = req.params.rowId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await timesheetService.updateRow({
      id: rowId,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "UpdateTimesheetRow",
      entity: "TimesheetRow",
      entityId: rowId,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error updating timesheet row", {
      action: "UpdateTimesheetRow",
      rowId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function patchRow(req, res, next) {
  const rowId = req.params.rowId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const row = await timesheetService.patchRow({
      id: rowId,
      data: req.body,
      customerId,
      userId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PatchTimesheetRow",
      entity: "TimesheetRow",
      entityId: rowId,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: row });
  } catch (error) {
    logger.logEvent("error", "Error patching timesheet row", {
      action: "PatchTimesheetRow",
      rowId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function deleteRow(req, res, next) {
  const rowId = req.params.rowId;
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    await timesheetService.deleteRow({ id: rowId, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteTimesheetRow",
      entity: "TimesheetRow",
      entityId: rowId,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting timesheet row", {
      action: "DeleteTimesheetRow",
      rowId,
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}

async function getUtilisation(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  try {
    const { from, to, includeNonBillable } = req.query || {};
    const params = {
      customerId,
      // Pass raw strings to service; service can parse/validate
      from: typeof from === "string" ? from : undefined,
      to: typeof to === "string" ? to : undefined,
      includeNonBillable:
        includeNonBillable === "true" || includeNonBillable === true,
    };
    const rows = await timesheetService.utilisation(params);
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "GetTimesheetUtilisation",
      entity: "TimesheetUtilisation",
      details: {
        from: params.from,
        to: params.to,
        includeNonBillable: params.includeNonBillable,
        count: Array.isArray(rows) ? rows.length : undefined,
      },
    });
    res.json({ status: "success", data: rows });
  } catch (error) {
    logger.logEvent("error", "Error fetching timesheet utilisation", {
      action: "GetTimesheetUtilisation",
      customerId,
      userId,
      error: error.message,
      statusCode: error.statusCode || 500,
      timestamp: new Date().toISOString(),
    });
    return next(error);
  }
}
