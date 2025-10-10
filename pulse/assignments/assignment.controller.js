const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const express = require("express");
const router = express.Router();
const validateRequest = require("@/middleware/validate-request");
const authorise = require("@/middleware/authorise");
const requirePulse = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "pulse",
});
const assignmentService = require("./assignment.service");
const {
  assignmentCreateSchema,
  assignmentUpdateSchema,
  assignmentPatchSchema,
} = require("./assignment.validator");

router.get("/", requirePulse, getAll);
router.get("/:id", requirePulse, getById);
router.post("/", requirePulse, validateRequest(assignmentCreateSchema), create);
router.put(
  "/:id",
  requirePulse,
  validateRequest(assignmentUpdateSchema),
  update
);
router.patch(
  "/:id",
  requirePulse,
  validateRequest(assignmentPatchSchema),
  patch
);
router.delete("/:id", requirePulse, _delete);

module.exports = router;

async function getAll(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const { budgetLineId } = req.query;
  try {
    const assignments = await assignmentService.getAll({
      customerId,
      budgetLineId,
      order: [["createdAt", "DESC"]],
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: budgetLineId ? "GetAssignmentsByBudgetLine" : "GetAllAssignments",
      entity: "Assignment",
      details: {
        budgetLineId: budgetLineId || undefined,
        count: Array.isArray(assignments) ? assignments.length : undefined,
      },
    });
    res.json({ status: "success", data: assignments });
  } catch (error) {
    logger.logEvent("error", "Error fetching assignments", {
      action: "GetAssignments",
      userId: req.auth?.id,
      customerId,
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
    const assignment = await assignmentService.getById({ id, customerId });
    if (assignment) {
      await auditService.logEvent({
        customerId,
        userId,
        ip,
        device,
        action: "GetAssignmentById",
        entity: "Assignment",
        entityId: id,
      });
      res.json({ status: "success", data: assignment });
    } else {
      res.status(404).json({
        status: "not_found",
        reason: "assignment_not_found",
        message: "Assignment not found",
      });
    }
  } catch (error) {
    logger.logEvent("error", "Error fetching assignment by ID", {
      action: "GetAssignmentById",
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
    const assignment = await assignmentService.create({
      data: req.body,
      customerId,
    });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "CreateAssignment",
      entity: "Assignment",
      entityId: assignment.id,
    });
    res.status(201).json({ status: "success", data: assignment });
  } catch (error) {
    logger.logEvent("error", "Error creating assignment", {
      action: "CreateAssignment",
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
    const assignment = await assignmentService.update({
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
      action: "UpdateAssignment",
      entity: "Assignment",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: assignment });
  } catch (error) {
    logger.logEvent("error", "Error updating assignment", {
      action: "UpdateAssignment",
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
    const assignment = await assignmentService.patch({
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
      action: "PatchAssignment",
      entity: "Assignment",
      entityId: id,
      details: { updates: Object.keys(req.body) },
    });
    res.json({ status: "success", data: assignment });
  } catch (error) {
    logger.logEvent("error", "Error patching assignment", {
      action: "PatchAssignment",
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
    await assignmentService.delete({ id, customerId, userId });
    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "DeleteAssignment",
      entity: "Assignment",
      entityId: id,
    });
    res.status(204).send();
  } catch (error) {
    logger.logEvent("error", "Error deleting assignment", {
      action: "DeleteAssignment",
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
