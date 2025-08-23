const express = require("express");
const router = express.Router();
const partnerService = require("./partner.service");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const { partnerSchema } = require("./partner.validator");

const {
  logCreateAudit,
  logReadAudit,
  logUpdateAudit,
  logDeleteAudit,
} = require("../audit/auditHelpers");

// --- Partner Routes ---
router.post("/", authorise(), validateRequest(partnerSchema), createPartner);
router.get("/", authorise(), getPartners);
router.get("/:id", authorise(), getPartnerById);
router.put("/:id", authorise(), validateRequest(partnerSchema), updatePartner);
router.delete("/:id", authorise(), deletePartner);

// --- Handlers ---
async function createPartner(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const partner = await partnerService.createPartner(userId, req.body);
    await logCreateAudit({
      entity: "Partner",
      customerId: null,
      userId,
      req,
      entityId: partner.id,
      reqBody: req.body,
      result: partner,
      action: "Create",
      ip,
      device,
    });
    res.status(201).json(partner.get ? partner.get({ plain: true }) : partner);
  } catch (err) {
    next(err);
  }
}

async function getPartners(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const partners = await partnerService.getPartners();
    await logReadAudit({
      entity: "Partner",
      customerId: null,
      userId,
      req,
      result: partners,
      action: "Read",
      details: { count: partners.length },
      ip,
      device,
    });
    res.json(partners.map((p) => (p.get ? p.get({ plain: true }) : p)));
  } catch (err) {
    next(err);
  }
}

async function updatePartner(req, res, next) {
  try {
    const id = req.params.id;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const before = await partnerService.getPartnerById(id);
    if (!before) {
      return res.status(404).json({ message: "Partner not found" });
    }
    const beforeData = before.get({ plain: true });
    const after = await partnerService.updatePartner(id, req.body);
    const afterData = after ? after.get({ plain: true }) : null;
    await logUpdateAudit({
      entity: "Partner",
      customerId: null,
      userId,
      req,
      reqBody: req.body,
      before: beforeData,
      after: afterData,
      action: "Update",
      entityId: id,
      ip,
      device,
      details: { name: afterData?.name, contactEmail: afterData?.contactEmail },
    });
    res.json(afterData);
  } catch (err) {
    next(err);
  }
}

async function deletePartner(req, res, next) {
  try {
    const id = req.params.id;
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const before = await partnerService.getPartnerById(id);
    if (!before) {
      return res.status(404).json({ message: "Partner not found" });
    }
    const beforeData = before.get({ plain: true });
    await partnerService.deletePartner(id);
    await logDeleteAudit({
      entity: "Partner",
      customerId: null,
      userId,
      req,
      action: "Delete",
      before: beforeData,
      entityId: id,
      ip,
      device,
      details: {
        name: beforeData?.name,
        contactEmail: beforeData?.contactEmail,
      },
    });
    res.status(204).send();
  } catch (err) {
    next(err);
  }
}

module.exports = router;

// --- New Handler: getPartnerById ---
async function getPartnerById(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;
    const partner = await partnerService.getPartnerById(id);
    if (!partner) {
      return res.status(404).json({ message: "Partner not found" });
    }
    const partnerData = partner.get({ plain: true });
    await logReadAudit({
      entity: "Partner",
      customerId: null,
      userId,
      req,
      result: partnerData,
      entityId: id,
      action: "Read",
      ip,
      device,
      details: {
        name: partnerData?.name,
        contactEmail: partnerData?.contactEmail,
      },
    });
    res.json(partnerData);
  } catch (err) {
    next(err);
  }
}
