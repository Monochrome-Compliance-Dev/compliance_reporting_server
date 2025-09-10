// maximiser.controller.js
const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const { logger } = require("../helpers/logger");
const { listTeams, compareTeams } = require("./maximiser.service");

// GET /pulse/maximiser/teams
router.get("/teams", authorise(), async (req, res, next) => {
  const customerId = req.auth?.customerId;
  try {
    logger.logEvent("info", "Pulse: maximiser listTeams invoked", {
      action: "PulseMaximiserListTeams",
      customerId,
      path: req.originalUrl,
    });
    const rows = await listTeams({ customerId });
    logger.logEvent("info", "Pulse: maximiser listTeams success", {
      action: "PulseMaximiserListTeams",
      customerId,
      count: Array.isArray(rows) ? rows.length : 0,
    });
    return res.json(rows);
  } catch (err) {
    logger.logEvent("error", "Pulse: maximiser listTeams failed", {
      action: "PulseMaximiserListTeams",
      customerId,
      error: err?.message,
    });
    next(err);
  }
});

// GET /pulse/maximiser/compare?teamIds=A,B&from=YYYY-MM-DD&to=YYYY-MM-DD&includeNonBillable=true
router.get("/compare", authorise(), async (req, res, next) => {
  const customerId = req.auth?.customerId;
  try {
    const { teamIds = "", from, to, includeNonBillable } = req.query || {};
    const ids = String(teamIds || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);

    logger.logEvent("info", "Pulse: maximiser compare invoked", {
      action: "PulseMaximiserCompare",
      customerId,
      teamIds: ids,
      from,
      to,
      includeNonBillable,
      path: req.originalUrl,
    });

    if (ids.length < 2) {
      return res
        .status(400)
        .json({
          error: "Select at least two teams to compare (teamIds=A,B,...)",
        });
    }

    const payload = await compareTeams({
      customerId,
      teamIds: ids,
      from: from || undefined,
      to: to || undefined,
      includeNonBillable:
        includeNonBillable === undefined
          ? true
          : String(includeNonBillable).toLowerCase() !== "false",
    });

    logger.logEvent("info", "Pulse: maximiser compare success", {
      action: "PulseMaximiserCompare",
      customerId,
      teams: Array.isArray(payload?.teams) ? payload.teams.length : 0,
    });

    return res.json(payload);
  } catch (err) {
    logger.logEvent("error", "Pulse: maximiser compare failed", {
      action: "PulseMaximiserCompare",
      customerId,
      error: err?.message,
    });
    next(err);
  }
});

module.exports = router;
