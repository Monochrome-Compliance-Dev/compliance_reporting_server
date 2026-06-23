const express = require("express");
const router = express.Router();

const authorise = require("@/middleware/authorise");
const runController = require("./run.controller");

const requireDataHub = authorise({
  roles: ["Admin", "Boss", "User"],
  features: "ptrs",
});

// Run CRUD
router.get("/", requireDataHub, runController.listRuns);
router.post("/", requireDataHub, runController.createRun);

router.get("/:runId", requireDataHub, runController.getRun);
router.patch("/:runId", requireDataHub, runController.updateRun);
router.put("/:runId", requireDataHub, runController.updateRun);
router.delete("/:runId", requireDataHub, runController.deleteRun);

module.exports = router;
