const express = require("express");
const { stageRunController } = require("../controllers/stage.controller.js");

const router = express.Router();
router.post("/:runId/stage", stageRunController);
module.exports = router;
