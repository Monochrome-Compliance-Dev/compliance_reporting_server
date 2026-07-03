const express = require("express");
const interactionsController = require("./interactions.controller");

const router = express.Router();

router.post("/", interactionsController.executeInteraction);

module.exports = router;
