const express = require("express");

const authorise = require("@/middleware/authorise");
const publishingController = require("./publishing.controller");

const router = express.Router();

const requireDataHub = authorise({
  roles: ["Admin", "Boss", "User"],
  features: ["dataHub", "ptrs"],
});

router.patch(
  "/:id/publish",
  requireDataHub,
  publishingController.publishDataset,
);

module.exports = router;
