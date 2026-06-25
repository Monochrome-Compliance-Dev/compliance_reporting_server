const express = require("express");

const authorise = require("@/middleware/authorise");
const mapController = require("./map.controller");

const router = express.Router();

const requireDataHub = authorise({
  roles: ["Admin", "Boss", "User"],
  features: ["dataHub", "ptrs"],
});

router.get("/:id/map", requireDataHub, mapController.getDatasetMap);

router.patch("/:id/map", requireDataHub, mapController.saveDatasetMap);

module.exports = router;
