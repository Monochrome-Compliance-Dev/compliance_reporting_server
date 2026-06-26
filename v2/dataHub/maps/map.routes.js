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

// Find compatible maps for this dataset
router.get(
  "/compatible-maps",
  requireDataHub,
  mapController.listCompatibleMaps,
);

// Import a compatible map into this dataset
router.post("/:id/map/import", requireDataHub, mapController.importDatasetMap);

module.exports = router;
