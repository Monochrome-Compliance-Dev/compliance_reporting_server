const express = require("express");
const authorise = require("@/middleware/authorise");
const controller = require("../controllers/schemaDefinition.controller");

const router = express.Router();

const requireSourceOnboarding = authorise({
  roles: ["Admin", "Boss", "User"],
  features: ["dataHub"],
});

router.get("/", requireSourceOnboarding, controller.listSchemaDefinitions);

router.post("/", requireSourceOnboarding, controller.createSchemaDefinition);

router.get("/:id", requireSourceOnboarding, controller.getSchemaDefinition);

router.put("/:id", requireSourceOnboarding, controller.updateSchemaDefinition);

router.post(
  "/:id/approve",
  requireSourceOnboarding,
  controller.approveSchemaDefinition,
);

router.post(
  "/:id/new-version",
  requireSourceOnboarding,
  controller.createNewSchemaDefinitionVersion,
);

router.post(
  "/:id/deprecate",
  requireSourceOnboarding,
  controller.deprecateSchemaDefinition,
);

module.exports = router;
