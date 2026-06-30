const express = require("express");

const schemaDefinitionRoutes = require("./schemaEngine/routes/schemaDefinition.routes");

const router = express.Router();

router.use("/schema-definitions", schemaDefinitionRoutes);

module.exports = router;
