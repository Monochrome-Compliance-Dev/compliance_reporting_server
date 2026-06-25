const express = require("express");
const router = express.Router();

router.use("/datasets", require("./datasets/dataset.routes"));
router.use("/maps", require("./maps/map.routes"));
router.use("/publishing", require("./publishing/publishing.routes"));

module.exports = router;
