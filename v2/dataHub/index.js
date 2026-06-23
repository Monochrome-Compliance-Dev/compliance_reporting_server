const express = require("express");
const router = express.Router();

router.use("/runs", require("./runs/run.routes"));
router.use("/datasets", require("./datasets/dataset.routes"));

module.exports = router;
