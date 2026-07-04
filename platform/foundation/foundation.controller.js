const foundationService = require("./foundation.service");

async function executeFoundation(req, res, next) {
  try {
    const result = await foundationService.executeFoundation(req);
    res.json(result);
  } catch (error) {
    next(error);
  }
}

module.exports = {
  executeFoundation,
};
