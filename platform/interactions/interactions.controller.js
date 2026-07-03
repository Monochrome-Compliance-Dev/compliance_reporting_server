const interactionsService = require("./interactions.service");

function executeInteraction(req, res, next) {
  try {
    const result = interactionsService.executeInteraction(req);
    res.json(result);
  } catch (error) {
    next(error);
  }
}

module.exports = {
  executeInteraction,
};
