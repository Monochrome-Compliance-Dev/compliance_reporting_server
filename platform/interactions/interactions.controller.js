const interactionsService = require("./interactions.service");

async function executeInteraction(req, res, next) {
  try {
    const result = await interactionsService.executeInteraction(req);
    res.json(result);
  } catch (error) {
    next(error);
  }
}

module.exports = {
  executeInteraction,
};
