// middleware/injectClientId.js

module.exports = (req, res, next) => {
  const clientId = req.auth?.clientId;
  if (!clientId) {
    console.error("Middleware error: clientId missing in req.auth");
    return res.status(400).json({ error: "clientId missing" });
  }

  req.body.clientId = clientId.replace(/[^a-zA-Z0-9-_]/g, "");
  next();
};
