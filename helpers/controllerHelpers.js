function getRequestMeta(req) {
  return {
    customerId: req.effectiveCustomerId,
    userId: req.auth?.id || null,
    ip: req.ip,
    device: req.headers["user-agent"],
  };
}

function badRequest(res, message) {
  return res.status(400).json({ status: "error", message });
}

function notFound(res, message = "Not found") {
  return res.status(404).json({ status: "error", message });
}

function success(res, data, statusCode = 200) {
  return res.status(statusCode).json({ status: "success", data });
}

module.exports = {
  getRequestMeta,
  badRequest,
  notFound,
  success,
};
