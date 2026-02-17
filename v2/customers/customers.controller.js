const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");

const customerService = require("./customers.service");

// Helper to extract a user id from the request (auth shape can vary)
function getUserId(req) {
  return (
    (req.auth && req.auth.id) ||
    (req.user && req.user.id) ||
    (req.user && req.user.sub) ||
    null
  );
}

async function listCustomers(req, res, next) {
  try {
    const customers = await customerService.listCustomers();
    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId: "-R1o0gWoZX",
      userId,
      ip,
      device,
      action: "ListCustomersV2",
      entity: "Customer",
      details: {
        count: Array.isArray(customers) ? customers.length : undefined,
      },
    });

    return res.json(customers);
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to list customers";

    logger.logEvent("error", "Error listing v2 customers", {
      action: "ListCustomersV2",
      userId: getUserId(req),
      customerId: "-R1o0gWoZX",
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    // propagate to global handler (still return safe response)
    return res.status(status).json({ status: "error", message });
  }
}

async function getCustomersByAccess(req, res, next) {
  try {
    const userId = getUserId(req);
    if (!userId) {
      return res.status(401).json({ status: "error", message: "Unauthorised" });
    }

    const list = await customerService.getCustomersByAccess({ userId });

    // Keep response shape as a plain array to match legacy FE expectations
    return res.json(Array.isArray(list) ? list : []);
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to load customer access";

    logger.logEvent("error", "Error loading v2 customer access", {
      action: "GetCustomersByAccessV2",
      userId: getUserId(req),
      customerId: null,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    return res.status(status).json({ status: "error", message });
  }
}

async function createCustomer(req, res, next) {
  try {
    const userId = getUserId(req);
    const customer = await customerService.createCustomer({
      data: req.body,
      userId,
    });

    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId: customer.id,
      userId,
      ip,
      device,
      action: "CreateCustomerV2",
      entity: "Customer",
      entityId: customer.id,
      details: {
        businessName: customer.businessName,
        abn: customer.abn,
      },
    });

    return res.status(201).json({
      status: "success",
      data: customer,
    });
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to create customer";

    logger.logEvent("error", "Error creating v2 customer", {
      action: "CreateCustomerV2",
      userId: getUserId(req),
      customerId: null,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    return res.status(status).json({ status: "error", message });
  }
}

async function updateCustomer(req, res, next) {
  try {
    const userId = getUserId(req);
    const { id } = req.params;

    const customer = await customerService.updateCustomer({
      id,
      data: req.body,
      userId,
    });

    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId: customer.id,
      userId,
      ip,
      device,
      action: "UpdateCustomerV2",
      entity: "Customer",
      entityId: customer.id,
      details: {
        updates: Object.keys(req.body || {}),
      },
    });

    return res.json({
      status: "success",
      data: customer,
    });
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to update customer";

    logger.logEvent("error", "Error updating v2 customer", {
      action: "UpdateCustomerV2",
      userId: getUserId(req),
      customerId: req.params.id,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    return res.status(status).json({ status: "error", message });
  }
}

async function deleteCustomer(req, res, next) {
  try {
    const { id } = req.params;

    const result = await customerService.deleteCustomer({ id });

    const userId = getUserId(req);
    const ip = req.ip;
    const device = req.headers["user-agent"];

    await auditService.logEvent({
      customerId: id,
      userId,
      ip,
      device,
      action: "DeleteCustomerV2",
      entity: "Customer",
      entityId: id,
    });

    return res.json({
      status: "success",
      message: result.message,
    });
  } catch (err) {
    const status = err.status || 500;
    const message = err.message || "Failed to delete customer";

    logger.logEvent("error", "Error deleting v2 customer", {
      action: "DeleteCustomerV2",
      userId: getUserId(req),
      customerId: req.params.id,
      error: err.message,
      statusCode: status,
      timestamp: new Date().toISOString(),
    });

    return res.status(status).json({ status: "error", message });
  }
}

module.exports = {
  listCustomers,
  getCustomersByAccess,
  createCustomer,
  updateCustomer,
  deleteCustomer,
};
