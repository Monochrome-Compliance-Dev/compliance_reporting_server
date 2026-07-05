const crypto = require("crypto");

const auditService = require("@/platform/audit/audit.service");
const identityService = require("@/platform/identity/identity.service");
const securityService = require("@/platform/security/security.service");

async function executeFoundation(req) {
  const executionContext = identityService.normaliseExecutionContext(req);

  const result = {
    success: true,
    foundationId: crypto.randomUUID(),
    capability: "foundation",
    message: "Platform foundation executed successfully.",
    actor: {
      id: executionContext.actorId,
      role: executionContext.role,
      customerId: executionContext.customerId,
    },
  };

  let securityObservation;

  try {
    securityObservation = securityService.enforceFoundationCommand({
      foundationId: result.foundationId,
      actor: result.actor,
      request: req,
    });
  } catch (error) {
    if (!error.securityObservation) {
      throw error;
    }

    await auditService.recordFoundationAudit({
      foundationId: result.foundationId,
      capability: result.capability,
      outcome: "denied",
      actor: result.actor,
      request: req,
      securityObservation: error.securityObservation,
      error,
    });

    throw error;
  }

  await auditService.recordFoundationAudit({
    foundationId: result.foundationId,
    capability: result.capability,
    outcome: "success",
    actor: result.actor,
    request: req,
    securityObservation,
  });

  return result;
}

module.exports = {
  executeFoundation,
};
