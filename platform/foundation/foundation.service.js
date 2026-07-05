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

  const securityObservation = securityService.observeFoundationCommand({
    foundationId: result.foundationId,
    actor: result.actor,
    request: req,
  });

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
