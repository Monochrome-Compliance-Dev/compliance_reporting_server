const { auditService } = require("../audit/audit.service");
if (process.env.NODE_ENV !== "test") {
  import("nanoid").then((mod) => {
    nanoid = mod.nanoid;
  });
}

async function logAuditChanges(
  clientId,
  tcpId,
  oldRecord,
  newValues,
  userId,
  step = null,
  action = "update",
  transaction = null
) {
  const now = new Date();
  const changes = [];

  for (const [field, newValue] of Object.entries(newValues)) {
    if (field === "updatedAt") continue;

    const oldValue = oldRecord ? oldRecord[field] : undefined;
    if (oldValue !== newValue) {
      changes.push(
        auditService.create(
          clientId,
          {
            id: nanoid(10),
            tcpId,
            fieldName: field,
            oldValue,
            newValue,
            step,
            user_id: userId,
            createdAt: now,
            action,
          },
          { transaction }
        )
      );
    }
  }

  await Promise.all(changes);
}

module.exports = {
  logAuditChanges,
};
