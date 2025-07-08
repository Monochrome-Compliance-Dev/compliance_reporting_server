const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const AuditEvent = sequelize.define(
    "AuditEvent",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
        // Unique ID for the audit event itself
      },
      clientId: {
        type: DataTypes.STRING(10),
        allowNull: false,
        // Tenant or customer that owns the data
      },
      userId: {
        type: DataTypes.STRING(10),
        allowNull: false,
        // User who performed the action
      },
      action: {
        type: DataTypes.STRING,
        allowNull: false,
        // Description of the action, e.g. "Create", "Update", "Delete", "Get"
      },
      entity: {
        type: DataTypes.STRING,
        allowNull: false,
        // The type of entity affected, e.g. "ReportingPeriod"
      },
      entityId: {
        type: DataTypes.STRING(10),
        allowNull: true,
        // The specific ID of the entity record affected (nullable for bulk actions)
      },
      details: {
        type: DataTypes.JSONB,
        allowNull: true,
        // Additional context, such as field diffs or counts
      },
    },
    {
      tableName: "tbl_audit_events",
      timestamps: true,
    }
  );

  return AuditEvent;
};
