const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    eventId: {
      type: DataTypes.STRING(64),
      allowNull: false,
      unique: true,
    },
    type: {
      type: DataTypes.STRING(64),
      allowNull: false,
    },
    processedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
  };

  const WebhookEvent = sequelize.define("webhook_event", attributes, {
    tableName: "tbl_webhook_event",
    timestamps: true,
    indexes: [
      {
        unique: true,
        fields: ["eventId"],
      },
    ],
  });

  return WebhookEvent;
}
