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

    // Entity Details
    entityName: { type: DataTypes.STRING, allowNull: false },
    entityABN: { type: DataTypes.STRING, allowNull: true },
    startEntity: { type: DataTypes.STRING, allowNull: true },
    section7: { type: DataTypes.STRING, allowNull: true },
    cce: { type: DataTypes.STRING, allowNull: true },
    charity: { type: DataTypes.STRING, allowNull: true },
    connectionToAustralia: { type: DataTypes.STRING, allowNull: true },
    revenue: { type: DataTypes.STRING, allowNull: true },
    controlled: { type: DataTypes.STRING, allowNull: true },
    stoppedReason: { type: DataTypes.STRING, allowNull: true },
    completed: { type: DataTypes.BOOLEAN, defaultValue: false },
    timestamp: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },

    // Metadata
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  return sequelize.define("entity", attributes, { tableName: "tbl_entity" });
}
