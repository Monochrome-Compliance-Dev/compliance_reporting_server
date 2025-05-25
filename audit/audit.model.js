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

    // Audit fields
    fieldName: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },
    oldValue: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    newValue: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    action: {
      type: DataTypes.ENUM("create", "update", "delete"),
      allowNull: false,
      index: true,
    },
    step: {
      type: DataTypes.STRING(20),
      allowNull: true,
    },

    // Metadata fields
    createdAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
  };

  return sequelize.define("audit", attributes, {
    tableName: "tbl_tcp_audit",
    timestamps: false,
    charset: "utf8mb4",
    collate: "utf8mb4_0900_ai_ci",
  });
}
