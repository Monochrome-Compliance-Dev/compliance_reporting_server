const { DataTypes } = require("sequelize");

const isTest = process.env.NODE_ENV === "test";

// Fully replace nanoid with a static fallback for tests
const _nanoid = isTest
  ? () => "test_" + Math.random().toString(36).substring(2, 10)
  : (...args) => {
      throw new Error(
        "nanoid not available at model runtime — load it before calling"
      );
    };

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => _nanoid(10),
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

  return sequelize.define("entity", attributes, {
    tableName: "tbl_entity",
    timestamps: true,
    charset: "utf8mb4",
    collate: "utf8mb4_0900_ai_ci",
  });
}
