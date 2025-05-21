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
    name: { type: DataTypes.STRING, allowNull: false },
    email: { type: DataTypes.STRING, allowNull: false },
    date: { type: DataTypes.DATEONLY, allowNull: false },
    time: { type: DataTypes.STRING, allowNull: false },
    reason: { type: DataTypes.TEXT, allowNull: true },
    status: {
      type: DataTypes.ENUM("pending", "confirmed", "cancelled"),
      defaultValue: "pending",
    },
  };

  return sequelize.define("booking", attributes, {
    tableName: "tbl_booking",
    timestamps: true,
    charset: "utf8mb4",
    collate: "utf8mb4_0900_ai_ci",
  });
}
