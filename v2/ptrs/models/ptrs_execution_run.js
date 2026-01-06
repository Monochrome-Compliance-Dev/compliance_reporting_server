const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => getNanoid(10),
      primaryKey: true,
    },

    customerId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    ptrsId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    step: {
      type: DataTypes.STRING(50),
      allowNull: false,
      comment: "Execution step name, e.g. stage, rules, export",
    },

    status: {
      type: DataTypes.STRING(20),
      allowNull: false,
      comment: "pending | running | success | failed",
    },

    inputHash: {
      type: DataTypes.STRING(64),
      allowNull: false,
      comment: "Hash of inputs/config used for this execution",
    },

    rowsIn: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    rowsOut: {
      type: DataTypes.INTEGER,
      allowNull: true,
    },

    stats: {
      type: DataTypes.JSONB,
      allowNull: true,
      comment: "Step-specific execution stats",
    },

    errorMessage: {
      type: DataTypes.TEXT,
      allowNull: true,
    },

    startedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },

    finishedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsExecutionRun = sequelize.define("PtrsExecutionRun", attributes, {
    tableName: "tbl_ptrs_execution_run",
    timestamps: false,
    paranoid: false,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["customerId", "ptrsId"] },
      { fields: ["step"] },
      { fields: ["status"] },
      { fields: ["inputHash"] },
    ],
  });

  return PtrsExecutionRun;
}
