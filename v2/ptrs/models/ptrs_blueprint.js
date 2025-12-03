const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    // e.g. "ptrsCalculationBlueprint", "veolia", "cosol", etc.
    id: {
      type: DataTypes.STRING(50),
      primaryKey: true,
      allowNull: false,
    },

    label: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Optional parent blueprint id, e.g. "ptrsCalculationBlueprint"
    extends: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },

    // Full JSON blueprint payload (shape matches existing JSON files)
    json: {
      type: DataTypes.JSONB,
      allowNull: false,
    },

    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    deletedAt: {
      type: DataTypes.DATE,
      allowNull: true,
    },
  };

  const PtrsBlueprint = sequelize.define("PtrsBlueprint", attributes, {
    tableName: "tbl_ptrs_blueprint",
    timestamps: true,
    paranoid: true,
    indexes: [{ fields: ["extends"] }],
  });

  return PtrsBlueprint;
}
