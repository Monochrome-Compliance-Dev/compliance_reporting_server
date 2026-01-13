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
    rowNo: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    data: {
      type: DataTypes.JSONB,
      allowNull: false,
    },
    errors: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
    },
  };

  const PtrsStageRow = sequelize.define("PtrsStageRow", attributes, {
    tableName: "tbl_ptrs_stage_row",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["customerId", "ptrsId"] },
    ],
  });

  return PtrsStageRow;
}
