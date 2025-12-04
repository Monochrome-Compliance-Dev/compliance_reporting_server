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
      // fully mapped + joined row payload
      type: DataTypes.JSONB,
      allowNull: false,
    },
    meta: {
      // optional: mapping version, timestamps, etc.
      type: DataTypes.JSONB,
      allowNull: true,
    },
  };

  const PtrsMappedRow = sequelize.define("PtrsMappedRow", attributes, {
    tableName: "tbl_ptrs_mapped_row",
    timestamps: true,
    paranoid: false,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["ptrsId"] },
      { fields: ["customerId", "ptrsId", "rowNo"], unique: true },
    ],
  });

  return PtrsMappedRow;
}
