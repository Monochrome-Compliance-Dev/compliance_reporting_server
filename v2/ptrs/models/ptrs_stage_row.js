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
    datasetId: {
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
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    updatedBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
  };

  const PtrsStageRow = sequelize.define("PtrsStageRow", attributes, {
    tableName: "tbl_ptrs_stage_row",
    timestamps: true,
    paranoid: true,
    indexes: [
      { name: "ptrs_stage_row_customer_id_idx", fields: ["customerId"] },
      { name: "ptrs_stage_row_ptrs_id_idx", fields: ["ptrsId"] },
      { name: "ptrs_stage_row_profile_id_idx", fields: ["profileId"] },
      { name: "ptrs_stage_row_dataset_id_idx", fields: ["datasetId"] },
      {
        name: "ptrs_stage_row_customer_ptrs_idx",
        fields: ["customerId", "ptrsId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_idx",
        fields: ["customerId", "ptrsId", "profileId"],
      },
      {
        name: "ptrs_stage_row_ptrs_dataset_idx",
        fields: ["ptrsId", "datasetId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_dataset_idx",
        fields: ["customerId", "ptrsId", "datasetId"],
      },
      {
        name: "ptrs_stage_row_customer_ptrs_profile_dataset_rowno_idx",
        fields: ["customerId", "ptrsId", "profileId", "datasetId", "rowNo"],
      },
    ],
  });

  return PtrsStageRow;
}
