const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// Governed payer/reporting identity for one transaction dataset.
function model(sequelize) {
  return sequelize.define(
    "PtrsDatasetReportingEntitySnapshot",
    {
      id: {
        type: DataTypes.STRING(10),
        defaultValue: () => getNanoid(10),
        primaryKey: true,
      },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      datasetId: { type: DataTypes.STRING(10), allowNull: false },
      entityName: { type: DataTypes.STRING(255), allowNull: false },
      abn: { type: DataTypes.TEXT, allowNull: false },
      acn: { type: DataTypes.STRING(14), allowNull: true },
      arbn: { type: DataTypes.STRING(14), allowNull: true },
      country: {
        type: DataTypes.STRING(2),
        allowNull: true,
        defaultValue: "AU",
      },
      source: { type: DataTypes.STRING(64), allowNull: true },
      meta: { type: DataTypes.JSONB, allowNull: true },
      createdBy: { type: DataTypes.STRING(10), allowNull: true },
      updatedBy: { type: DataTypes.STRING(10), allowNull: true },
      deletedAt: { type: DataTypes.DATE, allowNull: true },
    },
    {
      tableName: "tbl_ptrs_dataset_reporting_entity_snapshot",
      timestamps: true,
      paranoid: true,
      indexes: [
        {
          name: "ptrs_dataset_reporting_entity_customer_idx",
          fields: ["customerId"],
        },
        {
          name: "ptrs_dataset_reporting_entity_ptrs_idx",
          fields: ["ptrsId"],
        },
        {
          name: "ptrs_dataset_reporting_entity_dataset_ux",
          fields: ["datasetId"],
          unique: true,
        },
        {
          name: "ptrs_dataset_reporting_entity_scope_ux",
          fields: ["customerId", "ptrsId", "datasetId"],
          unique: true,
        },
        {
          name: "ptrs_dataset_reporting_entity_abn_idx",
          fields: ["abn"],
        },
      ],
    },
  );
}
