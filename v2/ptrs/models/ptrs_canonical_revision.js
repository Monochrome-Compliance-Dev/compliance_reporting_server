const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const PtrsCanonicalRevision = sequelize.define(
    "PtrsCanonicalRevision",
    {
      id: {
        type: DataTypes.STRING(10),
        defaultValue: () => getNanoid(10),
        primaryKey: true,
      },
      customerId: { type: DataTypes.STRING(10), allowNull: false },
      ptrsId: { type: DataTypes.STRING(10), allowNull: false },
      datasetId: { type: DataTypes.STRING(10), allowNull: false },
      adapterType: { type: DataTypes.STRING(50), allowNull: false },
      adapterVersion: { type: DataTypes.STRING(30), allowNull: true },
      sourceGroupScope: { type: DataTypes.STRING(100), allowNull: true },
      canonicalVersion: {
        type: DataTypes.STRING(30),
        allowNull: false,
      },
      semanticKind: {
        type: DataTypes.STRING(30),
        allowNull: false,
        validate: { isIn: [["accounting_event", "direct_payment"]] },
      },
      sourceSignature: { type: DataTypes.STRING(64), allowNull: false },
      mappingSignature: { type: DataTypes.STRING(64), allowNull: false },
      enrichmentSignature: { type: DataTypes.STRING(64), allowNull: false },
      materialSignature: { type: DataTypes.STRING(64), allowNull: false },
      inputSnapshot: { type: DataTypes.JSONB, allowNull: false },
      status: {
        type: DataTypes.STRING(20),
        allowNull: false,
        validate: { isIn: [["building", "succeeded", "failed"]] },
      },
      rowCount: { type: DataTypes.INTEGER, allowNull: true },
      completedAt: { type: DataTypes.DATE, allowNull: true },
      failure: { type: DataTypes.JSONB, allowNull: true },
      createdBy: { type: DataTypes.STRING(10), allowNull: true },
    },
    {
      tableName: "tbl_ptrs_canonical_revision",
      timestamps: true,
      updatedAt: false,
      paranoid: false,
      indexes: [
        {
          name: "ptrs_canonical_revision_customer_idx",
          fields: ["customerId"],
        },
        {
          name: "ptrs_canonical_revision_ptrs_idx",
          fields: ["ptrsId"],
        },
        {
          name: "ptrs_canonical_revision_dataset_idx",
          fields: ["datasetId"],
        },
        {
          name: "ptrs_canonical_revision_material_idx",
          fields: ["materialSignature"],
        },
        {
          name: "ptrs_canonical_revision_scope_idx",
          fields: ["customerId", "ptrsId", "datasetId", "status"],
        },
      ],
      hooks: {
        beforeUpdate(instance) {
          if (instance.previous("status") === "succeeded") {
            throw new Error("Successful canonical revisions are immutable");
          }
        },
      },
    },
  );

  return PtrsCanonicalRevision;
}
