const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

// 1:1 snapshot of the reporting entity used for a specific PTRS run.
// This is the canonical source for entity details in Metrics / Report / Board Pack.
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

    // Optional link back to the profile that produced/seeded this run
    profileId: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    entityName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    // Mandatory each time: the report submission identity
    abn: {
      type: DataTypes.STRING(14),
      allowNull: false,
    },

    acn: {
      type: DataTypes.STRING(14),
      allowNull: true,
    },

    arbn: {
      type: DataTypes.STRING(14),
      allowNull: true,
    },

    country: {
      type: DataTypes.STRING(2),
      allowNull: true,
      defaultValue: "AU",
    },

    // manual | from_profile | from_entity_ref | imported | derived
    source: {
      type: DataTypes.STRING(64),
      allowNull: true,
    },

    // Extra fields we don’t want to columnise yet (industry codes, future declarations, etc.)
    meta: {
      type: DataTypes.JSONB,
      allowNull: true,
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

  return sequelize.define("PtrsReportingEntitySnapshot", attributes, {
    tableName: "tbl_ptrs_reporting_entity_snapshot",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      // enforce 1:1 with ptrs
      { fields: ["ptrsId"], unique: true },
      { fields: ["customerId", "ptrsId"], unique: true },
      { fields: ["profileId"] },
      { fields: ["abn"] },
      { fields: ["entityName"] },
    ],
  });
}
