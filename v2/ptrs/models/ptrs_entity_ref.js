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

    profileId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },

    entityCode: {
      type: DataTypes.STRING(32),
      allowNull: true,
    },

    entityName: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },

    payerEntityName: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    abn: {
      type: DataTypes.STRING(14),
      allowNull: true,
    },

    acn: {
      type: DataTypes.STRING(14),
      allowNull: true,
    },

    country: {
      type: DataTypes.STRING(2),
      allowNull: true,
    },

    source: {
      type: DataTypes.STRING(64),
      allowNull: true,
    },

    notes: {
      type: DataTypes.TEXT,
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
  };

  return sequelize.define("PtrsEntityRef", attributes, {
    tableName: "tbl_ptrs_entity_ref",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["customerId", "profileId"] },
      { fields: ["entityCode"] },
      { fields: ["abn"] },
      { fields: ["entityName"] },
    ],
  });
}
