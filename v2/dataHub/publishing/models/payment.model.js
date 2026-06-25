const { DataTypes } = require("sequelize");
const { getNanoid } = require("@/helpers/nanoid_helper");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      primaryKey: true,
      defaultValue: () => getNanoid(10),
    },

    customerId: {
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

    sourceRowNumber: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },

    paymentDate: {
      type: DataTypes.DATE,
      allowNull: true,
    },

    paymentAmount: {
      type: DataTypes.DECIMAL(18, 2),
      allowNull: true,
    },

    currency: {
      type: DataTypes.STRING(10),
      allowNull: true,
    },

    payeeName: {
      type: DataTypes.STRING(500),
      allowNull: true,
    },

    payeeAbn: {
      type: DataTypes.STRING(20),
      allowNull: true,
    },

    payerName: {
      type: DataTypes.STRING(500),
      allowNull: true,
    },

    payerAbn: {
      type: DataTypes.STRING(20),
      allowNull: true,
    },

    invoiceReference: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    documentType: {
      type: DataTypes.STRING(100),
      allowNull: true,
    },

    paymentTerms: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    purchasingDocument: {
      type: DataTypes.STRING(255),
      allowNull: true,
    },

    rawRecord: {
      type: DataTypes.JSONB,
      allowNull: false,
      defaultValue: {},
    },

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

  const DataHubPayment = sequelize.define("DataHubPayment", attributes, {
    tableName: "tbl_data_hub_payment",
    timestamps: true,
    paranoid: true,
    indexes: [
      { fields: ["customerId"] },
      { fields: ["profileId"] },
      { fields: ["datasetId"] },
      {
        name: "ix_dh_payment_scope",
        fields: ["customerId", "profileId", "datasetId"],
      },
      {
        name: "ix_dh_payment_date",
        fields: ["paymentDate"],
      },
      {
        name: "ix_dh_payment_payee_abn",
        fields: ["payeeAbn"],
      },
      {
        name: "ix_dh_payment_invoice_reference",
        fields: ["invoiceReference"],
      },
    ],
  });

  return DataHubPayment;
}
