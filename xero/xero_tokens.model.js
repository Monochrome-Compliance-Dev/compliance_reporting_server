const { DataTypes } = require("sequelize");

// This model definition function expects a sequelize instance and returns the model
function defineXeroTokenModel(sequelize) {
  const XeroToken = sequelize.define(
    "XeroToken",
    {
      id: {
        type: DataTypes.UUID,
        defaultValue: DataTypes.UUIDV4,
        primaryKey: true,
      },
      access_token: {
        type: DataTypes.TEXT,
        allowNull: false,
      },
      refresh_token: {
        type: DataTypes.TEXT,
        allowNull: false,
      },
      expires: {
        type: DataTypes.DATE,
        allowNull: false,
      },
      created: {
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: DataTypes.NOW,
      },
      createdByIp: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      revoked: {
        type: DataTypes.DATE,
        allowNull: true,
      },
      revokedByIp: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      replacedByToken: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      clientId: {
        type: DataTypes.STRING(10),
        allowNull: false,
        references: {
          model: "tbl_client",
          key: "id",
        },
      },
      tenantId: {
        type: DataTypes.STRING,
        allowNull: false,
      },
    },
    {
      tableName: "xero_tokens",
      timestamps: false,
      getterMethods: {
        isExpired() {
          return Date.now() >= new Date(this.expires).getTime();
        },
        isActive() {
          return !this.revoked && !this.isExpired;
        },
      },
    }
  );
  return XeroToken;
}

module.exports = defineXeroTokenModel;
