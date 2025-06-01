const { DataTypes, Model } = require("sequelize");
const sequelize = require("../config/database");

class XeroToken extends Model {
  get isExpired() {
    return Date.now() >= this.expires;
  }

  get isActive() {
    return !this.revoked && !this.isExpired;
  }
}

XeroToken.init(
  {
    access_token: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    refresh_token: {
      type: DataTypes.STRING,
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
      type: DataTypes.STRING,
      allowNull: true,
    },
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: false,
      references: {
        model: "clients",
        key: "id",
      },
    },
  },
  {
    sequelize,
    modelName: "XeroToken",
    tableName: "xero_tokens",
    timestamps: false,
  }
);

module.exports = XeroToken;
