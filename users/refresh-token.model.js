const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    token: { type: DataTypes.STRING },
    expires: { type: DataTypes.DATE },
    created: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    createdByIp: { type: DataTypes.STRING },
    revoked: { type: DataTypes.DATE },
    revokedByIp: { type: DataTypes.STRING },
    replacedByToken: { type: DataTypes.STRING },
    isExpired: {
      type: DataTypes.VIRTUAL,
      get() {
        return Date.now() >= this.expires;
      },
    },
    isActive: {
      type: DataTypes.VIRTUAL,
      get() {
        return !this.revoked && !this.isExpired;
      },
    },
    // Foreign key for User
    userId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const options = {
    timestamps: false,
  };

  const RefreshToken = sequelize.define("refreshToken", attributes, options);

  RefreshToken.associate = (models) => {
    RefreshToken.belongsTo(models.User, {
      foreignKey: "userId",
      onDelete: "CASCADE",
    });
  };

  return RefreshToken;
}
