const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    email: { type: DataTypes.STRING, allowNull: false },
    phone: { type: DataTypes.STRING, allowNull: false },
    passwordHash: { type: DataTypes.STRING, allowNull: true },
    firstName: { type: DataTypes.STRING, allowNull: false },
    lastName: { type: DataTypes.STRING, allowNull: false },
    position: { type: DataTypes.STRING, allowNull: false },
    role: { type: DataTypes.STRING, allowNull: false },
    active: { type: DataTypes.BOOLEAN, allowNull: false },
    verificationToken: { type: DataTypes.STRING },
    verified: { type: DataTypes.DATE },
    resetToken: { type: DataTypes.STRING },
    resetTokenExpires: { type: DataTypes.DATE },
    passwordReset: { type: DataTypes.DATE },
    created: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    updated: { type: DataTypes.DATE },
    isVerified: {
      type: DataTypes.VIRTUAL,
      get() {
        return !!(this.verified || this.passwordReset);
      },
    },
    // Foreign key for Customer
    customerId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const options = {
    timestamps: false,
    defaultScope: {
      attributes: { exclude: ["passwordHash"] },
    },
    scopes: {
      withHash: { attributes: {} },
    },
  };

  const User = sequelize.define("user", attributes, options);

  User.associate = (models) => {
    // user - refreshToken
    User.hasMany(models.RefreshToken, {
      foreignKey: "userId",
      onDelete: "CASCADE",
    });

    // user - customer
    User.belongsTo(models.Customer, { foreignKey: "customerId" });
    models.Customer.hasMany(User, { foreignKey: "customerId" });
  };

  return User;
}
