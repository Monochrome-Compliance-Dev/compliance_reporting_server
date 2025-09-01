const { DataTypes, Op } = require("sequelize");
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
    customerId: { type: DataTypes.STRING, allowNull: false },
    userId: { type: DataTypes.STRING(10), allowNull: false },

    // Stripe linkage
    stripeCustomerId: { type: DataTypes.STRING, allowNull: true },
    stripeSubscriptionId: { type: DataTypes.STRING, allowNull: true },
    stripePriceId: { type: DataTypes.STRING, allowNull: true },

    // App-facing plan & seats
    planCode: { type: DataTypes.STRING, allowNull: true },
    seats: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 1 },

    // Status mirrors Stripe but kept as string to avoid ENUM migrations
    isActive: {
      type: DataTypes.BOOLEAN,
      allowNull: false,
      defaultValue: false,
    },
    status: {
      type: DataTypes.STRING,
      allowNull: false,
      defaultValue: "incomplete", // e.g., trialing | active | past_due | canceled ...
    },

    // Timing
    trialEndsAt: { type: DataTypes.DATE, allowNull: true },
    introPriceEndsAt: { type: DataTypes.DATE, allowNull: true },

    // Audit
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const StripeUser = sequelize.define("stripe_user", attributes, {
    tableName: "tbl_stripe_user",
    timestamps: true,
    indexes: [
      // Only one stripe linkage per (tenant, user)
      {
        unique: true,
        fields: ["customerId", "userId"],
      },
      // stripeCustomerId must be unique if present
      {
        unique: true,
        fields: ["stripeCustomerId"],
        where: {
          stripeCustomerId: { [Op.ne]: null },
        },
      },
      // stripeSubscriptionId must be unique if present
      {
        unique: true,
        fields: ["stripeSubscriptionId"],
        where: {
          stripeSubscriptionId: { [Op.ne]: null },
        },
      },
    ],
  });

  return StripeUser;
}
