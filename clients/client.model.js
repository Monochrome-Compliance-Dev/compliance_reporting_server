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
    businessName: { type: DataTypes.STRING, allowNull: false },
    abn: { type: DataTypes.STRING, allowNull: false },
    acn: { type: DataTypes.STRING, allowNull: true },
    addressline1: { type: DataTypes.STRING, allowNull: false },
    addressline2: { type: DataTypes.STRING, allowNull: true },
    addressline3: { type: DataTypes.STRING, allowNull: true },
    city: { type: DataTypes.STRING, allowNull: false },
    state: { type: DataTypes.STRING, allowNull: false },
    postcode: { type: DataTypes.STRING, allowNull: false },
    country: { type: DataTypes.STRING, allowNull: false },
    postaladdressline1: { type: DataTypes.STRING, allowNull: true },
    postaladdressline2: { type: DataTypes.STRING, allowNull: true },
    postaladdressline3: { type: DataTypes.STRING, allowNull: true },
    postalcity: { type: DataTypes.STRING, allowNull: true },
    postalstate: { type: DataTypes.STRING, allowNull: true },
    postalpostcode: { type: DataTypes.STRING, allowNull: true },
    postalcountry: { type: DataTypes.STRING, allowNull: true },
    industryCode: { type: DataTypes.STRING, allowNull: false },
    contactFirst: { type: DataTypes.STRING, allowNull: false },
    contactLast: { type: DataTypes.STRING, allowNull: false },
    contactPosition: { type: DataTypes.STRING, allowNull: false },
    contactEmail: { type: DataTypes.STRING, allowNull: false },
    contactPhone: { type: DataTypes.STRING, allowNull: false },
    controllingCorporationName: { type: DataTypes.STRING, allowNull: true },
    controllingCorporationAbn: { type: DataTypes.STRING, allowNull: true },
    controllingCorporationAcn: { type: DataTypes.STRING, allowNull: true },
    headEntityName: { type: DataTypes.STRING, allowNull: true },
    headEntityAbn: { type: DataTypes.STRING, allowNull: true },
    headEntityAcn: { type: DataTypes.STRING, allowNull: true },
    active: { type: DataTypes.BOOLEAN, defaultValue: true },
    paymentConfirmed: { type: DataTypes.BOOLEAN, defaultValue: false },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    billingType: {
      type: DataTypes.ENUM("DIRECT", "RESELLER"),
      allowNull: false,
      defaultValue: "DIRECT",
    },
    partnerId: {
      type: DataTypes.STRING(10),
      allowNull: true,
      references: {
        model: "tbl_partner",
        key: "id",
      },
    },
  };

  const Client = sequelize.define("client", attributes, {
    tableName: "tbl_client",
    timestamps: true,
    paranoid: true,
  });

  // Correct relationships
  Client.associate = (models) => {
    Client.hasMany(models.User, { foreignKey: "clientId" });
    Client.hasMany(models.Report, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    Client.hasMany(models.Tcp, { foreignKey: "clientId", onDelete: "CASCADE" });
    Client.hasMany(models.Audit, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    Client.belongsTo(models.Partner, { foreignKey: "partnerId" });
  };

  return Client;
}
