const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
    clientName: { type: DataTypes.STRING, allowNull: false },
    abn: { type: DataTypes.STRING, allowNull: false },
    acn: { type: DataTypes.STRING, allowNull: false },
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
  };

  return sequelize.define("client", attributes, { tableName: "tbl_clients" });
}
