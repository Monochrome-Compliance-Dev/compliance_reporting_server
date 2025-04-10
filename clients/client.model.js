const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
    ClientName: { type: DataTypes.STRING, allowNull: false },
    ABN: { type: DataTypes.STRING, allowNull: false },
    ACN: { type: DataTypes.STRING, allowNull: false },
    AddressLine1: { type: DataTypes.STRING, allowNull: false },
    AddressLine2: { type: DataTypes.STRING, allowNull: true },
    AddressLine3: { type: DataTypes.STRING, allowNull: true },
    City: { type: DataTypes.STRING, allowNull: false },
    State: { type: DataTypes.STRING, allowNull: false },
    Postcode: { type: DataTypes.STRING, allowNull: false },
    Country: { type: DataTypes.STRING, allowNull: false },
    PostalAddressLine1: { type: DataTypes.STRING, allowNull: true },
    PostalAddressLine2: { type: DataTypes.STRING, allowNull: true },
    PostalAddressLine3: { type: DataTypes.STRING, allowNull: true },
    PostalCity: { type: DataTypes.STRING, allowNull: true },
    PostalState: { type: DataTypes.STRING, allowNull: true },
    PostalPostcode: { type: DataTypes.STRING, allowNull: true },
    PostalCountry: { type: DataTypes.STRING, allowNull: true },
    BusinessIndustryCode: { type: DataTypes.STRING, allowNull: false },
    ContactFirst: { type: DataTypes.STRING, allowNull: false },
    ContactLast: { type: DataTypes.STRING, allowNull: false },
    ContactPosition: { type: DataTypes.STRING, allowNull: false },
    ContactEmail: { type: DataTypes.STRING, allowNull: false },
    ContactPhone: { type: DataTypes.STRING, allowNull: false },
    ControllingCorporationName: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationABN: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationACN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityName: { type: DataTypes.STRING, allowNull: true },
    HeadEntityABN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityACN: { type: DataTypes.STRING, allowNull: true },
  };

  return sequelize.define("client", attributes, { tableName: "tbl_clients" });
}
