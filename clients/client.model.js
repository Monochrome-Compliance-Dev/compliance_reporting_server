const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
    BusinessName: { type: DataTypes.STRING, allowNull: false },
    ABN: { type: DataTypes.STRING, allowNull: false },
    ACN: { type: DataTypes.STRING, allowNull: false },
    ControllingCorporationName: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationABN: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationACN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityName: { type: DataTypes.STRING, allowNull: true },
    HeadEntityABN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityACN: { type: DataTypes.STRING, allowNull: true },
    BusinessIndustryCode: { type: DataTypes.STRING, allowNull: false },
  };

  return sequelize.define("client", attributes, { tableName: "tbl_clients" });
}
