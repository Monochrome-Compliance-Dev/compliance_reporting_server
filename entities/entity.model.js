const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    BusinessName: { type: DataTypes.STRING, allowNull: false },
    ABN: { type: DataTypes.STRING, allowNull: false },
    ACN: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationName: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationABN: { type: DataTypes.STRING, allowNull: true },
    ControllingCorporationACN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityName: { type: DataTypes.STRING, allowNull: true },
    HeadEntityABN: { type: DataTypes.STRING, allowNull: true },
    HeadEntityACN: { type: DataTypes.STRING, allowNull: true },
    BusinessIndustryCode: { type: DataTypes.STRING, allowNull: true },
  };

  return sequelize.define("entity", attributes, { tableName: "tbl_entities" });
}
