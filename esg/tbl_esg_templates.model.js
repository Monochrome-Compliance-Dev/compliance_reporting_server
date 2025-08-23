const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const Template = sequelize.define(
    "Template",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
      },
      customerId: {
        type: DataTypes.STRING(10),
        allowNull: true, // null means global template
      },
      fieldType: {
        type: DataTypes.ENUM("indicator", "metric"),
        allowNull: false,
      },
      fieldName: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      category: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      defaultUnit: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      createdBy: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      updatedBy: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
    },
    {
      tableName: "tbl_esg_templates",
      timestamps: true,
      paranoid: true,
    }
  );
  return Template;
};
