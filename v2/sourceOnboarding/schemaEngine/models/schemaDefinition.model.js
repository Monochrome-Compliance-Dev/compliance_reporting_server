const { DataTypes } = require("sequelize");
const { nanoid } = require("nanoid");

module.exports = (sequelize) => {
  const SourceSchemaDefinition = sequelize.define(
    "SourceSchemaDefinition",
    {
      id: {
        type: DataTypes.STRING,
        primaryKey: true,
        allowNull: false,
        defaultValue: () => nanoid(),
      },
      schemaKey: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      name: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      datasetType: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      version: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      status: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      description: {
        type: DataTypes.TEXT,
        allowNull: true,
      },
      definition: {
        type: DataTypes.JSONB,
        allowNull: false,
      },
      createdBy: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      updatedBy: {
        type: DataTypes.STRING,
        allowNull: true,
      },
    },
    {
      tableName: "tbl_source_schema_definition",
      paranoid: true,
      indexes: [
        {
          unique: true,
          fields: ["schemaKey", "version"],
        },
        {
          fields: ["datasetType"],
        },
        {
          fields: ["status"],
        },
      ],
    },
  );

  return SourceSchemaDefinition;
};
