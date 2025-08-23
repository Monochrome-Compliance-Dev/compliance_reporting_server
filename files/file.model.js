const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const File = sequelize.define(
    "File",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      customerId: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
      indicatorId: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
      metricId: {
        type: DataTypes.STRING(10),
        allowNull: true,
      },
      filename: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      storagePath: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      mimeType: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      fileSize: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      uploadedBy: {
        type: DataTypes.STRING(10),
        allowNull: false,
      },
    },
    {
      tableName: "tbl_files",
      timestamps: true,
      paranoid: true,
    }
  );
  return File;
};
