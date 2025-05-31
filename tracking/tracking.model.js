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
    email: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ipAddress: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    userAgent: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    timestamp: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    referer: { type: DataTypes.TEXT },
    host: { type: DataTypes.STRING },
    origin: { type: DataTypes.STRING },
    campaignId: { type: DataTypes.STRING }, // optional query param
    geoCountry: { type: DataTypes.STRING }, // inferred from IP (next step)
    geoRegion: { type: DataTypes.STRING },
    geoCity: { type: DataTypes.STRING },
  };

  return sequelize.define("tracking", attributes, {
    tableName: "tbl_tracking",
    timestamps: false,
  });
}
