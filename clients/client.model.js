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
    customerId: { type: DataTypes.STRING, allowNull: false },
    phone: { type: DataTypes.STRING, allowNull: true },
    email: { type: DataTypes.STRING, allowNull: false },
    name: {
      type: DataTypes.STRING,
      allowNull: false,
    },
    abn: {
      type: DataTypes.STRING(20),
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
  };

  const Client = sequelize.define("client", attributes, {
    tableName: "tbl_pulse_client",
    timestamps: true,
  });

  return Client;
}
