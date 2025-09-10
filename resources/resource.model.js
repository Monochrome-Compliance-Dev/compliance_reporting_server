const { DataTypes, Op } = require("sequelize");
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
    name: { type: DataTypes.STRING, allowNull: false },
    position: { type: DataTypes.STRING, allowNull: true },
    team: { type: DataTypes.STRING, allowNull: true },
    hourlyRate: { type: DataTypes.DECIMAL(10, 2), allowNull: true },
    capacityHoursPerWeek: { type: DataTypes.INTEGER, allowNull: true },
    email: { type: DataTypes.STRING, allowNull: true },
    userId: { type: DataTypes.STRING(10), allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Resource = sequelize.define("resource", attributes, {
    tableName: "tbl_pulse_resource",
    timestamps: true,
    indexes: [
      {
        unique: true,
        fields: ["customerId", "userId"],
        where: {
          userId: { [Op.ne]: null },
        },
      },
    ],
  });

  return Resource;
}
