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
    clientId: { type: DataTypes.STRING(10), allowNull: true },
    name: { type: DataTypes.STRING, allowNull: false },
    startDate: { type: DataTypes.DATEONLY, allowNull: false },
    endDate: { type: DataTypes.DATEONLY, allowNull: false },
    status: {
      type: DataTypes.STRING(20),
      allowNull: false,
      defaultValue: "draft",
      validate: {
        isIn: [["draft", "budgeted", "ready", "active", "cancelled"]],
      },
    },
    statusChangedAt: { type: DataTypes.DATE, allowNull: true },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const Trackable = sequelize.define("Trackable", attributes, {
    tableName: "tbl_pulse_trackable",
    timestamps: true,
    paranoid: true, // enable soft-deletes via deletedAt
    indexes: [
      { fields: ["customerId"] },
      { fields: ["clientId"] },
      { fields: ["status"] },
      { fields: ["startDate", "endDate"] },
    ],
  });

  Trackable.addHook("beforeUpdate", (inst) => {
    if (inst.changed("status")) {
      inst.set("statusChangedAt", new Date());
    }
  });

  return Trackable;
}
