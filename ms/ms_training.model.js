const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const MSTraining = sequelize.define(
    "MSTraining",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      clientId: { type: DataTypes.STRING(10), allowNull: false },
      employeeName: { type: DataTypes.STRING, allowNull: false },
      department: { type: DataTypes.STRING, allowNull: false },
      completed: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: false,
      },
      completedAt: { type: DataTypes.DATE, allowNull: true },
      createdBy: { type: DataTypes.STRING(10), allowNull: false },
      updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    },
    {
      tableName: "tbl_ms_training",
      timestamps: true,
      paranoid: true,
    }
  );
  return MSTraining;
};
