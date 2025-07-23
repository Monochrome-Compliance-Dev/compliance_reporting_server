const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const MSGrievance = sequelize.define(
    "MSGrievance",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      clientId: { type: DataTypes.STRING(10), allowNull: false },
      description: { type: DataTypes.TEXT, allowNull: false },
      status: {
        type: DataTypes.ENUM("Open", "Closed", "Investigating"),
        allowNull: false,
      },
      reportedAt: { type: DataTypes.DATE },
      createdBy: { type: DataTypes.STRING(10), allowNull: false },
      updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    },
    {
      tableName: "tbl_ms_grievances",
      timestamps: true,
      paranoid: true,
    }
  );
  return MSGrievance;
};
