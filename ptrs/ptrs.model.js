const { DataTypes, INTEGER } = require("sequelize");
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
    reportingPeriodStartDate: { type: DataTypes.DATE, allowNull: false },
    reportingPeriodEndDate: { type: DataTypes.DATE, allowNull: false },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.STRING(10), allowNull: true },
    currentStep: { type: DataTypes.INTEGER, allowNull: false, defaultValue: 0 },
    status: {
      type: DataTypes.ENUM(
        "Created",
        "Cancelled",
        "In Progress",
        "Updated",
        "Received",
        "Accepted",
        "Rejected",
        "Submitted",
        "Deleted",
        "Validated"
      ),
      allowNull: false,
      defaultValue: "Created",
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: false },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    // Foreign key for Customer
    customerId: { type: DataTypes.STRING(10), allowNull: false },
  };

  const Ptrs = sequelize.define("ptrs", attributes, {
    tableName: "tbl_ptrs",
    timestamps: true,
  });

  Ptrs.associate = (models) => {
    Ptrs.belongsTo(models.Customer, { foreignKey: "customerId" });
    models.Customer.hasMany(Ptrs, {
      foreignKey: "customerId",
      onDelete: "CASCADE",
    });

    Ptrs.hasMany(models.Tcp, { foreignKey: "reportId", onDelete: "CASCADE" });
    models.Tcp.belongsTo(Ptrs, {
      foreignKey: "reportId",
      onDelete: "CASCADE",
    });
  };

  return Ptrs;
}
