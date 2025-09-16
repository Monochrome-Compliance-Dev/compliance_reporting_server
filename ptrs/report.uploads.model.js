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
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    ptrsId: { type: DataTypes.STRING(10), allowNull: false },
    filename: { type: DataTypes.STRING, allowNull: false },
    filepath: { type: DataTypes.STRING, allowNull: false },
    recordCount: { type: DataTypes.INTEGER, allowNull: false },
    uploadedAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
  };

  const ReportUploads = sequelize.define("PtrsUpload", attributes, {
    tableName: "tbl_ptrs_upload",
    timestamps: false,
  });

  ReportUploads.associate = (models) => {
    ReportUploads.belongsTo(models.ptrs, {
      foreignKey: "ptrsId",
      as: "ptrs",
    });
    ReportUploads.belongsTo(models.customer, {
      foreignKey: "customerId",
      as: "customer",
    });
  };

  return ReportUploads;
}
