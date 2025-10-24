const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = model;

/**
 * PTRS v2: Upload metadata (parent for staging rows, mapping, transforms, calc, reports).
 */
function model(sequelize) {
  const attributes = {
    id: {
      type: DataTypes.STRING(10),
      defaultValue: () => nanoid(10),
      primaryKey: true,
    },
    customerId: { type: DataTypes.STRING(10), allowNull: false },
    fileName: { type: DataTypes.STRING, allowNull: false },
    fileSize: { type: DataTypes.INTEGER, allowNull: true },
    mimeType: { type: DataTypes.STRING, allowNull: true },
    hash: { type: DataTypes.STRING, allowNull: true }, // e.g., sha256
    rowCount: { type: DataTypes.INTEGER, allowNull: true },
    status: {
      type: DataTypes.STRING(20),
      allowNull: false,
      defaultValue: "Uploaded",
      validate: {
        isIn: [
          [
            "Uploaded",
            "Ingested",
            "Mapped",
            "Transformed",
            "Calculated",
            "Error",
          ],
        ],
      },
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const PtrsUpload = sequelize.define("ptrs_upload", attributes, {
    tableName: "tbl_ptrs_upload",
    timestamps: true,
    paranoid: false,
    indexes: [
      { name: "idx_ptrs_upload_customer", fields: ["customerId"] },
      { name: "idx_ptrs_upload_hash", fields: ["hash"] },
    ],
  });

  return PtrsUpload;
}
