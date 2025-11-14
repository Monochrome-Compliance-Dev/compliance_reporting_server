const { DataTypes, INTEGER } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const PtrsStageRow = sequelize.define(
    "ptrs_stage_row",
    {
      id: {
        type: DataTypes.STRING,
        primaryKey: true,
        defaultValue: () => nanoid(10),
      },
      customerId: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      runId: {
        type: DataTypes.STRING,
        allowNull: false,
      },
      rowNo: {
        type: DataTypes.INTEGER,
        allowNull: false,
      },
      srcRowId: {
        type: DataTypes.STRING,
        allowNull: true,
      },
      data: { type: DataTypes.JSONB, allowNull: true, field: "data" },
      errors: { type: DataTypes.JSONB, allowNull: true, field: "errors" },
      standard: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      custom: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
      meta: {
        type: DataTypes.JSONB,
        allowNull: true,
      },
    },
    {
      tableName: "tbl_ptrs_stage_row",
      timestamps: true,
      indexes: [
        { fields: ["runId"] },
        { fields: ["customerId", "runId", "rowNo"] },
      ],
    }
  );

  return PtrsStageRow;
};
