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
    runName: { type: DataTypes.STRING(100), allowNull: false },
    periodKey: { type: DataTypes.STRING(7), allowNull: false },
    reportingPeriodStartDate: { type: DataTypes.STRING(10), allowNull: false },
    reportingPeriodEndDate: { type: DataTypes.STRING(10), allowNull: false },
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
    paranoid: true, // enable soft-deletes via deletedAt
    indexes: [
      {
        unique: true,
        name: "uniq_ptrs_customer_period_runname",
        fields: ["customerId", "periodKey", "runName"],
      },
    ],
    validate: {
      datesAlignWithPeriodKey() {
        const pk = this.periodKey;
        const start = this.reportingPeriodStartDate;
        const end = this.reportingPeriodEndDate;
        if (!pk || !start || !end) return; // allow other validators to flag requireds
        const year = String(pk).slice(0, 4);
        const half = String(pk).slice(5);
        const expected =
          half === "01"
            ? { s: `${year}-01-01`, e: `${year}-06-30` }
            : { s: `${year}-07-01`, e: `${year}-12-31` };
        if (start !== expected.s || end !== expected.e) {
          throw new Error(
            `reportingPeriodStartDate/endDate must align with periodKey (${pk})`
          );
        }
      },
    },
  });

  return Ptrs;
}
