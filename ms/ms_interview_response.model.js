const { DataTypes } = require("sequelize");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = (sequelize) => {
  const MSInterviewResponse = sequelize.define(
    "MSInterviewResponse",
    {
      id: {
        type: DataTypes.STRING(10),
        primaryKey: true,
      },
      clientId: { type: DataTypes.STRING(10), allowNull: false },
      reportingPeriodId: { type: DataTypes.STRING(10), allowNull: false },
      section: { type: DataTypes.STRING, allowNull: false },
      question: { type: DataTypes.TEXT, allowNull: false },
      answer: { type: DataTypes.TEXT, allowNull: false },
      createdBy: { type: DataTypes.STRING(10), allowNull: false },
      updatedBy: { type: DataTypes.STRING(10), allowNull: true },
    },
    {
      tableName: "tbl_ms_interview_responses",
      timestamps: true,
      paranoid: true,
    }
  );
  return MSInterviewResponse;
};
