const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    ReportingPeriodStartDate: { type: DataTypes.DATE, allowNull: false },
    ReportingPeriodEndDate: { type: DataTypes.DATE, allowNull: false },
    ReportComments: { type: DataTypes.STRING, allowNull: true },
    SubmitterFirstName: { type: DataTypes.STRING, allowNull: false },
    SubmitterLastName: { type: DataTypes.STRING, allowNull: false },
    SubmitterPosition: { type: DataTypes.STRING, allowNull: true },
    SubmitterPhoneNumber: { type: DataTypes.STRING, allowNull: true },
    SubmitterEmail: { type: DataTypes.STRING, allowNull: false },
    ApproverFirstName: { type: DataTypes.STRING, allowNull: true },
    ApproverLastName: { type: DataTypes.STRING, allowNull: true },
    ApproverPosition: { type: DataTypes.STRING, allowNull: true },
    ApproverPhoneNumber: { type: DataTypes.STRING, allowNull: true },
    ApproverEmail: { type: DataTypes.STRING, allowNull: true },
    ApprovalDate: { type: DataTypes.DATE, allowNull: true },
    PrincipalGoverningBodyName: { type: DataTypes.STRING, allowNull: true },
    PrincipalGoverningBodyDescription: {
      type: DataTypes.STRING,
      allowNull: true,
    },
    ResponsibleMemberDeclaration: { type: DataTypes.STRING, allowNull: true },
    created: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    updated: { type: DataTypes.DATE },
  };

  return sequelize.define("report", attributes, { tableName: "tbl_reports" });
}
