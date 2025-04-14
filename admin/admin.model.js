const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
    ReportingPeriodStartDate: { type: DataTypes.DATE, allowNull: false },
    ReportingPeriodEndDate: { type: DataTypes.DATE, allowNull: false },
    ReportComments: { type: DataTypes.STRING, allowNull: true },
    SubmitterFirstName: { type: DataTypes.STRING, allowNull: true },
    SubmitterLastName: { type: DataTypes.STRING, allowNull: true },
    SubmitterPosition: { type: DataTypes.STRING, allowNull: true },
    SubmitterPhoneNumber: { type: DataTypes.STRING, allowNull: true },
    SubmitterEmail: { type: DataTypes.STRING, allowNull: true },
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
    reportName: { type: DataTypes.STRING, allowNull: false },
    createdBy: { type: DataTypes.INTEGER, allowNull: false },
    updatedBy: { type: DataTypes.INTEGER, allowNull: true },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.INTEGER, allowNull: true },
    reportStatus: {
      type: DataTypes.ENUM(
        "Created",
        "Cancelled",
        "Received",
        "Accepted",
        "Rejected"
      ),
      allowNull: false,
      defaultValue: "Created",
    },
    statusUpdatedDate: { type: DataTypes.DATE, allowNull: true },
    clientId: { type: DataTypes.INTEGER, allowNull: false },
  };

  return sequelize.define("admin", attributes, { tableName: "tbl_admin" });
}
