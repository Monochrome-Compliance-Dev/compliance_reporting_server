const { DataTypes } = require("sequelize");

module.exports = model;

function model(sequelize) {
  const attributes = {
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
    createdBy: { type: DataTypes.INTEGER, allowNull: false },
    updatedBy: { type: DataTypes.INTEGER, allowNull: true },
    submittedDate: { type: DataTypes.DATE, allowNull: true },
    submittedBy: { type: DataTypes.INTEGER, allowNull: true },
    statusUpdatedDate: { type: DataTypes.DATE, allowNull: true },
  };

  return sequelize.define("submission", attributes, {
    tableName: "tbl_submissions",
  });
}
