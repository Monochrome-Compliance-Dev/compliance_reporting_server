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
    customerId: { type: DataTypes.STRING(12), allowNull: false },
    field: {
      // which TCP field the rule checks
      type: DataTypes.STRING(32),
      allowNull: false,
      validate: { isIn: [["description", "accountCode"]] },
    },
    term: { type: DataTypes.STRING(255), allowNull: false },
    matchType: {
      type: DataTypes.STRING(16),
      allowNull: false,
      validate: { isIn: [["contains", "equals", "regex"]] },
    },
    createdBy: { type: DataTypes.STRING(10), allowNull: true },
    updatedBy: { type: DataTypes.STRING(10), allowNull: true },
  };

  const ExclusionKeywordCustomerRef = sequelize.define(
    "ExclusionKeywordCustomerRef",
    attributes,
    {
      tableName: "tbl_exclusion_keyword_customer_ref",
      timestamps: true,
      paranoid: true,
      indexes: [{ fields: ["customerId", "field", "term"], unique: true }],
    }
  );

  return ExclusionKeywordCustomerRef;
}
