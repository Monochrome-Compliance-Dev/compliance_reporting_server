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
    fieldName: {
      type: DataTypes.STRING(50),
      allowNull: false,
    },
    oldValue: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    newValue: {
      type: DataTypes.STRING(50),
      allowNull: true,
    },
    action: {
      type: DataTypes.ENUM("create", "update", "delete"),
      allowNull: false,
      index: true,
    },
    step: {
      type: DataTypes.STRING(20),
      allowNull: true,
    },
    createdAt: {
      type: DataTypes.DATE,
      allowNull: false,
      defaultValue: DataTypes.NOW,
    },
    createdBy: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    // Foreign keys
    tcpId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
    clientId: {
      type: DataTypes.STRING(10),
      allowNull: false,
    },
  };

  const Audit = sequelize.define("audit", attributes, {
    tableName: "tbl_tcp_audit",
    timestamps: false,
  });

  Audit.associate = (models) => {
    Audit.belongsTo(models.Tcp, { foreignKey: "tcpId", onDelete: "CASCADE" });
    models.Tcp.hasMany(Audit, { foreignKey: "tcpId", onDelete: "CASCADE" });

    Audit.belongsTo(models.Client, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
    models.Client.hasMany(Audit, {
      foreignKey: "clientId",
      onDelete: "CASCADE",
    });
  };

  return Audit;
}
