const config = require("../config.json");
const mysql = require("mysql2/promise");
const { Sequelize } = require("sequelize");

const winston = require("./logger");

// Use environment variables with fallback to config.json
const DB_HOST = process.env.DB_HOST || config.database.host;
const DB_PORT = process.env.DB_PORT || config.database.port;
const DB_USER = process.env.DB_USER || config.database.user;
const DB_PASSWORD = process.env.DB_PASSWORD || config.database.password;
const DB_NAME = process.env.DB_NAME || config.database.database;
const DB_SOCKET_PATH = process.env.DB_SOCKET_PATH || config.database.socketPath;

const sequelize = new Sequelize(DB_NAME, DB_USER, DB_PASSWORD, {
  dialect: "mysql",
  host: DB_HOST,
  dialectOptions: { decimalNumbers: true, socketPath: DB_SOCKET_PATH },
  pool: {
    max: 100,
    min: 0,
    acquire: 30000,
    idle: 10000,
  },
  logging: console.log,
  hooks: {
    afterConnect: async (connection) => {
      await connection
        .promise()
        .query("SET collation_connection = 'utf8mb4_0900_ai_ci'");
    },
  },
});

module.exports = db = {
  sequelize,
};

initialize();

// mysql: counting number of tickets which are open per day basis
// https://dba.stackexchange.com/questions/101249/mysql-counting-number-of-tickets-which-are-open-per-day-basis

async function initialize() {
  let retries = 5;
  while (retries) {
    try {
      const connection = await mysql.createConnection({
        host: DB_HOST,
        port: DB_PORT,
        user: DB_USER,
        password: DB_PASSWORD,
        socketPath: DB_SOCKET_PATH,
      });
      await connection.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\`;`);
      break;
    } catch (err) {
      retries -= 1;
      winston.error("Database connection failed. Retrying...", err);
      if (!retries) throw err;
      await new Promise((res) => setTimeout(res, 5000));
    }
  }

  // connect to db
  await sequelize.authenticate();
  winston.info(
    "Connection to the Compliance Reporting database has been established successfully."
  );

  // init models and add them to the exported db object
  db.User = require("../users/user.model")(sequelize);
  db.RefreshToken = require("../users/refresh-token.model")(sequelize);
  db.Client = require("../clients/client.model")(sequelize);
  db.Report = require("../reports/report.model")(sequelize);
  db.Tcp = require("../tcp/tcp.model")(sequelize);
  db.Tat = require("../tat/tat.model")(sequelize);
  db.Entity = require("../entities/entity.model")(sequelize);
  db.Booking = require("../booking/booking.model")(sequelize);
  db.Tracking = require("../tracking/tracking.model")(sequelize);

  // define relationships
  db.User.hasMany(db.RefreshToken, { onDelete: "CASCADE" });
  db.RefreshToken.belongsTo(db.User);
  db.User.belongsTo(db.Client);
  db.Client.hasMany(db.User);
  db.Client.hasMany(db.Report, { onDelete: "CASCADE" });
  db.Client.hasMany(db.Tat, { onDelete: "CASCADE" });
  db.Client.hasMany(db.Tcp, { onDelete: "CASCADE" });
  db.Report.belongsTo(db.Client);
  db.Tcp.belongsTo(db.Report, { onDelete: "CASCADE" });
  db.Report.hasMany(db.Tcp, { onDelete: "CASCADE" });
  db.Tat.belongsTo(db.Report, { onDelete: "CASCADE" });
  db.Report.hasMany(db.Tat, { onDelete: "CASCADE" });

  // sync all models with database
  await sequelize.sync();
}
