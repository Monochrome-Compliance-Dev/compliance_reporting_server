const config = require("../config.json");
const mysql = require("mysql2/promise");
const { Sequelize } = require("sequelize");

// Use environment variables with fallback to config.json
const DB_HOST = process.env.DB_HOST || config.database.host;
const DB_PORT = process.env.DB_PORT || config.database.port;
const DB_USER = process.env.DB_USER || config.database.user;
const DB_PASSWORD = process.env.DB_PASSWORD || config.database.password;
const DB_NAME = process.env.DB_NAME || config.database.database;
const DB_SOCKET_PATH = process.env.DB_SOCKET_PATH || config.database.socketPath;

module.exports = db = {};

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
      console.error("Database connection failed. Retrying...", err);
      if (!retries) throw err;
      await new Promise((res) => setTimeout(res, 5000));
    }
  }

  // connect to db
  const sequelize = await initSequelize(
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_SOCKET_PATH
  );

  // init models and add them to the exported db object
  db.User = require("../users/user.model")(sequelize);
  db.RefreshToken = require("../users/refresh-token.model")(sequelize);
  db.Client = require("../clients/client.model")(sequelize);
  db.Record = require("../records/record.model")(sequelize);

  // define relationships
  db.User.hasMany(db.RefreshToken, { onDelete: "CASCADE" });
  db.RefreshToken.belongsTo(db.User);
  db.User.belongsTo(db.Client);
  db.Client.hasMany(db.User);
  db.Client.hasMany(db.Record);
  db.Record.belongsTo(db.Client);
  db.Client.hasMany(db.Record, { onDelete: "CASCADE" });
  db.Record.belongsTo(db.Client, { onDelete: "CASCADE" });

  // sync all models with database
  await sequelize.sync();
}

async function initSequelize(database, user, password, host, socketPath) {
  const sequelize = new Sequelize(database, user, password, {
    dialect: "mysql",
    dialectOptions: { decimalNumbers: true, socketPath },
    host,
    pool: {
      max: 100,
      min: 0,
      acquire: 30000,
      idle: 10000,
    },
  });

  try {
    await sequelize.authenticate();
    console.log(
      "Connection to the Compliance Reporting database has been established successfully."
    );
  } catch (error) {
    console.error(
      "Unable to connect to the Compliance Reporting database:",
      error
    );
  }
  return sequelize;
}
