const db = require("../helpers/db");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  logger.logEvent("info", "Fetching all clients", { action: "GetAllClients" });
  return await db.Client.findAll();
}

async function getById(id) {
  logger.logEvent("info", "Fetching client by ID", {
    action: "GetClient",
    clientId: id,
  });
  return await getClient(id);
}

async function create(params) {
  logger.logEvent("info", "Creating new client", {
    action: "CreateClient",
    abn: params.abn,
  });
  // validate
  if (await db.Client.findOne({ where: { abn: params.abn } })) {
    throw { status: 500, message: "Client with this ABN already exists" };
  }

  // save client
  const client = await db.Client.create(params);

  // Create views for tables locked down to the client
  await createClientViews(client.id);
  await createClientTriggers(client.id);
  logger.logEvent("info", "Client created", {
    action: "CreateClient",
    clientId: client.id,
  });
  return client;
}

async function update(id, params) {
  logger.logEvent("info", "Updating client", {
    action: "UpdateClient",
    clientId: id,
  });
  const client = await getClient(id);

  // validate
  // if (
  //   params.businessName !== client.businessName &&
  //   (await db.Client.findOne({ where: { businessName: params.businessName } }))
  // ) {
  //   throw "Client with this ABN already exists";
  // }

  // copy params to client and save
  Object.assign(client, params);
  await client.save();
  logger.logEvent("info", "Client updated", {
    action: "UpdateClient",
    clientId: id,
  });
}

async function _delete(id) {
  logger.logEvent("info", "Deleting client", {
    action: "DeleteClient",
    clientId: id,
  });
  const client = await getClient(id);

  // Drop views
  for (const tableName of tablesNames) {
    const viewName = `client_${client.id}_${tableName}`;
    try {
      await db.sequelize.query(`DROP VIEW IF EXISTS \`${viewName}\`;`);
    } catch (error) {
      logger.logEvent("error", "Failed to drop view", {
        action: "DropClientView",
        viewName,
        error: error.message,
      });
    }
  }

  // Drop triggers
  const triggerNames = ["before_insert", "before_update"];
  for (const table of tablesNames) {
    for (const name of triggerNames) {
      const triggerName = `trg_client_${client.id}_${table}_${name}`;
      try {
        await db.sequelize.query(`DROP TRIGGER IF EXISTS \`${triggerName}\`;`);
      } catch (error) {
        logger.logEvent("error", "Failed to drop trigger", {
          action: "DropClientTrigger",
          triggerName,
          error: error.message,
        });
      }
    }
  }

  // Delete the client record
  await client.destroy();
  logger.logEvent("warn", "Client deleted", {
    action: "DeleteClient",
    clientId: id,
  });
}

// helper functions
async function getClient(id) {
  const client = await db.Client.findByPk(id);
  if (!client) throw { status: 404, message: "Client not found" };
  return client;
}

const tablesNames = ["tbl_report", "tbl_tat", "tbl_tcp"];

async function createClientViews(clientId) {
  const results = [];

  for (const tableName of tablesNames) {
    const viewName = `client_${clientId}_${tableName}`;

    const sql = `
      CREATE OR REPLACE VIEW \`${viewName}\` AS
      SELECT * FROM ${tableName} WHERE clientId = ?
    `;

    const safeSql = sql.replace("?", `'${clientId}'`);
    try {
      await db.sequelize.query(safeSql); // Ensure db.sequelize is properly initialized
      results.push({ tableName, success: true });
      logger.logEvent("info", "View created", {
        action: "CreateClientView",
        viewName,
        clientId,
        tableName,
      });
    } catch (error) {
      results.push({ tableName, success: false, error: error.message });
      logger.logEvent("error", "Failed to create view", {
        action: "CreateClientView",
        viewName,
        clientId,
        tableName,
        error: error.message,
      });
    }
  }

  if (results.some((result) => !result.success)) {
    throw new Error("Failed to create some client views");
  }

  return results;
}

async function createClientTriggers(clientId) {
  const triggerTemplates = [
    {
      name: `before_insert`,
      timing: `BEFORE`,
      event: `INSERT`,
      logic: `
        IF NEW.id IS NULL OR NEW.id = '' THEN
          SET NEW.id = SUBSTRING(REPLACE(UUID(), '-', ''), 1, 10);
        END IF;

        SET NEW.clientId = @current_client_id;

        IF NEW.createdAt IS NULL THEN
          SET NEW.createdAt = NOW();
        END IF;

        IF NEW.updatedAt IS NULL THEN
          SET NEW.updatedAt = NOW();
        END IF;
      `,
    },
    {
      name: `before_update`,
      timing: `BEFORE`,
      event: `UPDATE`,
      logic: `
        IF NEW.clientId != OLD.clientId THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'clientId cannot be changed';
        END IF;

        IF OLD.clientId != @current_client_id THEN
          SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Unauthorized client update';
        END IF;

        SET NEW.updatedAt = NOW();
      `,
    },
  ];

  for (const table of tablesNames) {
    for (const trigger of triggerTemplates) {
      const triggerName = `trg_client_${clientId}_${table}_${trigger.name}`;
      const sql = `
        CREATE TRIGGER \`${triggerName}\`
        ${trigger.timing} ${trigger.event} ON \`${table}\`
        FOR EACH ROW
        BEGIN
          ${trigger.logic}
        END;
      `;

      try {
        await db.sequelize.query(`DROP TRIGGER IF EXISTS \`${triggerName}\`;`);
        await db.sequelize.query(sql);
        logger.logEvent("info", "Trigger created", {
          action: "CreateClientTrigger",
          triggerName,
          clientId,
          tableName: table,
        });
      } catch (error) {
        logger.logEvent("error", "Failed to create trigger", {
          action: "CreateClientTrigger",
          triggerName,
          clientId,
          tableName: table,
          error: error.message,
        });
      }
    }
  }
}
