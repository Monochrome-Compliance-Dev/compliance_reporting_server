/**
 * TODO: Consider alternative test DB strategy
 *
 * We're currently using Sequelize with mysql2 in a Jest test environment.
 * Known issues:
 * - mysql2@3.x in combination with Node 20+ and Jest can trigger:
 *   - `authSwitch.authSwitchRequest is not a function`
 *   - `import after Jest teardown`
 *   - Hanging TCPWRAP handles
 *
 * Current workaround:
 * - Use mysql_native_password plugin on test user
 * - Avoid dynamic import of ESM-only libraries (e.g. nanoid) during model definition
 *
 * Future options:
 * 1. Switch to SQLite in-memory DB for tests
 * 2. Use Vitest (or another modern runner) with full async teardown control
 * 3. Mock Sequelize and avoid DB integration in Jest entirely
 */
const db = require("../helpers/db");

beforeAll(async () => {
  try {
    await db.sequelize.authenticate();
  } catch (err) {
    console.error("❌ Failed to connect to test DB:", err);
    throw err;
  }
});

afterAll(async () => {
  try {
    await db.sequelize.close();
  } catch (err) {
    console.error("❌ Failed to disconnect from test DB:", err);
  }
});
