const http = require("http");
const { Client } = require("pg");

const server = http.createServer(async (_, res) => {
  const client = new Client({
    host: process.env.DB_HOST,
    port: 5432,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  });

  try {
    await client.connect();
    const result = await client.query("SELECT 1 AS result");
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ db: "ok", result: result.rows[0].result }));
  } catch (err) {
    res.writeHead(500);
    res.end("DB connection failed: " + err.message);
  } finally {
    await client.end();
  }
});

server.listen(8082, () => {
  console.log("DB ping server listening on port 8082");
});
