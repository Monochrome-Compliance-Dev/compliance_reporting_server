const http = require("http");
http
  .createServer((_, res) => {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("pong");
  })
  .listen(8082, () => console.log("Health server listening on port 8082"));
