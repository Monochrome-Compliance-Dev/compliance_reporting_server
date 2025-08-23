// server-test.js
const { Server } = require("socket.io");
const http = require("http");

const server = http.createServer((req, res) => {
  res.writeHead(200);
  res.end("Socket.io test server running");
});

const io = new Server(server, {
  cors: {
    origin: "http://localhost:3000",
    methods: ["GET", "POST"],
  },
});

io.on("connection", (socket) => {
  console.log("🚀 New socket.io connection: ", socket.id);

  socket.emit("hello", { message: "Hi there customer!" });

  socket.on("ping-me", (data) => {
    console.log("💬 Received from customer:", data);
    socket.emit("pong-back", { message: "Pong from server" });
  });

  socket.on("disconnect", () => {
    console.log("❌ Customer disconnected:", socket.id);
  });
});

server.listen(4000, () => {
  console.log("✅ Socket.io test server listening on port 4000");
});
