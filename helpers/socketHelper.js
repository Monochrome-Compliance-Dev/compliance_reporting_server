function emitSocketEvent(io, { userId }, type, stage, payload = {}) {
  if (io && userId && type && stage) {
    io.to(userId).emit("statusUpdate", { type, stage, payload });
  }
}

module.exports = { emitSocketEvent };
