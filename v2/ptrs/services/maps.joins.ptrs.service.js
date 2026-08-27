const { safeMeta, slog } = require("@/v2/ptrs/services/ptrs.service");

function logComposeJoinProbeOnce({
  logger,
  loggedRef,
  customerId,
  ptrsId,
  message,
  meta,
}) {
  if (loggedRef.logged || !(logger && logger.debug)) return;
  loggedRef.logged = true;
  slog.debug(
    message,
    safeMeta({
      customerId,
      ptrsId,
      ...meta,
    }),
  );
}

module.exports = {
  logComposeJoinProbeOnce,
};
