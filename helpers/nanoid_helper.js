// helpers/nanoid_helper.js
let nanoidFn = null;

// fire the dynamic import once at module load
(async () => {
  const { nanoid } = await import("nanoid");
  nanoidFn = nanoid;
})();

/**
 * Return a nanoid string of the given size.
 * This MUST be synchronous when used in Sequelize defaultValue.
 */
function getNanoid(size = 10) {
  if (!nanoidFn) {
    // If this ever happens, it means something tried to use it
    // before the import resolved at startup.
    throw new Error("nanoid not initialised yet");
  }
  return nanoidFn(size);
}

module.exports = { getNanoid };
