let nanoidCached = null;
async function getNanoid() {
  if (!nanoidCached) {
    nanoidCached = (await import("nanoid")).nanoid;
  }
  return nanoidCached;
}

module.exports = { getNanoid };
