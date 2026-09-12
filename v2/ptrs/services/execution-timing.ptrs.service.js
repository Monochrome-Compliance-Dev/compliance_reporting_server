function startExecutionTiming() {
  return {
    startedAt: new Date().toISOString(),
    monotonicStartedAt: process.hrtime.bigint(),
  };
}

function finishExecutionTiming(timing) {
  return {
    startedAt: timing.startedAt,
    finishedAt: new Date().toISOString(),
    elapsedMs:
      Number(process.hrtime.bigint() - timing.monotonicStartedAt) / 1e6,
  };
}

async function measureExecutionPhase({ timings, name, run }) {
  const timing = startExecutionTiming();
  try {
    return await run();
  } finally {
    timings[name] = finishExecutionTiming(timing);
  }
}

module.exports = {
  finishExecutionTiming,
  measureExecutionPhase,
  startExecutionTiming,
};
