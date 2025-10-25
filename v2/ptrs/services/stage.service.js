const db = require("@/db/database.js");
const { logger } = require("@/utils/logger.js");

async function stageRun({ customerId, runId, profileId }) {
  const t0 = Date.now();
  const Run = db.models.ptrs_run;
  const Dataset = db.models.ptrs_raw_dataset;
  const Stage = db.models.ptrs_stage_row;
  const Map = db.models.ptrs_map;

  const run = await Run.findByPk(runId);
  if (!run) throw new Error(`Run ${runId} not found`);

  const mapRec = await Map.findOne({ where: { runId, customerId } });
  if (!mapRec) throw new Error("No mapping found for this run");

  const datasets = await Dataset.findAll({ where: { runId, customerId } });
  const main = datasets.find((d) => d.role === "transactions") || datasets[0];
  if (!main) throw new Error("No dataset uploaded yet");

  const map = mapRec.mappings || {};
  const rows = main.rows || [];
  if (!Array.isArray(rows)) throw new Error("Dataset not parsed to rows");

  await Stage.destroy({ where: { runId } });

  const stagedRows = [];
  for (const r of rows) {
    const standard = {};
    const custom = {};
    for (const [source, cfg] of Object.entries(map)) {
      const target = cfg?.field;
      const val = r[source];
      if (!target) continue;
      const isStandard = [
        "payerAbn",
        "payerEntityName",
        "payeeAbn",
        "payeeEntityName",
        "invoiceIssueDate",
        "paymentDate",
        "invoiceAmount",
        "paymentAmount",
        "documentNumber",
        "documentType",
        "companyCode",
        "currency",
      ].includes(target);
      if (isStandard) standard[target] = val;
      else custom[target] = val;
    }
    stagedRows.push({
      id: r.id || undefined,
      customerId,
      runId,
      srcRowId: r.id || null,
      standard,
      custom,
      meta: { sourceFile: main.fileName, lineNo: r.lineNo || null },
    });
  }

  await Stage.bulkCreate(stagedRows);
  const tookMs = Date.now() - t0;
  logger.info(`Staged ${stagedRows.length} rows for ${runId}`, {
    meta: { runId, rows: stagedRows.length, tookMs },
  });
  return { rowsIn: rows.length, rowsOut: stagedRows.length, tookMs };
}

module.exports = { stageRun };
