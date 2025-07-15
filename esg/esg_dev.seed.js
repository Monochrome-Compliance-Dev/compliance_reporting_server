require("dotenv").config({ path: "../.env.development" });
const db = require("../db/database");

(async () => {
  await db.sequelize.sync();
  await db.sequelize.query(`SET app.current_client_id = 'rahfwOLxLN'`);
  const { ReportingPeriod, ESGMetric } = db.sequelize.models;

  // Find latest reporting period for the dev client
  const reportingPeriod = "9Vuzr03SSO";

  console.log(`Randomising metrics for reporting period ${reportingPeriod}`);

  const metrics = await ESGMetric.findAll({
    where: { reportingPeriodId: reportingPeriod },
  });

  for (const metric of metrics) {
    metric.value = Math.floor(Math.random() * 1000);
    await metric.save();
  }

  console.log(`Updated ${metrics.length} metrics with random values.`);
  process.exit();
})();
