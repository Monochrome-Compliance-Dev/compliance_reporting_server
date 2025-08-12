const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const db = require("../db/database");
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();

module.exports = {
  createIndicator,
  createMetric,
  getMetricsByClient,
  createReportingPeriod,
  getReportingPeriodsByClient,
  getIndicatorsByReportingPeriodId,
  getMetricsByReportingPeriodId,
  getMetricById,
  deleteIndicator,
  deleteMetric,
  getReportingPeriodById,
  updateReportingPeriod,
  // Units CRUD
  createUnit,
  getUnitsByClient,
  getUnitById,
  updateUnit,
  deleteUnit,
  cloneTemplatesForReportingPeriod,
};

async function createIndicator(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.ESGIndicator.create(
      {
        ...params,
        createdBy: params.userId,
        updatedBy: params.userId,
      },
      {
        ...options,
        transaction: t,
      }
    );
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function createMetric(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const indicator = await db.ESGIndicator.findByPk(params.indicatorId, {
      transaction: t,
    });
    if (!indicator) {
      throw { status: 400, message: "Invalid indicatorId" };
    }

    const period = await db.ReportingPeriod.findByPk(params.reportingPeriodId, {
      transaction: t,
    });
    if (!period) {
      throw { status: 400, message: "Invalid reportingPeriodId" };
    }

    const result = await db.ESGMetric.create(
      {
        ...params,
        createdBy: params.userId,
        updatedBy: params.userId,
      },
      {
        ...options,
        transaction: t,
      }
    );

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getMetricsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metrics = await db.ESGMetric.findAll({
      ...options,
      transaction: t,
    });
    return metrics.map((r) => r.get({ plain: true }));
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function createReportingPeriod(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.ReportingPeriod.create(
      {
        ...params,
        createdBy: params.userId,
        updatedBy: params.userId,
      },
      {
        ...options,
        transaction: t,
      }
    );

    await t.commit();

    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getReportingPeriodsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const periods = await db.ReportingPeriod.findAll({
      ...options,
      transaction: t,
    });

    return periods.map((r) => r.get({ plain: true }));
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getIndicatorsByReportingPeriodId(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const indicators = await db.ESGIndicator.findAll({
      where: { reportingPeriodId },
      ...options,
      transaction: t,
    });

    return indicators.map((r) => r.get({ plain: true }));
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getMetricsByReportingPeriodId(
  clientId,
  reportingPeriodId,
  options = {}
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metrics = await db.ESGMetric.findAll({
      where: { reportingPeriodId },
      ...options,
      transaction: t,
    });

    return metrics.map((r) => r.get({ plain: true }));
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getMetricById(clientId, metricId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const metric = await db.ESGMetric.findOne({
      where: { id: metricId },
      ...options,
      transaction: t,
    });

    return metric ? metric.get({ plain: true }) : null;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

// Soft delete ESG Indicator by id
async function deleteIndicator(clientId, indicatorId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.ESGIndicator.destroy({
      where: { id: indicatorId },
      ...options,
      transaction: t,
    });

    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

// Soft delete ESG Metric by id
async function deleteMetric(clientId, metricId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.ESGMetric.destroy({
      where: { id: metricId },
      ...options,
      transaction: t,
    });

    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getReportingPeriodById(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const period = await db.ReportingPeriod.findByPk(id, {
      ...options,
      transaction: t,
    });

    return period ? period.get({ plain: true }) : null;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function updateReportingPeriod(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    await db.ReportingPeriod.update(
      { ...params.updates, updatedBy: params.userId },
      {
        where: { id: params.id },
        ...options,
        transaction: t,
      }
    );

    await t.commit();
    return { message: "ReportingPeriod updated." };
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

// ---- Unit CRUD ----
async function createUnit(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    const result = await db.Unit.create(
      {
        ...params,
        createdBy: params.userId,
        updatedBy: params.userId,
      },
      {
        ...options,
        transaction: t,
      }
    );
    await t.commit();
    return result.get({ plain: true });
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function getUnitsByClient(clientId, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const units = await db.Unit.findAll({
      ...options,
      transaction: t,
    });
    return units.map((r) => r.get({ plain: true }));
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function getUnitById(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const unit = await db.Unit.findByPk(id, {
      ...options,
      transaction: t,
    });
    return unit ? unit.get({ plain: true }) : null;
  } catch (error) {
    throw error;
  } finally {
    if (!t.finished) await t.rollback();
  }
}

async function updateUnit(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    await db.Unit.update(
      { ...params.updates, updatedBy: params.userId },
      {
        where: { id: params.id },
        ...options,
        transaction: t,
      }
    );
    await t.commit();
    return { message: "Unit updated." };
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function deleteUnit(clientId, id, options = {}) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    await db.Unit.destroy({
      where: { id },
      ...options,
      transaction: t,
    });
    await t.commit();
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}

async function cloneTemplatesForReportingPeriod(params, options = {}) {
  const t = await beginTransactionWithClientContext(params.clientId);
  try {
    // Fetch global templates plus client-specific templates
    const templates = await db.Template.findAll({
      where: {
        [db.Sequelize.Op.or]: [
          { clientId: null },
          { clientId: params.clientId },
        ],
      },
      transaction: t,
    });

    // Separate by type
    const indicatorTemplates = templates.filter(
      (t) => t.fieldType === "indicator"
    );
    const metricTemplates = templates.filter((t) => t.fieldType === "metric");

    // Clone indicators
    const clonedIndicators = await Promise.all(
      indicatorTemplates.map(async (template) => {
        return await db.ESGIndicator.create(
          {
            id: nanoid(10),
            clientId: params.clientId,
            reportingPeriodId: params.reportingPeriodId,
            code: template.fieldName,
            name: template.fieldName,
            description: template.description,
            category: template.category,
            isTemplate: false,
            createdBy: params.userId,
            updatedBy: params.userId,
          },
          { transaction: t }
        );
      })
    );

    // Clone metrics, matching to cloned indicators by fieldName
    await Promise.all(
      metricTemplates.map(async (template) => {
        const matchingIndicator = clonedIndicators.find(
          (ci) => ci.code === template.fieldName
        );
        if (!matchingIndicator) return;

        return await db.ESGMetric.create(
          {
            id: nanoid(10),
            clientId: params.clientId,
            reportingPeriodId: params.reportingPeriodId,
            indicatorId: matchingIndicator.id,
            value: 0,
            unitId: null, // can be improved to look up unit by template.defaultUnit
            isTemplate: false,
            createdBy: params.userId,
            updatedBy: params.userId,
          },
          { transaction: t }
        );
      })
    );

    await t.commit();
    return { message: "Templates cloned for reporting period." };
  } catch (error) {
    if (!t.finished) await t.rollback();
    throw error;
  }
}
