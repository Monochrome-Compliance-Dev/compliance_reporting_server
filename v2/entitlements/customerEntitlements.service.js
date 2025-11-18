const db = require("@/db/database");
const { Op } = require("sequelize");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

// Central list of supported feature keys for v2 entitlements
const SUPPORTED_FEATURE_KEYS = ["ptrs", "pulse"];

function ensureSupportedFeatureKey(feature) {
  if (!SUPPORTED_FEATURE_KEYS.includes(feature)) {
    throw {
      status: 400,
      message: `Unsupported feature key: ${feature}`,
    };
  }
}

/**
 * List all active entitlements for a customer.
 * This returns only currently valid records for the given customer.
 */
async function listByCustomer({ customerId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const now = new Date();
    const entitlements = await db.FeatureEntitlement.findAll({
      where: {
        customerId,
        status: "active",
        validFrom: {
          [Op.lte]: now,
        },
        [Op.or]: [
          { validTo: null },
          {
            validTo: {
              [Op.gt]: now,
            },
          },
        ],
      },
      order: [["feature", "ASC"]],
      transaction: t,
    });

    await t.commit();

    return entitlements.map((e) => e.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

/**
 * Set entitlements for a customer based on a features array.
 * features: [{ feature, enabled }, ...]
 *
 * For enabled features:
 *  - create a record if it does not exist
 *  - if a soft-deleted record exists, restore it
 *
 * For disabled features:
 *  - soft-delete any existing record
 */
async function setForCustomer({ customerId, features, userId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }

  if (!Array.isArray(features)) {
    throw { status: 400, message: "Missing or invalid features array" };
  }

  // Validate feature keys up front
  for (const f of features) {
    if (!f || typeof f.feature !== "string") {
      throw {
        status: 400,
        message: "Each feature must include a feature",
      };
    }
    ensureSupportedFeatureKey(f.feature);
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // Load existing (including soft-deleted) for this customer
    const existing = await db.FeatureEntitlement.findAll({
      where: { customerId },
      paranoid: false,
      transaction: t,
    });

    const existingByKey = new Map();
    for (const row of existing) {
      if (!existingByKey.has(row.feature)) {
        existingByKey.set(row.feature, row);
      }
    }

    for (const { feature, enabled } of features) {
      const current = existingByKey.get(feature);
      const now = new Date();

      if (enabled) {
        if (current) {
          // If it is soft-deleted, restore it; otherwise just update audit/validity
          if (current.deletedAt) {
            await current.restore({ transaction: t });
          }
          current.status = "active";
          current.source = current.source || "boss";
          current.validFrom = now;
          current.validTo = null;
          current.updatedBy = userId || current.updatedBy;
          await current.save({ transaction: t });
        } else {
          await db.FeatureEntitlement.create(
            {
              customerId,
              feature,
              status: "active",
              source: "boss",
              validFrom: now,
              validTo: null,
              createdBy: userId,
              updatedBy: userId,
            },
            { transaction: t }
          );
        }
      } else if (!enabled && current && !current.deletedAt) {
        // Mark as inactive for history, then soft-delete
        const now = new Date();
        current.status = "inactive";
        current.validTo = now;
        current.updatedBy = userId || current.updatedBy;
        await current.save({ transaction: t });
        await current.destroy({ transaction: t });
      }
    }

    await t.commit();

    // Return the updated active entitlements (fresh read)
    return listByCustomer({ customerId });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) await t.rollback();
  }
}

module.exports = {
  listByCustomer,
  setForCustomer,
  SUPPORTED_FEATURE_KEYS,
};
