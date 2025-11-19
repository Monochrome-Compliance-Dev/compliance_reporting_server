const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

/**
 * List all profiles for a customer.
 * RLS is enforced via beginTransactionWithCustomerContext(customerId).
 */
async function listByCustomer({ customerId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const profiles = await db.CustomerProfile.findAll({
      where: { customerId },
      order: [["name", "ASC"]],
      transaction: t,
    });

    await t.commit();

    return profiles.map((p) => p.get({ plain: true }));
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) {
      await t.rollback();
    }
  }
}

/**
 * Create a new profile for a customer.
 */
async function createProfile({ customerId, data, userId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const profile = await db.CustomerProfile.create(
      {
        customerId,
        name: data.name,
        description: data.description ?? null,
        product: data.product,
        createdBy: userId,
        updatedBy: userId,
      },
      { transaction: t }
    );

    await t.commit();

    return profile.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) {
      await t.rollback();
    }
  }
}

/**
 * Update an existing profile for a customer.
 */
async function updateProfile({ customerId, profileId, data, userId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }
  if (!profileId) {
    throw { status: 400, message: "Missing profileId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const profile = await db.CustomerProfile.findOne({
      where: {
        id: profileId,
        customerId,
      },
      transaction: t,
    });

    if (typeof data.name === "string") {
      profile.name = data.name;
    }
    if (typeof data.product === "string") {
      profile.product = data.product;
    }
    if (typeof data.description === "string" || data.description === null) {
      profile.description = data.description;
    }
    profile.updatedBy = userId || profile.updatedBy;

    await profile.save({ transaction: t });
    await t.commit();

    return profile.get({ plain: true });
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) {
      await t.rollback();
    }
  }
}

/**
 * Delete a profile for a customer.
 */
async function deleteProfile({ customerId, profileId, userId }) {
  if (!customerId) {
    throw { status: 400, message: "Missing customerId" };
  }
  if (!profileId) {
    throw { status: 400, message: "Missing profileId" };
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const profile = await db.CustomerProfile.findOne({
      where: {
        id: profileId,
        customerId,
      },
      transaction: t,
    });

    if (!profile) {
      throw { status: 404, message: "Profile not found" };
    }

    profile.updatedBy = userId || profile.updatedBy;
    await profile.save({ transaction: t });
    await profile.destroy({ transaction: t });

    await t.commit();

    return {
      message: "Profile deleted",
    };
  } catch (error) {
    await t.rollback();
    throw { status: error.status || 500, message: error.message || error };
  } finally {
    if (!t.finished) {
      await t.rollback();
    }
  }
}

module.exports = {
  listByCustomer,
  createProfile,
  updateProfile,
  deleteProfile,
};
