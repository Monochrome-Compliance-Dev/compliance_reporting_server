const db = require("../db/database");

module.exports = {
  createPartner,
  getPartners,
  getPartnerById,
  updatePartner,
  deletePartner,
};

async function createPartner(userId, params) {
  const t = await db.sequelize.transaction();
  try {
    const newPartner = await db.Partner.create(
      {
        ...params,
        createdBy: userId,
        updatedBy: userId,
      },
      { transaction: t }
    );
    await t.commit();
    return newPartner;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getPartners() {
  const t = await db.sequelize.transaction();
  try {
    const partners = await db.Partner.findAll({ transaction: t });
    await t.commit();
    return partners;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getPartnerById(id) {
  const t = await db.sequelize.transaction();
  try {
    const partner = await db.Partner.findByPk(id, { transaction: t });
    await t.commit();
    return partner;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updatePartner(id, params) {
  const t = await db.sequelize.transaction();
  try {
    const [count, [updated]] = await db.Partner.update(params, {
      where: { id },
      returning: true,
      transaction: t,
    });
    await t.commit();
    return updated;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function deletePartner(id) {
  const t = await db.sequelize.transaction();
  try {
    const partner = await db.Partner.findOne({ where: { id }, transaction: t });
    if (partner) {
      await partner.destroy({ transaction: t });
    }
    await t.commit();
    return partner;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}
