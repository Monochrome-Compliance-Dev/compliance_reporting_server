const db = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

module.exports = {
  createProduct,
  getProducts,
  getProductById,
  updateProduct,
  deleteProduct,
};

async function createProduct(data) {
  const t = await db.sequelize.transaction();
  try {
    const created = await db.Product.create(data, { transaction: t });
    await t.commit();
    return created;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getProducts() {
  const t = await db.sequelize.transaction();
  try {
    const products = await db.Product.findAll({ transaction: t });
    await t.commit();
    return products;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getProductById(id) {
  const t = await db.sequelize.transaction();
  try {
    const product = await db.Product.findByPk(id, { transaction: t });
    await t.commit();
    return product;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateProduct(id, data) {
  const t = await db.sequelize.transaction();
  try {
    const [count, [updated]] = await db.Product.update(data, {
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

async function deleteProduct(id) {
  const t = await db.sequelize.transaction();
  try {
    const product = await db.Product.findByPk(id, { transaction: t });
    if (product) {
      await product.destroy({ transaction: t });
    }
    await t.commit();
    return product;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}
