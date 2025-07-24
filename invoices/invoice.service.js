const db = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

module.exports = {
  generateInvoicesForPeriod,
  getInvoiceById,
  updateInvoice,
  deleteInvoice,
};

async function generateInvoicesForPeriod(reportingPeriodId, userId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const allClients = await db.Client.findAll({ transaction: t });
    const platformProduct = await db.Product.findOne({
      where: { code: "PLATFORM", active: true },
      transaction: t,
    });
    const moduleProducts = await db.Product.findAll({
      where: { type: "module", active: true },
      transaction: t,
    });

    const invoices = [];

    for (const client of allClients) {
      const billingType = client.billingType;
      const groupKey = billingType === "DIRECT" ? client.id : client.partnerId;
      const existingInvoice = invoices.find((inv) => inv.groupKey === groupKey);

      let lineItems = [];

      if (billingType === "DIRECT") {
        if (!platformProduct) throw new Error("Platform product not found");
        lineItems.push({
          description: `Compliance platform subscription – ${reportingPeriodId}`,
          productId: platformProduct.id,
          amount: platformProduct.amount,
          module: "solution",
          relatedRecordId: null,
        });
      } else {
        for (const product of moduleProducts) {
          lineItems.push({
            description: `${product.name} – ${reportingPeriodId}`,
            productId: product.id,
            amount: product.amount,
            module: product.code.toLowerCase(),
            relatedRecordId: client.id,
          });
        }
      }

      const totalAmount = lineItems.reduce(
        (sum, li) => sum + parseFloat(li.amount),
        0
      );

      if (existingInvoice) {
        existingInvoice.lineItems.push(...lineItems);
        existingInvoice.totalAmount += totalAmount;
      } else {
        invoices.push({
          groupKey,
          billingType,
          clientId: billingType === "DIRECT" ? client.id : null,
          partnerId: billingType === "PARTNER" ? client.partnerId : null,
          reportingPeriodId,
          issuedAt: new Date(),
          totalAmount,
          status: "draft",
          createdBy: userId,
          lineItems,
        });
      }
    }

    const savedInvoices = [];

    for (const data of invoices) {
      const { lineItems, ...invoiceData } = data;
      const invoice = await db.Invoice.create(invoiceData, { transaction: t });
      for (const item of lineItems) {
        await db.InvoiceLine.create(
          { ...item, invoiceId: invoice.id },
          { transaction: t }
        );
      }
      savedInvoices.push(invoice.get({ plain: true }));
    }

    await t.commit();
    return savedInvoices;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getInvoiceById(id, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const invoice = await db.Invoice.findByPk(id, {
      include: [{ model: db.InvoiceLine }],
      transaction: t,
    });
    await t.commit();
    return invoice;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function updateInvoice(id, clientId, data) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const [count, [updated]] = await db.Invoice.update(data, {
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

async function deleteInvoice(id, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const invoice = await db.Invoice.findByPk(id, { transaction: t });
    if (invoice) {
      await invoice.destroy({ transaction: t });
    }
    await t.commit();
    return invoice;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}
