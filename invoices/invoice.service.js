const db = require("../db/database");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");
const { Op } = require("sequelize");

module.exports = {
  generateInvoicesForPeriod,
  getInvoiceById,
  updateInvoice,
  deleteInvoice,
  generatePartnerInvoicesForPeriod,
  getInvoicesByScope,
};

async function generateInvoicesForPeriod(reportingPeriodId, userId, clientId) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const allClients = await db.Client.findAll({
      where: { id: clientId },
      transaction: t,
    });
    // console.log("allClients: ", allClients);
    const platformProduct = await db.Product.findOne({
      where: { code: "PLATFORM", active: true },
      transaction: t,
    });
    // console.log("platformProduct: ", platformProduct);
    const moduleProducts = await db.Product.findAll({
      where: { type: "module", active: true },
      transaction: t,
    });
    // console.log("moduleProducts: ", moduleProducts);

    const invoices = [];

    for (const client of allClients) {
      const billingType = client.billingType;
      //   console.log("billingType: ", billingType);
      const groupKey = billingType === "DIRECT" ? client.id : client.partnerId;
      //   console.log("groupKey: ", groupKey);
      const existingInvoice = invoices.find((inv) => inv.groupKey === groupKey);
      //   console.log("existingInvoice: ", existingInvoice);

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
      //   console.log("lineItems: ", lineItems);

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
          clientId: client.id,
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
    // console.log("invoices: ", invoices);

    const savedInvoices = [];

    for (const data of invoices) {
      const { lineItems, ...invoiceData } = data;
      //   console.log("invoiceData: ", invoiceData);
      const invoice = await db.Invoice.create(invoiceData, { transaction: t });
      //   console.log("invoice: ", invoice);
      for (const item of lineItems) {
        const product = await db.Product.findByPk(item.productId, {
          transaction: t,
        });

        await db.InvoiceLine.create(
          {
            ...item,
            invoiceId: invoice.id,
            module: product?.module, // Pull module from tbl_product
            createdBy: userId,
            updatedBy: userId,
          },
          { transaction: t }
        );
      }
      savedInvoices.push(invoice);
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

async function generatePartnerInvoicesForPeriod(
  reportingPeriodId,
  userId,
  clientId
) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const partners = await db.Partner.findAll({ transaction: t });
    const moduleProducts = await db.Product.findAll({
      where: { type: "module", active: true },
      transaction: t,
    });

    const invoices = [];

    for (const partner of partners) {
      const clients = await db.Client.findAll({
        where: { partnerId: partner.id },
        transaction: t,
      });

      let lineItems = [];

      for (const client of clients) {
        for (const product of moduleProducts) {
          lineItems.push({
            description: `${product.name} – ${reportingPeriodId} (Client: ${client.name})`,
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

      if (lineItems.length > 0) {
        invoices.push({
          partnerId: partner.id,
          clientId,
          billingType: "PARTNER",
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
      const invoice = await db.Invoice.create(
        { ...invoiceData, updatedBy: userId },
        { transaction: t }
      );
      for (const item of lineItems) {
        await db.InvoiceLine.create(
          { ...item, invoiceId: invoice.id, updatedBy: userId },
          { transaction: t }
        );
      }
      savedInvoices.push(invoice);
    }

    await t.commit();
    return savedInvoices;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}

async function getInvoicesByScope({ clientId, partnerId }) {
  const t = await beginTransactionWithClientContext(clientId);
  try {
    const where = {};

    if (partnerId) {
      where.partnerId = partnerId;
    } else if (clientId) {
      where.clientId = clientId;
    }

    const invoices = await db.Invoice.findAll({
      where,
      transaction: t,
    });

    const invoiceIds = invoices.map((inv) => inv.id);

    const lines = await db.InvoiceLine.findAll({
      where: {
        invoiceId: { [Op.in]: invoiceIds },
      },
      transaction: t,
    });

    const linesByInvoice = lines.reduce((acc, line) => {
      if (!acc[line.invoiceId]) acc[line.invoiceId] = [];
      acc[line.invoiceId].push(line.get({ plain: true }));
      return acc;
    }, {});

    const result = invoices.map((inv) => {
      const invoice = inv.get({ plain: true });
      invoice.lineItems = linesByInvoice[invoice.id] || [];
      return invoice;
    });

    await t.commit();
    return result;
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  }
}
