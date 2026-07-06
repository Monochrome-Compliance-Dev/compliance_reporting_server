const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");

async function withCustomerTransaction(customerId, work) {
  if (!customerId) {
    throw new Error("customerId is required for customer transaction.");
  }

  if (typeof work !== "function") {
    throw new Error("work function is required for customer transaction.");
  }

  const transaction = await beginTransactionWithCustomerContext(customerId);

  try {
    const result = await work(transaction);
    await transaction.commit();
    return result;
  } catch (error) {
    if (!transaction.finished) {
      await transaction.rollback();
    }
    throw error;
  }
}

module.exports = {
  withCustomerTransaction,
};
