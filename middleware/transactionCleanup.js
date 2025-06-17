// middleware/transactionCleanup.js

module.exports = (req, res, next) => {
  res.on("finish", async () => {
    if (req.dbTransaction && req.dbTransaction._createdByRLS) {
      try {
        await req.dbTransaction.rollback();
        console.warn("Rolled back uncommitted RLS transaction automatically.");
      } catch (err) {
        console.error("Error during automatic RLS transaction rollback:", err);
      }
      req.dbTransaction = null;
    }
  });
  next();
};
