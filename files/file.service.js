const db = require("../db/database");
const { scanFile } = require("../middleware/virus-scan");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");

module.exports = {
  createFile,
  getFileById,
  getFilesByIndicatorOrMetric,
  deleteFile,
};

async function createFile(fileData, customerId, userId) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    if (!fileData.path) {
      throw { status: 400, message: "No file content to scan." };
    }
    await scanFile(fileData.path, fileData.mimeType, fileData.filename);

    const savedFile = await db.File.create(
      {
        id: fileData.id,
        customerId,
        indicatorId: fileData.indicatorId || null,
        metricId: fileData.metricId || null,
        filename: fileData.filename,
        storagePath: fileData.storagePath,
        mimeType: fileData.mimeType,
        fileSize: fileData.fileSize,
        uploadedBy: userId,
      },
      { transaction }
    );

    await transaction.commit();
    return savedFile.get({ plain: true });
  } catch (err) {
    if (!transaction.finished) await transaction.rollback();
    throw err;
  }
}

async function getFileById(fileId, customerId) {
  const file = await db.File.findOne({
    where: { id: fileId, customerId },
  });
  if (!file) {
    throw { status: 404, message: `File ${fileId} not found` };
  }
  return file.get({ plain: true });
}

async function getFilesByIndicatorOrMetric(
  { indicatorId, metricId },
  customerId
) {
  const where = { customerId };
  if (indicatorId) where.indicatorId = indicatorId;
  if (metricId) where.metricId = metricId;

  return await db.File.findAll({ where });
}

async function deleteFile(fileId, customerId) {
  const transaction = await beginTransactionWithCustomerContext(customerId);
  try {
    const file = await db.File.findOne({ where: { id: fileId, customerId } });
    if (!file) {
      throw { status: 404, message: `File ${fileId} not found` };
    }
    await file.destroy({ transaction });
    await transaction.commit();
    return true;
  } catch (err) {
    if (!transaction.finished) await transaction.rollback();
    throw err;
  }
}
