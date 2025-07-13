const db = require("../db/database");
const { scanFile } = require("../middleware/virus-scan");
const {
  beginTransactionWithClientContext,
} = require("../helpers/setClientIdRLS");

module.exports = {
  createFile,
  getFileById,
  getFilesByIndicatorOrMetric,
  deleteFile,
};

async function createFile(fileData, clientId, userId) {
  const transaction = await beginTransactionWithClientContext(clientId);
  try {
    if (!fileData.path) {
      throw { status: 400, message: "No file content to scan." };
    }
    await scanFile(fileData.path, fileData.mimeType, fileData.filename);

    const savedFile = await db.File.create(
      {
        id: fileData.id,
        clientId,
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

async function getFileById(fileId, clientId) {
  const file = await db.File.findOne({
    where: { id: fileId, clientId },
  });
  if (!file) {
    throw { status: 404, message: `File ${fileId} not found` };
  }
  return file.get({ plain: true });
}

async function getFilesByIndicatorOrMetric(
  { indicatorId, metricId },
  clientId
) {
  const where = { clientId };
  if (indicatorId) where.indicatorId = indicatorId;
  if (metricId) where.metricId = metricId;

  return await db.File.findAll({ where });
}

async function deleteFile(fileId, clientId) {
  const transaction = await beginTransactionWithClientContext(clientId);
  try {
    const file = await db.File.findOne({ where: { id: fileId, clientId } });
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
