const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const { QueryTypes, Op } = require("sequelize");

const {
  normaliseKeyword,
  normaliseKeywordField,
  normaliseKeywordMatchType,
} = require("./exclusions.shared");

function getKeywordModel(sequelize) {
  return sequelize?.models?.PtrsExclusionKeywordCustomerRef;
}

async function listKeywordExclusions({ customerId, profileId }) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const rows = await sequelize.query(
      `
        SELECT
          k."id",
          k."keyword",
          k."field",
          k."matchType",
          k."notes",
          k."createdAt",
          k."updatedAt"
        FROM "tbl_ptrs_exclusion_keyword_customer_ref" k
        WHERE
          k."customerId" = :customerId
          AND k."profileId" = :profileId
          AND k."deletedAt" IS NULL
        ORDER BY lower(k."keyword") ASC
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { customerId, profileId },
        transaction: t,
      },
    );

    await t.commit();
    return Array.isArray(rows) ? rows : [];
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function createKeywordExclusion({
  customerId,
  profileId,
  keyword,
  field,
  matchType,
  notes,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keyword) throw new Error("keyword is required");
  if (!field) throw new Error("field is required");
  if (!matchType) throw new Error("matchType is required");

  const cleaned = normaliseKeyword(keyword);
  if (!cleaned) throw new Error("keyword is required");
  if (cleaned.length > 200) throw new Error("keyword is too long (max 200)");

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const Model = getKeywordModel(sequelize);
    if (!Model) {
      throw new Error(
        "Exclusion keyword model not registered in Sequelize: PtrsExclusionKeywordCustomerRef",
      );
    }

    const cleanedKeyword = cleaned;
    const normalisedField = normaliseKeywordField(field);
    const normalisedMatchType = normaliseKeywordMatchType(matchType);

    if (!normalisedField) throw new Error("Invalid keyword field");
    if (!normalisedMatchType) throw new Error("Invalid keyword matchType");

    const trimmedUserId = userId ? String(userId).slice(0, 10) : null;

    const existing = await Model.findOne({
      where: {
        customerId,
        profileId,
        [Op.and]: [
          sequelize.where(
            sequelize.fn("lower", sequelize.col("keyword")),
            sequelize.fn("lower", cleanedKeyword),
          ),
        ],
      },
      transaction: t,
      paranoid: false,
    });

    if (existing) {
      existing.keyword = cleanedKeyword;
      existing.field = normalisedField;
      existing.matchType = normalisedMatchType;
      existing.notes = notes ?? null;
      existing.deletedAt = null;
      existing.updatedBy = trimmedUserId;

      await existing.save({ transaction: t });
      await t.commit();

      return {
        id: existing.id,
        keyword: existing.keyword,
        field: existing.field,
        matchType: existing.matchType,
        notes: existing.notes,
        createdAt: existing.createdAt,
        updatedAt: existing.updatedAt,
      };
    }

    const created = await Model.create(
      {
        customerId,
        profileId,
        keyword: cleanedKeyword,
        field: normalisedField,
        matchType: normalisedMatchType,
        notes: notes ?? null,
        createdBy: trimmedUserId,
        updatedBy: trimmedUserId,
      },
      { transaction: t },
    );

    await t.commit();

    return {
      id: created.id,
      keyword: created.keyword,
      field: created.field,
      matchType: created.matchType,
      notes: created.notes,
      createdAt: created.createdAt,
      updatedAt: created.updatedAt,
    };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function updateKeywordExclusion({
  customerId,
  profileId,
  keywordId,
  keyword,
  field,
  matchType,
  notes = null,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keywordId) throw new Error("keywordId is required");
  if (!field) throw new Error("field is required");
  if (!matchType) throw new Error("matchType is required");

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const Model = getKeywordModel(sequelize);
  if (!Model) {
    throw new Error(
      "Keyword model not registered in Sequelize: PtrsExclusionKeywordCustomerRef",
    );
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await Model.findOne({
      where: {
        id: keywordId,
        customerId,
        profileId,
        deletedAt: null,
      },
      transaction: t,
    });

    if (!row) {
      const err = new Error("Keyword not found");
      err.statusCode = 404;
      throw err;
    }

    row.keyword = normaliseKeyword(keyword);
    if (!row.keyword) throw new Error("keyword is required");

    const normalisedField = normaliseKeywordField(field);
    const normalisedMatchType = normaliseKeywordMatchType(matchType);

    if (!normalisedField) throw new Error("Invalid keyword field");
    if (!normalisedMatchType) throw new Error("Invalid keyword matchType");

    row.field = normalisedField;
    row.matchType = normalisedMatchType;
    row.notes = notes ?? null;
    if (userId) row.updatedBy = String(userId).slice(0, 10);

    await row.save({ transaction: t });
    await t.commit();

    return typeof row.toJSON === "function" ? row.toJSON() : row;
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

async function deleteKeywordExclusion({
  customerId,
  profileId,
  keywordId,
  userId,
}) {
  if (!customerId) throw new Error("customerId is required");
  if (!profileId) throw new Error("profileId is required");
  if (!keywordId) throw new Error("keywordId is required");

  const sequelize = db?.sequelize;
  if (!sequelize) {
    throw new Error("Database not initialised: db.sequelize missing");
  }

  const t = await beginTransactionWithCustomerContext(customerId);

  try {
    const row = await sequelize.query(
      `
        UPDATE "tbl_ptrs_exclusion_keyword_customer_ref"
        SET
          "deletedAt" = now(),
          "updatedAt" = now(),
          "updatedBy" = :userId
        WHERE
          "id" = :keywordId
          AND "customerId" = :customerId
          AND "profileId" = :profileId
          AND "deletedAt" IS NULL
        RETURNING "id"
      `,
      {
        type: QueryTypes.SELECT,
        replacements: { keywordId, customerId, profileId, userId },
        transaction: t,
      },
    );

    await t.commit();
    return { deleted: true, id: row?.[0]?.id || keywordId };
  } catch (err) {
    if (!t.finished) {
      try {
        await t.rollback();
      } catch (_) {}
    }
    throw err;
  }
}

module.exports = {
  listKeywordExclusions,
  createKeywordExclusion,
  updateKeywordExclusion,
  deleteKeywordExclusion,
};
