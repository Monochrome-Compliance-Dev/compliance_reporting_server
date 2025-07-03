function normaliseFakeAbns(record) {
  if (typeof record === "string" && /^x[\dx]*$/i.test(record.trim())) {
    console.warn(`Normalising fake ABN value for record ${record}`);
    record = null;
  }
}

function sanitiseRecordFields(record, fields = []) {
  normaliseFakeAbns(record);
  fields.forEach((field) => {
    if (record[field] === "" || record[field] === " ") {
      record[field] = null;
    } else if (record[field] !== null && record[field] !== undefined) {
      const parsed = parseInt(record[field], 10);
      record[field] = isNaN(parsed) ? null : parsed;
    }
  });
}

function sanitiseNumericMiddleware(fields = []) {
  return function (req, res, next) {
    if (req.body && typeof req.body === "object") {
      const records = Array.isArray(req.body) ? req.body : [req.body];
      for (const record of records) {
        sanitiseRecordFields(record, fields);
      }
    }
    next();
  };
}

module.exports = {
  sanitiseNumericMiddleware,
  sanitiseRecordFields,
  normaliseFakeAbns,
};
