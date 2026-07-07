const fs = require("fs");

function createError(message, status = 400) {
  const error = new Error(message);
  error.status = status;
  return error;
}

function requireBuffer(buffer) {
  if (!Buffer.isBuffer(buffer)) {
    throw createError("CSV buffer is required for dataset creation.");
  }

  if (buffer.length === 0) {
    throw createError("CSV buffer must not be empty.");
  }
}

function requireFilePath(filePath) {
  if (!filePath) {
    throw createError("CSV file path is required for dataset creation.");
  }
}

function normaliseCsvText(buffer) {
  return buffer
    .toString("utf8")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
}

function splitCsvLine(line) {
  const values = [];
  let currentValue = "";
  let insideQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const character = line[index];
    const nextCharacter = line[index + 1];

    if (character === '"' && insideQuotes && nextCharacter === '"') {
      currentValue += '"';
      index += 1;
    } else if (character === '"') {
      insideQuotes = !insideQuotes;
    } else if (character === "," && !insideQuotes) {
      values.push(currentValue.trim());
      currentValue = "";
    } else {
      currentValue += character;
    }
  }

  values.push(currentValue.trim());

  return values;
}

function getContentLines(text) {
  return text.split("\n").filter((line, index, lines) => {
    if (line.trim() !== "") {
      return true;
    }

    return lines
      .slice(0, index)
      .some((previousLine) => previousLine.trim() !== "");
  });
}

function getHeaderLine(lines) {
  const headerLine = lines.find((line) => line.trim() !== "");

  if (!headerLine) {
    throw createError("CSV appears to have no header row.");
  }

  return headerLine;
}

function buildUniqueHeaders(rawHeaders) {
  if (!rawHeaders.length || rawHeaders.every((header) => header === "")) {
    throw createError("CSV appears to have no header row.");
  }

  const seen = new Map();

  return rawHeaders.map((rawHeader, index) => {
    const baseHeader = rawHeader || `column_${index + 1}`;
    const occurrence = (seen.get(baseHeader) || 0) + 1;

    seen.set(baseHeader, occurrence);

    return occurrence === 1 ? baseHeader : `${baseHeader}_${occurrence}`;
  });
}

function countDataRows(lines, headerLine) {
  const headerIndex = lines.indexOf(headerLine);

  return lines.slice(headerIndex + 1).filter((line) => line.trim() !== "")
    .length;
}

function inspectCsvBuffer(buffer) {
  requireBuffer(buffer);

  const text = normaliseCsvText(buffer);
  const lines = getContentLines(text);
  const headerLine = getHeaderLine(lines);
  const rawHeaders = splitCsvLine(headerLine);
  const headers = buildUniqueHeaders(rawHeaders);
  const rowsCount = countDataRows(lines, headerLine);

  return {
    headers,
    headersCount: headers.length,
    rowsCount,
  };
}

function inspectCsvFile(filePath) {
  requireFilePath(filePath);

  const buffer = fs.readFileSync(filePath);
  return inspectCsvBuffer(buffer);
}

module.exports = {
  inspectCsvBuffer,
  inspectCsvFile,
};
