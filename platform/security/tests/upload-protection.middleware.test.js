const express = require("express");
const multer = require("multer");
const request = require("supertest");

jest.mock("@/helpers/logger", () => ({
  logger: {
    logEvent: jest.fn(),
  },
}));

const { logger } = require("@/helpers/logger");
const {
  createUploadProtection,
  uploadProtectionErrorHandler,
} = require("@/platform/security/upload-protection.middleware");

function createApp({ maxFileSizeBytes = 1024, maxFieldCount = 10 } = {}) {
  const app = express();

  app.use((request, response, next) => {
    request.id = "request-123";
    next();
  });

  app.post(
    "/upload",
    createUploadProtection({
      storage: multer.memoryStorage(),
      maxFileSizeBytes,
      maxFieldCount,
    }),
    (request, response) => {
      response.status(200).json({
        status: "success",
        fileName: request.file.originalname,
        mimeType: request.file.mimetype,
        size: request.file.size,
      });
    },
  );

  app.use(uploadProtectionErrorHandler);

  return app;
}

describe("upload-protection.middleware", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe("createUploadProtection", () => {
    it("fails loudly when storage is missing", () => {
      expect(() => createUploadProtection()).toThrow(
        "storage is required for upload protection.",
      );
    });

    it("fails loudly when maxFileSizeBytes is invalid", () => {
      expect(() =>
        createUploadProtection({
          storage: multer.memoryStorage(),
          maxFileSizeBytes: 0,
        }),
      ).toThrow(
        "maxFileSizeBytes must be a positive integer for upload protection.",
      );
    });

    it("fails loudly when maxFieldCount is invalid", () => {
      expect(() =>
        createUploadProtection({
          storage: multer.memoryStorage(),
          maxFieldCount: 0,
        }),
      ).toThrow(
        "maxFieldCount must be a positive integer for upload protection.",
      );
    });

    it("accepts a CSV file with an allowed MIME type", async () => {
      const app = createApp();

      const response = await request(app)
        .post("/upload")
        .attach("file", Buffer.from("invoice,amount\nINV-1,100\n"), {
          filename: "payments.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(200);
      expect(response.body).toEqual({
        status: "success",
        fileName: "payments.csv",
        mimeType: "text/csv",
        size: expect.any(Number),
      });
      expect(logger.logEvent).not.toHaveBeenCalled();
    });

    it("rejects a non-CSV filename even when the MIME type is CSV", async () => {
      const app = createApp();

      const response = await request(app)
        .post("/upload")
        .attach("file", Buffer.from("invoice,amount\nINV-1,100\n"), {
          filename: "payments.txt",
          contentType: "text/csv",
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        status: "error",
        code: "INVALID_UPLOAD_TYPE",
        message: "Only CSV files may be uploaded.",
        requestId: "request-123",
      });
    });

    it("rejects a CSV filename with an unapproved MIME type", async () => {
      const app = createApp();

      const response = await request(app)
        .post("/upload")
        .attach("file", Buffer.from("invoice,amount\nINV-1,100\n"), {
          filename: "payments.csv",
          contentType: "application/octet-stream",
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        status: "error",
        code: "INVALID_UPLOAD_TYPE",
        message: "Only CSV files may be uploaded.",
        requestId: "request-123",
      });
    });

    it("rejects files above the configured size limit", async () => {
      const app = createApp({
        maxFileSizeBytes: 100,
      });

      const response = await request(app)
        .post("/upload")
        .attach("file", Buffer.from("x".repeat(500)), {
          filename: "large.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(413);
      expect(response.body).toEqual({
        status: "error",
        code: "UPLOAD_TOO_LARGE",
        message: "Uploaded file exceeds the permitted size.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Upload rejected",
        expect.objectContaining({
          action: "UploadRejected",
          requestId: "request-123",
          method: "POST",
          path: "/upload",
          reason: "LIMIT_FILE_SIZE",
        }),
      );
    });

    it("rejects an unexpected upload field", async () => {
      const app = createApp();

      const response = await request(app)
        .post("/upload")
        .attach("document", Buffer.from("invoice,amount\nINV-1,100\n"), {
          filename: "payments.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        status: "error",
        code: "INVALID_UPLOAD",
        message: "Upload does not meet the permitted file requirements.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Upload rejected",
        expect.objectContaining({
          reason: "LIMIT_UNEXPECTED_FILE",
        }),
      );
    });

    it("rejects uploads with too many non-file fields", async () => {
      const app = createApp({
        maxFieldCount: 2,
      });

      const response = await request(app)
        .post("/upload")
        .field("sourceName", "Payments")
        .field("datasetType", "payment")
        .field("profileId", "profile-123")
        .attach("file", Buffer.from("invoice,amount\nINV-1,100\n"), {
          filename: "payments.csv",
          contentType: "text/csv",
        });

      expect(response.status).toBe(400);
      expect(response.body).toEqual({
        status: "error",
        code: "INVALID_UPLOAD",
        message: "Upload does not meet the permitted file requirements.",
        requestId: "request-123",
      });

      expect(logger.logEvent).toHaveBeenCalledWith(
        "warn",
        "Upload rejected",
        expect.objectContaining({
          reason: "LIMIT_FIELD_COUNT",
        }),
      );
    });
  });

  describe("uploadProtectionErrorHandler", () => {
    it("passes unrelated errors to the next error handler", () => {
      const error = new Error("Unexpected upload failure");
      const request = {
        id: "request-123",
        method: "POST",
        originalUrl: "/upload",
        headers: {},
      };
      const response = {};
      const next = jest.fn();

      uploadProtectionErrorHandler(error, request, response, next);

      expect(next).toHaveBeenCalledWith(error);
      expect(logger.logEvent).not.toHaveBeenCalled();
    });
  });
});
