const fs = require("fs");
const express = require("express");
const request = require("supertest");

const {
  cleanupUploadedFile,
  uploadCsv,
} = require("@/v2/ptrs/middleware/csv-upload.ptrs.middleware");

describe("PTRS CSV upload middleware", () => {
  test("stores multipart source bytes on disk rather than req.file.buffer", async () => {
    const app = express();
    app.post("/upload", uploadCsv.single("file"), async (req, res, next) => {
      try {
        const exists = await fs.promises
          .stat(req.file.path)
          .then((stat) => stat.isFile());
        res.json({
          exists,
          hasBuffer: Object.prototype.hasOwnProperty.call(req.file, "buffer"),
          size: req.file.size,
        });
      } catch (error) {
        next(error);
      } finally {
        await cleanupUploadedFile(req.file);
      }
    });

    const response = await request(app)
      .post("/upload")
      .attach("file", Buffer.from("a,b\n1,2\n"), "input.csv")
      .expect(200);

    expect(response.body).toEqual({ exists: true, hasBuffer: false, size: 8 });
  });
});
