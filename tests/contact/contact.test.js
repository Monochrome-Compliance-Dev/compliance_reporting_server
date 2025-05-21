const request = require("supertest");
const app = require("../../server");

describe("POST /api/contact", () => {
  it("should submit a contact form successfully", async () => {
    const res = await request(app).post("/api/contact").send({
      name: "Jane Doe",
      email: "jane@example.com",
      message: "Hi there! This is a test.",
    });

    expect(res.statusCode).toBe(200);
    expect(res.body.success).toBe(true);
  });
});
