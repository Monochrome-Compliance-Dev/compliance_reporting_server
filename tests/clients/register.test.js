const request = require("supertest");
const app = require("../../server");

describe("POST /api/clients/register", () => {
  it("should register a client successfully", async () => {
    const res = await request(app).post("/api/clients/register").send({
      businessName: "Demo Pty Ltd",
      abn: "12345678901",
      contactName: "Test Person",
      email: "test@demo.com",
      phone: "0411222333",
    });

    expect(res.statusCode).toBe(201); // Adjust if you return 200 or 204 instead
    expect(res.body.success).toBe(true);
    expect(res.body.clientId).toBeDefined();
  });
});
