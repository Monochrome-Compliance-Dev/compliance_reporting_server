const request = require("supertest");
const app = require("../../server");

describe("POST /api/entities", () => {
  it("should save an eligible entity with required data", async () => {
    const res = await request(app).post("/api/entities").send({
      entityName: "Example Holdings Pty Ltd",
      entityABN: "98765432109",
      revenue: "over-100m",
      completed: true,
    });

    expect(res.statusCode).toBe(201);
    expect(res.body.message).toBe("Entity created");
  });

  it("should fail when entityName is missing", async () => {
    const res = await request(app).post("/api/entities").send({
      revenue: "under-100m",
    });

    expect(res.statusCode).toBe(400); // Joi schema validation fail
  });
});
