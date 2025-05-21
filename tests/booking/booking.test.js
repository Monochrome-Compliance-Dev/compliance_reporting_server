// tests/booking/booking.test.js
const request = require("supertest");
const app = require("../../server");

describe("POST /api/booking", () => {
  it("should create a new booking with valid data", async () => {
    const res = await request(app).post("/api/booking").send({
      name: "Alice Booking",
      email: "alice@example.com",
      date: "2025-06-01",
      time: "10:30",
      reason: "Strategy meeting",
    });

    expect(res.statusCode).toBe(200);
    expect(res.body).toHaveProperty("id");
    expect(res.body.name).toBe("Alice Booking");
  });

  it("should reject booking with missing fields", async () => {
    const res = await request(app).post("/api/booking").send({
      name: "Missing Email",
    });

    expect(res.statusCode).toBe(400); // due to validateRequest middleware
  });
});
