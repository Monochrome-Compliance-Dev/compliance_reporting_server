const request = require('supertest');
const app = require('../../src/app');

describe('Privacy Policy Page', () => {
  it('should return 200 OK for GET /api/policies/privacy-policy', async () => {
    const res = await request(app).get('/api/policies/privacy-policy');
    expect(res.statusCode).toBe(200);
  });
});
