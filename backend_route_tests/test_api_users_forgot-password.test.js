const request = require('supertest');
const app = require('../../src/app');

describe('Forgot Password', () => {
  it('should return 200 OK for GET /api/users/forgot-password', async () => {
    const res = await request(app).get('/api/users/forgot-password');
    expect(res.statusCode).toBe(200);
  });
});
