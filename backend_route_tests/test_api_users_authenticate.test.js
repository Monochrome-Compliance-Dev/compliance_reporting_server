const request = require('supertest');
const app = require('../../src/app');

describe('Login Endpoint', () => {
  it('should return 200 OK for GET /api/users/authenticate', async () => {
    const res = await request(app).get('/api/users/authenticate');
    expect(res.statusCode).toBe(200);
  });
});
