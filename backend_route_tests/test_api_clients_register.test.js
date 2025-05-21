const request = require('supertest');
const app = require('../../src/app');

describe('Client Registration Endpoint', () => {
  it('should return 200 OK for GET /api/clients/register', async () => {
    const res = await request(app).get('/api/clients/register');
    expect(res.statusCode).toBe(200);
  });
});
