# Monochrome Compliance Gold Standard (v1)

_Updated: July 2025_

This document defines the architectural gold standard for Monochrome Compliance services.  
All new modules (TCP, ESG, Reports, Users, etc) must adhere to this pattern.

---

## ✅ Key Principles

### 1. Auth & Client Context

- JWT must embed `clientId` and `userId`.
- `authorise.js` enforces JWT validity and populates `req.auth`.
- `validateRequest.js` injects `clientId` into each record from `req.auth.clientId` and fails explicitly if missing.

### 2. Controller

- Only handles:
  - Extracting parameters
  - Calling the service
  - Sending HTTP responses
- No DB or business logic.
- Always logs to `auditService`.

### 3. Service Layer

- Opens transaction using:
  ```js
  const t = await beginTransactionWithClientContext(clientId);
  ```
- Always structured:
  ```js
  try {
    await doThing({ transaction: t });
    await t.commit();
  } catch (err) {
    if (!t.finished) await t.rollback();
    throw err;
  } finally {
    if (!t.finished) await t.rollback();
  }
  ```
- Guarantees all writes are within transaction + RLS scope.

### 4. Database

- Max pool: `20` connections.
- Graceful SIGTERM shutdown closes pool.
- All reads/writes require explicit `transaction`.
- Never use global `SET` outside transaction.

### 5. Logging & Auditing

- All business audit events (create, update, delete, login, etc.) are logged exclusively in the controller using `auditService` or `logger.logEvent`.
- The service layer does not perform audit logging of successful operations; it only manages DB work and may log technical errors or security warnings if needed.

### 6. Security

- Helmet, CSP, HSTS.
- Rate limiters on all `/api` and aggressive limits on auth endpoints.
- CORS only allows whitelisted domains.

### 7. Errors

- Use consistent error structures, thrown from services, caught by `errorHandler`.

---

## 🚀 Usage

- Copy ESG route structure for new modules.
- Run all tests + `health-check` before pushing to main.
- Update this document with new patterns as architecture evolves.

---

_Monochrome Compliance Engineering_
