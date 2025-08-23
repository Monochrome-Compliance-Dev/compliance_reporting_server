# Monochrome Compliance Gold Standard (v1)

_Updated: July 2025_

This document defines the architectural gold standard for Monochrome Compliance services.  
All new modules (TCP, ESG, Reports, Users, etc) must adhere to this pattern.

---

## 🎯 Controller Pattern (v2 – August 2025)

This section documents the agreed best-practice controller pattern for Monochrome Compliance services, refining responsibilities, handler structure, error handling, logging, naming, and more.

### Responsibilities Split: Controllers vs Services

- **Controller**:
  - Extracts, normalizes, and validates request parameters.
  - Calls the appropriate service method.
  - Handles all HTTP response formatting and codes.
  - Performs audit logging for all business events.
  - No business or database logic.
- **Service**:
  - Contains all business logic and DB access.
  - Manages transactions.
  - Never sends HTTP responses or logs business audit events.

---

### Standard Handler Shape

Each controller handler follows these steps:

1. Normalize and validate parameters.
2. Call the service and await result.
3. Log audit event if applicable.
4. Send HTTP response using standard envelope.
5. Catch and forward errors.

Example:

```js
async function createThing(req, res, next) {
  try {
    // 1. Normalize & validate
    const params = normalizeCreateThingParams(req);
    validateCreateThing(params);

    // 2. Service call
    const result = await thingService.create(params, req.auth);

    // 3. Audit log
    await auditService.logEvent({
      action: "create",
      entity: "thing",
      entityId: result.id,
      userId: req.auth.userId,
      customerId: req.auth.customerId,
      details: params,
    });

    // 4. Respond
    res.status(201).json({ data: result });
  } catch (err) {
    // 5. Error
    next(err);
  }
}
```

---

### Success Response Codes

| Action | HTTP Status | Envelope        |
| ------ | ----------- | --------------- |
| Read   | 200         | `{ data: ... }` |
| Create | 201         | `{ data: ... }` |
| Update | 200         | `{ data: ... }` |
| Patch  | 200         | `{ data: ... }` |
| Delete | 204         | _No content_    |

---

### Parameter Normalization Example

All parameter extraction/normalization is done in a pure function, outside the handler:

```js
function normalizeCreateThingParams(req) {
  return {
    name: req.body.name?.trim(),
    description: req.body.description?.trim() || null,
    isActive: Boolean(req.body.isActive),
    effectiveDate: req.body.effectiveDate
      ? new Date(req.body.effectiveDate)
      : null,
    customerId: req.auth.customerId,
  };
}
```

---

### Validation Guidelines

- Use a dedicated validation function per endpoint.
- Throw a `ValidationError` (custom error class) with details if invalid.
- Validation is always explicit, never implicit/coerced.
- Example:

```js
function validateCreateThing(params) {
  if (!params.name) throw new ValidationError("Name is required");
  if (params.name.length > 100) throw new ValidationError("Name too long");
  // ...other rules
}
```

---

### Audit Logging Guidelines

- Controllers must log all business events (create, update, delete, login, etc.) using `auditService.logEvent` or equivalent.
- Log after service call, before sending response.
- Include: `action`, `entity`, `entityId`, `userId`, `customerId`, and relevant details.
- Do not log technical errors as audit events (those go to error logs).

---

### Error Handling Guidelines

- Service throws errors (e.g. `NotFoundError`, `ValidationError`, `ForbiddenError`).
- Controller catches errors and calls `next(err)`; global error handler formats response.
- Never leak stack traces or internal error messages to customers.
- Error responses use envelope:
  ```json
  { "error": { "type": "ValidationError", "message": "Name is required" } }
  ```

---

### Response Envelope Rules

- All successful responses are wrapped:  
  `{ data: ... }`
- Error responses wrapped as:  
  `{ error: { type, message } }`
- Never return raw data or arrays at the top level.
- For paginated endpoints:
  ```json
  {
    "data": [ ... ],
    "pagination": {
      "offset": 0,
      "limit": 20,
      "total": 42
    }
  }
  ```

---

### Naming & Actions Conventions

- Controller files: `<entity>Controller.js`
- Service files: `<entity>Service.js`
- Route names:
  - `GET /api/things` (list)
  - `GET /api/things/:id` (read)
  - `POST /api/things` (create)
  - `PATCH /api/things/:id` (update/patch)
  - `DELETE /api/things/:id` (delete)
- Handler names: `listThings`, `getThing`, `createThing`, `patchThing`, `deleteThing`

---

### Pagination & Filtering Baseline

- List endpoints accept `offset`, `limit`, and filter params.
- Default `limit`: 20; max: 100.
- Always return `pagination` object as shown above.
- Filtering params are normalized and validated in controller.

---

### Dates & Casing Rules

- All dates in API are ISO8601 UTC strings.
- All property names are camelCase in JSON and code.
- Never use snake_case or PascalCase in API responses.

---

### Minimal Controller Templates

#### Read (GET by ID)

```js
async function getThing(req, res, next) {
  try {
    const { id } = req.params;
    const customerId = req.auth.customerId;
    const result = await thingService.getById(id, customerId);
    if (!result) throw new NotFoundError("Thing not found");
    res.status(200).json({ data: result });
  } catch (err) {
    next(err);
  }
}
```

#### Create (POST)

```js
async function createThing(req, res, next) {
  try {
    const params = normalizeCreateThingParams(req);
    validateCreateThing(params);
    const result = await thingService.create(params, req.auth);
    await auditService.logEvent({
      action: "create",
      entity: "thing",
      entityId: result.id,
      userId: req.auth.userId,
      customerId: req.auth.customerId,
      details: params,
    });
    res.status(201).json({ data: result });
  } catch (err) {
    next(err);
  }
}
```

#### Patch (PATCH)

```js
async function patchThing(req, res, next) {
  try {
    const { id } = req.params;
    const params = normalizePatchThingParams(req);
    validatePatchThing(params);
    const result = await thingService.patch(id, params, req.auth);
    await auditService.logEvent({
      action: "patch",
      entity: "thing",
      entityId: id,
      userId: req.auth.userId,
      customerId: req.auth.customerId,
      details: params,
    });
    res.status(200).json({ data: result });
  } catch (err) {
    next(err);
  }
}
```

#### Delete (DELETE)

```js
async function deleteThing(req, res, next) {
  try {
    const { id } = req.params;
    await thingService.delete(id, req.auth);
    await auditService.logEvent({
      action: "delete",
      entity: "thing",
      entityId: id,
      userId: req.auth.userId,
      customerId: req.auth.customerId,
    });
    res.status(204).end();
  } catch (err) {
    next(err);
  }
}
```

---

## ✅ Key Principles

### 1. Auth & Customer Context

- JWT must embed `customerId` and `userId`.
- `authorise.js` enforces JWT validity and populates `req.auth`.
- `validateRequest.js` injects `customerId` into each record from `req.auth.customerId` and fails explicitly if missing.

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
  const t = await beginTransactionWithCustomerContext(customerId);
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
