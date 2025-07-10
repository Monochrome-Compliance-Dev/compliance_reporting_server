## 📄 Multi-Tenancy Architecture with PostgreSQL RLS

### 1️⃣ Executive Summary

This document describes the architecture for strict tenant isolation using PostgreSQL Row-Level Security (RLS) with session-based client context. It ensures data belonging to one client cannot be seen or modified by another, while remaining performant and maintainable.

---

### 2️⃣ High-Level Overview

- **Components**:

  - **Base Tables**: All client data is stored together.
  - **RLS Policies**: Enforce data access at the row level, checking against a session variable.
  - **Session Context**: `SET LOCAL app.current_client_id` for each transaction.

- **Goal**: Prevent cross-tenant data access, enforced by the database itself.

---

### 3️⃣ Key Elements

#### ✅ RLS Policies

- Defined directly on tables, e.g.:

  ```sql
  ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;
  CREATE POLICY client_isolation_policy
    ON tbl_tcp
    USING (clientId = current_setting('app.current_client_id'));
  ```

- Prevents any `SELECT`, `UPDATE`, or `DELETE` from affecting rows outside the client’s scope.

#### ✅ Session Context

- Application explicitly sets the client context for each transaction:
  ```sql
  SET LOCAL app.current_client_id = 'YSPlLRE6ND';
  ```
- Scoped only to the current transaction, ensuring no cross-request leakage.

---

### 4️⃣ Data Flow Examples

#### **SELECT / UPDATE / DELETE**

- App opens transaction.
- Sets `app.current_client_id`.
- Executes queries; RLS ensures rows filtered by client.

#### **INSERT**

- `BEFORE INSERT` triggers or check constraints ensure `clientId` matches `app.current_client_id` or is set automatically.

---

### 5️⃣ Risks & Caveats

- Always require `SET LOCAL` in a transaction. Forgetting to set this means no rows are accessible (safe by design).
- Must ensure all application queries happen inside the same transaction.
- Slight overhead from Postgres checking `current_setting` on each row.

---

### 6️⃣ Setup & Maintenance

✅ **RLS Policies**

- Created by migration scripts or helper functions per table.
- Must add policies whenever a new table with `clientId` is introduced.

✅ **Session Context Management**

- Handled by `beginTransactionWithClientContext(clientId)` helper.
- Rolls back immediately on failure to set context.

✅ **Schema Changes**

- RLS policy must be added to new tables or re-applied if schema is rebuilt.

---

### 7️⃣ Future Considerations

- If high throughput causes RLS to become a bottleneck, evaluate partitioning by `clientId`.
- Continue regular audits of RLS policies to ensure coverage.

---
