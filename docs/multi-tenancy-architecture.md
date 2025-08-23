## 📄 Multi-Tenancy Architecture with PostgreSQL RLS

### 1️⃣ Executive Summary

This document describes the architecture for strict tenant isolation using PostgreSQL Row-Level Security (RLS) with session-based customer context. It ensures data belonging to one customer cannot be seen or modified by another, while remaining performant and maintainable.

---

### 2️⃣ High-Level Overview

- **Components**:

  - **Base Tables**: All customer data is stored together.
  - **RLS Policies**: Enforce data access at the row level, checking against a session variable.
  - **Session Context**: `SET LOCAL app.current_customer_id` for each transaction.

- **Goal**: Prevent cross-tenant data access, enforced by the database itself.

---

### 3️⃣ Key Elements

#### ✅ RLS Policies

- Defined directly on tables, e.g.:

  ```sql
  ALTER TABLE tbl_tcp ENABLE ROW LEVEL SECURITY;
  CREATE POLICY customer_isolation_policy
    ON tbl_tcp
    USING (customerId = current_setting('app.current_customer_id'));
  ```

- Prevents any `SELECT`, `UPDATE`, or `DELETE` from affecting rows outside the customer’s scope.

#### ✅ Session Context

- Application explicitly sets the customer context for each transaction:
  ```sql
  SET LOCAL app.current_customer_id = 'YSPlLRE6ND';
  ```
- Scoped only to the current transaction, ensuring no cross-request leakage.

---

### 4️⃣ Data Flow Examples

#### **SELECT / UPDATE / DELETE**

- App opens transaction.
- Sets `app.current_customer_id`.
- Executes queries; RLS ensures rows filtered by customer.

#### **INSERT**

- `BEFORE INSERT` triggers or check constraints ensure `customerId` matches `app.current_customer_id` or is set automatically.

---

### 5️⃣ Risks & Caveats

- Always require `SET LOCAL` in a transaction. Forgetting to set this means no rows are accessible (safe by design).
- Must ensure all application queries happen inside the same transaction.
- Slight overhead from Postgres checking `current_setting` on each row.

---

### 6️⃣ Setup & Maintenance

✅ **RLS Policies**

- Created by migration scripts or helper functions per table.
- Must add policies whenever a new table with `customerId` is introduced.

✅ **Session Context Management**

- Handled by `beginTransactionWithCustomerContext(customerId)` helper.
- Rolls back immediately on failure to set context.

✅ **Schema Changes**

- RLS policy must be added to new tables or re-applied if schema is rebuilt.

---

### 7️⃣ Future Considerations

- If high throughput causes RLS to become a bottleneck, evaluate partitioning by `customerId`.
- Continue regular audits of RLS policies to ensure coverage.

---
