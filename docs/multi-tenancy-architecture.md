## 📄 Multi-Tenancy Architecture with Views & Triggers

### 1️⃣ Executive Summary

This document outlines the architecture for strict tenant isolation using MySQL views and triggers. It ensures data belonging to one client cannot be seen or modified by another, while simplifying the application’s data access.

---

### 2️⃣ High-Level Overview

- **Components**:
  - **Base Tables**: Store all client data.
  - **Client-Specific Views**: Filter data by `clientId`.
  - **Triggers**: Enforce `clientId` on all inserts/updates.
- **Goal**: Prevent cross-tenant data access while maintaining performance and simplicity.

---

### 3️⃣ Diagram

_(Insert a diagram showing:_

- Users → Views → Base Tables
- Triggers sitting between views and tables
  )\*

---

### 4️⃣ Key Elements

#### ✅ Views

- **Naming**: `client_{clientId}_{tableName}`
- **Example**:
  ```sql
  CREATE VIEW client_YSPlLRE6ND_tbl_tcp AS
  SELECT * FROM tbl_tcp WHERE clientId = 'YSPlLRE6ND';
  ```
- **Purpose**: Restrict read/write access to only relevant client data.

#### ✅ Triggers

- **Naming**: `trg_client_{clientId}_{tableName}_{event}`
- **`before_insert`**:
  - Ensures `clientId` is always populated from the application’s request.
- **`before_update`**:
  - Prevents any change to `clientId`.
- **Example**:
  ```sql
  CREATE TRIGGER trg_client_YSPlLRE6ND_tbl_tcp_before_insert
  BEFORE INSERT ON tbl_tcp
  FOR EACH ROW
  BEGIN
    IF NEW.clientId IS NULL OR NEW.clientId = '' THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'clientId is required';
    END IF;
  END;
  ```

---

### 5️⃣ Data Flow Examples

#### **SELECT**

- Users query views.
- Views only expose rows with matching `clientId`.

#### **INSERT**

- App sends `INSERT` to the view.
- Trigger injects/validates `clientId` for the final insert.

#### **UPDATE**

- App sends `UPDATE` to the view.
- Trigger ensures `clientId` isn’t changed.

#### **DELETE**

- Users can only delete rows in their view.

---

### 6️⃣ Risks & Caveats

- Views only protect at the DB layer if app only queries through them.
- Triggers must be updated with schema changes.
- Slight performance overhead due to triggers.

---

### 7️⃣ Setup & Maintenance

✅ **Creating Views**

- Managed by `createClientViews(clientId)`.
- Automatically created per client for all relevant tables.

✅ **Creating Triggers**

- Managed by `createClientTriggers(clientId)`.
- Each table’s `INSERT` and `UPDATE` triggers enforce `clientId` presence and immutability.

✅ **Schema Updates**

- Must re-run view/trigger creation if new tables are added or columns change.

---

### 8️⃣ Example Scripts

```sql
-- Create View
CREATE VIEW client_YSPlLRE6ND_tbl_tcp AS
SELECT * FROM tbl_tcp WHERE clientId = 'YSPlLRE6ND';

-- Create Trigger
CREATE TRIGGER trg_client_YSPlLRE6ND_tbl_tcp_before_insert
BEFORE INSERT ON tbl_tcp
FOR EACH ROW
BEGIN
  IF NEW.clientId IS NULL THEN
    SET NEW.clientId = 'YSPlLRE6ND';
  END IF;
END;
```

---

### 9️⃣ Future Considerations

- Evaluate database-level row security policies (like PostgreSQL RLS) if migration possible.
- Monitor performance overhead of triggers for high-transaction tables.

---
