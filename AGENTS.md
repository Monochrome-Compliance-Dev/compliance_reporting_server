---

## `/Users/darryllrobinson/Projects/compliance_reporting_server/AGENTS.md`

```markdown
# Backend Repository Instructions

This repository contains the Monochrome Compliance backend.

Before making changes, inspect the relevant routes, controllers, services, models, middleware, utilities and tests.

## Technology and Hosting

- Node.js
- Express
- Sequelize
- PostgreSQL
- Elastic Beanstalk

Do not make infrastructure assumptions beyond what is present in the repository or documented elsewhere.

## Architecture

Preserve the existing separation of responsibilities between:

- routes
- controllers
- services
- models
- middleware
- utilities

Business logic should normally live in services rather than controllers.

Controllers should remain focused on request handling, validation orchestration and response delivery.

Do not introduce new architectural layers without a clear and approved reason.

## Data and API Behaviour

- Validate inputs explicitly.
- Preserve clear and predictable response structures.
- Avoid silently accepting invalid states.
- Prefer explicit failure over hidden fallback behaviour.
- Do not add backwards compatibility unless specifically requested.
- Maintain tenant isolation and existing `customerId` scoping.
- Preserve row-level security assumptions and patterns.
- Do not weaken authentication, authorisation or audit behaviour for convenience.

## Logging and Audit

- Use existing Winston logging patterns.
- Log meaningful operational and audit events.
- Do not add noisy or redundant logs.
- Do not log secrets, credentials, tokens or sensitive data.
- Preserve structured JSON logging where already established.

## Database Changes

- Inspect existing models, migrations and constraints before changing persistence behaviour.
- Do not bypass established Sequelize patterns.
- Avoid destructive schema changes unless explicitly approved.
- Keep migrations focused and reversible where the repository convention supports it.
- Do not conceal duplicate, invalid or inconsistent data through broad fallback logic.

## Code Quality

- Keep changes focused.
- Avoid unrelated refactoring.
- Extend existing services and utilities where sensible.
- Avoid speculative abstractions.
- Do not suppress lint or test failures.
- Preserve existing error-handling conventions unless the task explicitly improves them.

## Testing

After implementation:

- Run the relevant unit tests.
- Run targeted tests before broader suites where practical.
- Report exactly what was run.
- Report failures or unverified behaviour clearly.
- Do not claim success when tests were not run.

## Implementation Approach

For substantial work:

1. Inspect the relevant implementation and documentation.
2. Explain the current behaviour.
3. Propose a concise plan.
4. Wait for approval unless instructed to implement immediately.
5. Make focused changes.
6. Run relevant tests and validation.
7. Summarise the result, decisions and remaining risks.
```
