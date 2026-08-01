# Backend Repository Instructions

This repository contains the Monochrome Compliance backend.

The backend owns business rules, persistence, validation, tenant isolation, authorisation, audit behaviour and stable API contracts.

Before making changes, inspect the relevant routes, controllers, services, models, middleware, migrations, utilities and tests.

## Authoritative Documentation

For platform architecture, capability boundaries, domain responsibilities and significant engineering decisions, consult the Platform Handbook.

Do not introduce backend behaviour that conflicts with documented Platform Foundations, accepted ADRs or active initiative architecture.

Implementation details remain authoritative in this repository where they do not conflict with the Platform Handbook.

## Technology and Hosting

- Node.js
- Express 5
- Sequelize 6
- PostgreSQL
- Jest 30
- Elastic Beanstalk

Do not make infrastructure assumptions beyond repository configuration and documented platform architecture.

## Architecture Boundaries

Preserve the established separation between:

- routes
- controllers
- services
- models
- middleware
- utilities

Responsibilities should normally remain:

- Routes: endpoint registration and middleware composition.
- Controllers: HTTP request handling and response delivery.
- Services: business rules and use-case orchestration.
- Models: persistence structures and database relationships.
- Middleware: authentication, authorisation, validation and request concerns.
- Utilities: focused reusable technical helpers.

Do not introduce new architectural layers without a clear and approved reason.

## API Behaviour

- Validate inputs explicitly.
- Return clear and predictable response structures.
- Fail loudly when data is invalid or incomplete.
- Do not silently coerce, discard or conceal invalid states.
- Do not add backwards compatibility unless specifically requested.
- Keep contract changes deliberate and documented.
- Avoid leaking persistence-specific structures where a stable API contract is expected.
- Coordinate frontend contract changes with the frontend API normalisation layer.

## Tenant Isolation and Security

- Maintain `customerId` scoping throughout customer-owned operations.
- Preserve row-level security assumptions and patterns.
- Enforce authentication and authorisation at the appropriate boundaries.
- Do not trust customer, user or ownership identifiers supplied by the client without server-side verification.
- Do not weaken validation, security or audit behaviour for convenience.
- Do not expose secrets, credentials, tokens or sensitive data.
- Keep Security and Identity responsibilities distinct where documented.

## Services and Business Rules

- Business rules belong in services rather than controllers.
- Keep controllers thin.
- Prefer extending an existing service where ownership is clear.
- Avoid duplicate rule implementations across services.
- Use canonical domain concepts and field names.
- Preserve explicit transaction boundaries for multi-step persistence operations.
- Do not hide partial failures behind successful responses.

## Database Changes

- Inspect existing models, migrations, constraints and indexes before changing persistence behaviour.
- Use established Sequelize patterns.
- Keep migrations focused.
- Avoid destructive schema changes unless explicitly approved.
- Preserve data integrity through database constraints where appropriate.
- Do not use broad fallback logic to conceal duplicate or inconsistent data.
- Consider tenant isolation, existing data and rollback implications before changing schema or data shape.

## Logging and Audit

- Use existing Winston logging patterns.
- Preserve structured JSON logging.
- Log meaningful operational, security and audit events.
- Avoid noisy or redundant logs.
- Never log secrets, credentials, tokens or unnecessarily sensitive payloads.
- Include sufficient identifiers for traceability without exposing protected data.
- Do not replace auditable failures with silent handling.

## Errors

- Follow established error-handling conventions.
- Throw or return proper error objects rather than plain objects or ambiguous values.
- Preserve meaningful status codes and machine-readable error details.
- Do not suppress unexpected errors.
- Distinguish validation, authorisation, not-found, conflict and internal failures clearly.

## Code Quality

- Keep changes focused.
- Avoid unrelated refactoring.
- Extend existing services and utilities where sensible.
- Avoid speculative abstractions.
- Do not suppress lint or test failures.
- Do not introduce compatibility or fallback behaviour unless explicitly requested.
- Preserve existing naming and module conventions.
- Use JavaScript files only.

## Testing

- Add or update targeted tests for changed business behaviour.
- Run targeted tests before broader suites where practical.
- Use the repository’s established test commands and patterns.
- Verify failure paths as well as successful behaviour.
- Report exactly what was run.
- Report failures or unverified behaviour clearly.
- Do not claim success when tests were not run.

## Validation

For completed changes:

- Run available repository validation for changed files.
- Do not claim linting was completed unless a working lint command is present or explicitly invoked through an available configuration.
- Run relevant unit and integration tests.
- Run `git diff --check`.
- Verify migrations and model changes where applicable.
- Confirm tenant isolation and authorisation behaviour for affected endpoints.
- Report pre-existing warnings separately from issues introduced by the task.

## Definition of Done

Work is complete only when:

- the requested behaviour is implemented;
- business rules remain in the correct layer;
- API contracts are deliberate and predictable;
- tenant isolation, security and audit behaviour are preserved;
- relevant tests and validation have passed;
- unrelated files remain untouched;
- the Platform Handbook is updated where durable architecture or engineering decisions changed;
- remaining risks or unverified behaviour are clearly reported.

## Repository Commands

- Development server: `npm run start:dev`
- Production-style server: `npm start`
- Unit tests: `npm run test:unit`
- Integration tests: `npm run test:integration`
- General Jest suite: `npm test`

## Platform Transition Boundary

The repository contains both legacy solution code and the new Platform architecture.

The `/platform` area represents the target platform architecture and is conceptually equivalent to a new major platform generation.

- New Platform Capability implementation belongs under `/platform`.
- Code outside `/platform` is predominantly legacy solution implementation.
- Do not use legacy controllers, services, models or route structure as the default architecture for new Platform work.
- Legacy code may be inspected and selectively reused where its behaviour is valid and consistent with approved Platform Capability ownership.
- Reuse must not transfer solution-specific ownership or coupling into Platform Capabilities.
- Do not migrate or rewrite legacy code unless required by the approved capability slice.
- Do not create new Platform behaviour outside `/platform` merely to match an existing legacy implementation.
- Platform Capabilities expose published services and contracts that solutions compose and consume.
- Solutions must not bypass published capability services to manipulate capability-owned persistence directly.
- Where `/platform` implementation and legacy behaviour differ, treat the Platform Handbook and approved `/platform` architecture as the target state.
