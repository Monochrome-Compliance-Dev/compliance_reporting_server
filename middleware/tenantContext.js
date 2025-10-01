// middleware/tenantContext.js
// Resolves the effective tenant (customer) for this request based on:
// - Logged-in user's home customerId (from DB / JWT)
// - Optional X-Customer-Id header (acting-as), validated via CustomerAccess mapping
// Optionally loads entitlements for the effective tenant and attaches helpers to req.
//
// EXPECTED ORDER: run AFTER your auth/JWT middleware so req.auth.id is present.

const db = require("../db/database");
const { Op } = require("sequelize");
const {
  getEntitlements: getCustomerEntitlements,
} = require("../customers/customer.service");

const DEFAULT_ELEVATED = ["Boss"];
const ACTIVE_ENT_STATES = ["active", "trial", "grace"];

/**
 * Tenant resolver middleware.
 *
 * @param {Object} opts
 * @param {boolean} [opts.loadEntitlements=true] - if true, loads tenant-scoped entitlements into req.entitlements
 * @param {boolean} [opts.enforceMapping=true]   - if true, requires a CustomerAccess mapping to act-as
 * @param {string[]} [opts.elevatedRoles]        - roles allowed to act-as (default Boss/Owner/Admin/Superadmin)
 * @returns {(req,res,next)=>Promise<void>}
 */
function tenantContext(opts = {}) {
  const {
    loadEntitlements = true,
    enforceMapping = true,
    elevatedRoles = DEFAULT_ELEVATED,
  } = opts;

  return async function resolveTenant(req, res, next) {
    try {
      // Ensure user is available (authorise middleware typically sets req.auth.id)
      let user = req.user;
      if (!user) {
        if (!req.auth?.id) {
          return res.status(401).json({ message: "Unauthorised" });
        }
        user = await db.User.findByPk(req.auth.id);
        if (!user) {
          return res.status(401).json({ message: "Unauthorised" });
        }
        req.user = user; // cache for downstream handlers
      }

      // Determine requested tenant from header or route param (customers/:customerId/...)
      const headerIdRaw = req.get("x-customer-id");
      //   console.log("headerIdRaw: ", headerIdRaw);
      const headerId = headerIdRaw ? String(headerIdRaw).trim() : "";
      //   console.log("headerId: ", headerId);
      const paramIdRaw =
        req.params && req.params.customerId ? req.params.customerId : "";
      //   console.log("paramIdRaw: ", paramIdRaw);
      const paramId = paramIdRaw ? String(paramIdRaw).trim() : "";
      //   console.log("paramId: ", paramId);

      // If both paramId and headerId exist and differ, respond with 400 error
      if (paramId && headerId && paramId !== headerId) {
        return res.status(400).json({
          status: "bad_request",
          reason: "conflicting_tenant_ids",
          message:
            "Conflicting tenant IDs provided in URL parameter and header.",
        });
      }

      // Prefer paramId when present; otherwise consider headerId
      const requestedId = paramId || headerId || "";
      //   console.log("requestedId: ", requestedId);
      const wantsActingAs = !!requestedId && requestedId !== user.customerId;
      //   console.log("wantsActingAs: ", wantsActingAs);

      // Determine if user has an elevated role
      const baseRole = (
        typeof user.role === "string" ? user.role : user.role?.name || ""
      ).toString();
      const isElevated = elevatedRoles
        .map((r) => r.toLowerCase())
        .includes(baseRole.toLowerCase());

      // Validate acting-as via mapping if a non-home tenant is requested
      let actingAccess = null;
      if (wantsActingAs) {
        if (!isElevated) {
          return res.status(403).json({
            status: "forbidden",
            reason: "insufficient_role",
            message:
              "You don’t have the required role to act on behalf of another customer.",
          });
        }
        if (enforceMapping) {
          actingAccess = await db.CustomerAccess.findOne({
            where: { userId: user.id, customerId: requestedId },
          });
          if (!actingAccess) {
            return res.status(403).json({
              status: "forbidden",
              reason: "no_customer_access",
              message:
                "You don’t have access to the requested customer. Please contact your administrator.",
            });
          }
        }
      }

      const effectiveCustomerId = wantsActingAs ? requestedId : user.customerId;
      // Fail fast if we could not determine an effective tenant
      if (!effectiveCustomerId) {
        return res.status(400).json({
          status: "bad_request",
          reason: "missing_customer_context",
          message:
            "Tenant context could not be determined. Please re-select a customer or contact support.",
        });
      }

      // Attach effective tenant context
      req.tenantCustomerId = effectiveCustomerId;
      req.effectiveCustomerId = effectiveCustomerId; // alias
      req.actingAs = wantsActingAs ? effectiveCustomerId : null;
      req.actingRole = wantsActingAs
        ? actingAccess?.role || baseRole
        : baseRole;

      // (Optional) Load entitlements for effective tenant
      if (loadEntitlements) {
        // Delegate to customer.service which sets RLS context and returns entitlements
        const ents = await getCustomerEntitlements({
          customerId: effectiveCustomerId,
        });
        // Normalize to array of feature slugs (service may return rows or strings)
        const features = Array.isArray(ents)
          ? ents
              .map((e) => (e && typeof e === "object" ? e.feature : e))
              .filter(Boolean)
          : [];
        const entSet = new Set(features);
        req.entitlements = entSet;
        req.hasFeature = (f) => entSet.has(f);
      }

      // NOTE: Setting the Postgres RLS GUC (app.current_customer_id) is done in the DB layer.
      // When you add a per-request transaction wrapper, call:
      //   SELECT set_config('app.current_customer_id', :effectiveCustomerId, true)
      // early in the transaction so all queries in the request are RLS-scoped.

      // Always hydrate payloads so validators/controllers that still expect customerId
      // see the effective tenant derived by tenantContext.
      if (req.effectiveCustomerId) {
        req.body = { ...(req.body || {}), customerId: req.effectiveCustomerId };
        req.query = {
          ...(req.query || {}),
          customerId: req.effectiveCustomerId,
        };
      }
      return next();
    } catch (err) {
      return next(err);
    }
  };
}

/**
 * Convenience guard: require a feature entitlement for this route.
 * Must run AFTER tenantContext({ loadEntitlements: true }).
 * @param {string|string[]} feature
 */
function requireFeature(feature) {
  const needed = Array.isArray(feature) ? feature : [feature];
  return function (req, res, next) {
    if (!req.entitlements || typeof req.hasFeature !== "function") {
      return res.status(500).json({
        message:
          "Entitlements not initialised; ensure tenantContext middleware runs first",
      });
    }
    const ok = needed.every((f) => req.hasFeature(f));
    if (!ok) {
      return res
        .status(403)
        .json({ message: "Forbidden: required feature not enabled" });
    }
    return next();
  };
}

/**
 * Rejects any client-provided tenant in body/query to enforce the contract:
 * tenant must come from :customerId path param or X-Customer-Id header.
 * Options:
 *  - allowMatch: if true, allow a body/query customerId that EXACTLY matches req.effectiveCustomerId.
 */
function rejectClientTenantParam({ allowMatch = false } = {}) {
  return function (req, res, next) {
    const candidate =
      (req.body && req.body.customerId) || (req.query && req.query.customerId);
    if (!candidate) return next();

    const provided = String(candidate).trim();
    if (allowMatch && provided === req.effectiveCustomerId) return next();

    return res.status(400).json({
      status: "bad_request",
      reason: "no_client_tenant",
      message:
        "Do not pass customerId in body or query. Use :customerId in the URL or the X-Customer-Id header.",
      details: {
        provided,
        expected: allowMatch ? req.effectiveCustomerId : undefined,
      },
    });
  };
}

/**
 * Hydrate request body with customerId from tenant context for validators that still expect it.
 * Use per-route BEFORE validateRequest(...).
 */
function withTenantInBody(req, _res, next) {
  req.body = { ...(req.body || {}), customerId: req.effectiveCustomerId };
  next();
}

/**
 * Hydrate request query with customerId from tenant context for validators that still expect it.
 * Use per-route BEFORE validateRequest(..., "query").
 */
function withTenantInQuery(req, _res, next) {
  req.query = { ...(req.query || {}), customerId: req.effectiveCustomerId };
  next();
}

module.exports = {
  tenantContext,
  requireFeature,
  rejectClientTenantParam,
  withTenantInBody,
  withTenantInQuery,
};
