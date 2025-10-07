const { expressjwt: expressJwt } = require("express-jwt");
const secret = process.env.JWT_SECRET;
const db = require("@/db/database");
const { logger } = require("@/helpers/logger");
const { tenantContext } = require("./tenantContext");

module.exports = authorise;

function authorise(input = {}) {
  // Normalize input to support roles as string/array or options object with roles and features
  let roles = [];
  let features = null;
  let mode = "all";

  if (typeof input === "string" || Array.isArray(input)) {
    roles = typeof input === "string" ? [input] : input;
  } else if (typeof input === "object" && input !== null) {
    if (input.roles) {
      roles = typeof input.roles === "string" ? [input.roles] : input.roles;
    }
    if (input.features) {
      features = Array.isArray(input.features)
        ? input.features
        : [input.features];
      mode = input.mode === "any" ? "any" : "all";
    }
  }

  return [
    // authenticate JWT token and attach user to request object (req.auth)
    expressJwt({ secret, algorithms: ["HS256"] }),

    // authorize based on user role
    async (req, res, next) => {
      // console.log("req.body:", req.body);
      // console.log("req.auth:", req.auth);
      const user = await db.User.findByPk(req.auth.id);

      if (!user || (roles.length && !roles.includes(user.role))) {
        logger.logEvent("warn", "Unauthorised access attempt", {
          action: "AuthoriseAccessCheck",
          userId: req.auth.id,
        });
        return res.status(401).json({
          status: "unauthorised",
          reason: "role_denied",
          message: "Unauthorised",
        });
      }

      // Prevent write access for Audit role
      const writeMethods = ["POST", "PUT", "PATCH", "DELETE"];
      if (user.role === "Audit" && writeMethods.includes(req.method)) {
        logger.logEvent("warn", "Write attempt by Audit role", {
          action: "ReadOnlyEnforced",
          userId: req.auth.id,
          method: req.method,
          path: req.originalUrl,
        });
        return res.status(403).json({
          status: "forbidden",
          reason: "read_only_role",
          message: "Read-only access",
        });
      }

      const refreshTokens = await user.getRefreshTokens();
      req.auth.ownsToken = (token) =>
        !!refreshTokens.find((x) => x.token === token);

      next();
    },

    tenantContext({ loadEntitlements: true, enforceMapping: true }),

    (req, res, next) => {
      if (!features) return next();

      const ent = req.entitlements;
      const hasRequiredFeatures =
        ent && typeof ent.has === "function"
          ? mode === "all"
            ? features.every((f) => ent.has(f))
            : features.some((f) => ent.has(f))
          : false;

      if (!hasRequiredFeatures) {
        logger.logEvent(
          "warn",
          "Forbidden access: missing required feature(s)",
          {
            action: "FeatureAccessCheck",
            userId: req.auth.id,
            customerId:
              req.tenantCustomerId || (req.user && req.user.customerId),
            requiredFeatures: features,
            mode,
          }
        );
        return res.status(403).json({
          status: "forbidden",
          reason: "missing_feature",
          message: "Forbidden: Required feature(s) not enabled",
        });
      }

      return next();
    },
  ];
}
