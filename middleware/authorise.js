const { expressjwt: expressJwt } = require("express-jwt");
const { Op } = require("sequelize");
const secret = process.env.JWT_SECRET;
const db = require("../db/database");
const { logger } = require("../helpers/logger");

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
        return res.status(401).json({ message: "Unauthorised" });
      }

      if (user.customerId !== req.auth.customerId) {
        logger.logEvent("warn", "Forbidden access: customer mismatch", {
          action: "AuthoriseAccessCheck",
          userId: req.auth.id,
          role: user?.role,
          customerId: user?.customerId,
        });
        return res
          .status(403)
          .json({ message: "Forbidden: Tenant access denied" });
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
        return res.status(403).json({ message: "Forbidden: Read-only access" });
      }

      const refreshTokens = await user.getRefreshTokens();
      req.auth.ownsToken = (token) =>
        !!refreshTokens.find((x) => x.token === token);

      // Load feature entitlements for this tenant so downstream routes can gate access
      try {
        const ACTIVE_STATES = ["active", "trial", "grace"]; // usable states
        const now = new Date();
        const rows = await db.FeatureEntitlement.findAll({
          where: {
            customerId: user.customerId,
            status: { [Op.in]: ACTIVE_STATES },
            [Op.or]: [{ validTo: null }, { validTo: { [Op.gte]: now } }],
          },
          attributes: ["feature"],
        });

        const featuresSet = new Set(rows.map((r) => r.feature));
        req.entitlements = featuresSet; // Set<string>
        req.hasFeature = (f) => featuresSet.has(f);
      } catch (e) {
        logger.logEvent("error", "Entitlements load failed", {
          action: "EntitlementsLoad",
          error: e?.message,
          customerId: user.customerId,
        });
        req.entitlements = new Set();
        req.hasFeature = () => false;
      }

      // Enforce feature requirements if features option was passed
      if (features) {
        let hasRequiredFeatures;
        if (mode === "all") {
          hasRequiredFeatures = features.every((f) => req.entitlements.has(f));
        } else {
          // mode 'any'
          hasRequiredFeatures = features.some((f) => req.entitlements.has(f));
        }

        if (!hasRequiredFeatures) {
          logger.logEvent(
            "warn",
            "Forbidden access: missing required feature(s)",
            {
              action: "FeatureAccessCheck",
              userId: req.auth.id,
              customerId: user.customerId,
              requiredFeatures: features,
              mode: mode,
            }
          );
          return res
            .status(403)
            .json({ message: "Forbidden: Required feature(s) not enabled" });
        }
      }

      next();
    },
  ];
}
