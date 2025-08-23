const { expressjwt: expressJwt } = require("express-jwt");
const secret = process.env.JWT_SECRET;
const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = authorise;

function authorise(roles = []) {
  // roles param can be a single role string (e.g. Role.User or 'User')
  // or an array of roles (e.g. [Role.Admin, Role.User] or ['Admin', 'User'])
  if (typeof roles === "string") {
    roles = [roles];
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

      logger.logEvent("info", "User authorised", {
        action: "AuthoriseAccessGranted",
        userId: req.auth.id,
        role: user.role,
        customerId: user.customerId,
        ip: req.ip,
      });

      const refreshTokens = await user.getRefreshTokens();
      req.auth.ownsToken = (token) =>
        !!refreshTokens.find((x) => x.token === token);

      next();
    },
  ];
}
