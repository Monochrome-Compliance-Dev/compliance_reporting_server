const { expressjwt: expressJwt } = require("express-jwt");
const { secret } = require("../config.json");
const db = require("../helpers/db");
const logger = require("../helpers/logger");

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
      const user = await db.User.findByPk(req.auth.id);

      if (!user || (roles.length && !roles.includes(user.role))) {
        logger.warn(`Unauthorised access attempt by user ID: ${req.auth.id}`);
        return res.status(401).json({ message: "Unauthorised" });
      }

      if (user.clientId !== req.auth.clientId) {
        logger.warn(
          `Unauthorised access - User ID: ${req.auth.id}, Role: ${user?.role}, Client: ${user?.clientId}`
        );
        return res
          .status(403)
          .json({ message: "Forbidden: Tenant access denied" });
      }

      logger.info(
        `User authorised - ID: ${req.auth.id}, Role: ${user.role}, Client: ${user.clientId}, IP: ${req.ip}`
      );

      const refreshTokens = await user.getRefreshTokens();
      req.auth.ownsToken = (token) =>
        !!refreshTokens.find((x) => x.token === token);
      next();
    },
  ];
}
