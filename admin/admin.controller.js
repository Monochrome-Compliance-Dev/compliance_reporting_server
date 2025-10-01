const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const adminService = require("./admin.service");
const { logger } = require("../helpers/logger");
const { saveBlogSchema, saveFaqSchema } = require("./admin.validator");

// routes
router.post(
  "/save-blog",
  authorise({ roles: ["Admin", "Boss", "User"] }),
  validateRequest(saveBlogSchema),
  saveBlog
);
router.post(
  "/save-faq",
  authorise({ roles: ["Admin", "Boss", "User"] }),
  validateRequest(saveFaqSchema),
  saveFaq
);
router.get("/content", getAllContent);
router.get("/content/:slug", getContentBySlug);

module.exports = router;

function saveBlog(req, res, next) {
  logger.logEvent("info", "Saving blog post", {
    action: "SaveBlog",
    userId: req?.auth?.id || "unknown",
    ip: req.ip,
  });
  adminService
    .saveBlog({ ...req.body, userId: req?.auth?.id })
    .then(() => {
      logger.logEvent("info", "Blog post saved", { action: "SaveBlog" });
      res.json({ message: "Blog post saved successfully." });
    })
    .catch((error) => {
      logger.logEvent("error", "Error saving blog post", {
        action: "SaveBlog",
        error: error.message,
      });
      next(error);
    });
}

function saveFaq(req, res, next) {
  logger.logEvent("info", "Saving FAQ", {
    action: "SaveFaq",
    userId: req?.auth?.id || "unknown",
    ip: req.ip,
  });
  adminService
    .saveFaq({ ...req.body, userId: req?.auth?.id })
    .then(() => {
      logger.logEvent("info", "FAQ saved", { action: "SaveFaq" });
      res.json({ message: "FAQ saved successfully." });
    })
    .catch((error) => {
      logger.logEvent("error", "Error saving FAQ", {
        action: "SaveFaq",
        error: error.message,
      });
      next(error);
    });
}

function getAllContent(req, res, next) {
  adminService
    .getAllContent()
    .then((data) => res.json(data))
    .catch(next);
}

function getContentBySlug(req, res, next) {
  adminService
    .getContentBySlug(req.params.slug)
    .then((data) => {
      if (!data) return res.sendStatus(404);
      res.json(data);
    })
    .catch(next);
}
