const fs = require("fs");
const path = require("path");
const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  saveBlog,
  saveFaq,
  getAllContent,
  getContentBySlug,
};

async function saveBlog({ title, slug, content, userId }) {
  logger.logEvent("info", "Saving blog", {
    action: "SaveBlog",
    slug,
    userId,
  });

  const blogDir = path.join(__dirname, "../public/blog");
  if (!fs.existsSync(blogDir)) fs.mkdirSync(blogDir, { recursive: true });

  const filePath = path.join(blogDir, `${slug}.md`);
  const markdown = `---
title: ${title}
slug: ${slug}
date: ${new Date().toISOString()}
---

${content}
`;

  fs.writeFileSync(filePath, markdown, "utf-8");

  const existing = await db.AdminContent.findOne({ where: { slug } });
  if (existing) {
    await existing.update({ title, content, updatedBy: userId });
  } else {
    await db.AdminContent.create({
      type: "blog",
      title,
      slug,
      content,
      createdBy: userId,
    });
  }

  logger.logEvent("info", "Blog saved", {
    action: "SaveBlog",
    slug,
  });
}

async function saveFaq({ content, userId }) {
  logger.logEvent("info", "Saving FAQ", {
    action: "SaveFaq",
    userId,
  });

  const faqPath = path.join(__dirname, "../public/content/faq.md");
  fs.writeFileSync(faqPath, content, "utf-8");

  const slug = "faq";
  const existing = await db.AdminContent.findOne({ where: { slug } });
  if (existing) {
    await existing.update({ title: "FAQ", content, updatedBy: userId });
  } else {
    await db.AdminContent.create({
      type: "faq",
      title: "FAQ",
      slug,
      content,
      createdBy: userId,
    });
  }

  logger.logEvent("info", "FAQ saved", {
    action: "SaveFaq",
  });
}

async function getAllContent() {
  logger.logEvent("info", "Fetching all content", {
    action: "GetAllAdminContent",
  });

  return db.AdminContent.findAll({
    order: [["createdAt", "DESC"]],
  });
}

async function getContentBySlug(slug) {
  logger.logEvent("info", "Fetching content by slug", {
    action: "GetAdminContentBySlug",
    slug,
  });

  return db.AdminContent.findOne({ where: { slug } });
}
