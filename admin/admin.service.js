const fs = require("fs");
const path = require("path");
const db = require("../helpers/db");

async function saveBlog({ title, slug, content, userId }) {
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

  const existing = await db.adminContent.findOne({ where: { slug } });
  if (existing) {
    await existing.update({ title, content, updatedBy: userId });
  } else {
    await db.adminContent.create({
      type: "blog",
      title,
      slug,
      content,
      createdBy: userId,
    });
  }
}

async function saveFaq({ content, userId }) {
  const faqPath = path.join(__dirname, "../public/content/faq.md");
  fs.writeFileSync(faqPath, content, "utf-8");

  const slug = "faq";
  const existing = await db.adminContent.findOne({ where: { slug } });
  if (existing) {
    await existing.update({ title: "FAQ", content, updatedBy: userId });
  } else {
    await db.adminContent.create({
      type: "faq",
      title: "FAQ",
      slug,
      content,
      createdBy: userId,
    });
  }
}

async function getAllContent() {
  return db.adminContent.findAll({
    order: [["createdAt", "DESC"]],
  });
}

async function getContentBySlug(slug) {
  return db.adminContent.findOne({ where: { slug } });
}

module.exports = {
  saveBlog,
  saveFaq,
  getAllContent,
  getContentBySlug,
};
