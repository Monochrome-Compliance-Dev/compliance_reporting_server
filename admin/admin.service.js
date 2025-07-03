const fs = require("fs");
const path = require("path");
const db = require("../db/database");
const { logger } = require("../helpers/logger");
const https = require("https");

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

let cachedRemoteContent = null;
let cacheTimestamp = 0;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

async function loadRemoteContent() {
  const now = Date.now();
  if (cachedRemoteContent && now - cacheTimestamp < CACHE_TTL_MS) {
    return cachedRemoteContent;
  }

  const s3Url =
    "https://monochrome-content.s3.ap-southeast-2.amazonaws.com/blog-faq.json";

  return new Promise((resolve, reject) => {
    https
      .get(s3Url, (res) => {
        let rawData = "";
        res.on("data", (chunk) => (rawData += chunk));
        res.on("end", () => {
          try {
            const parsed = JSON.parse(rawData);
            const allContent = [
              ...(parsed.blogs || []),
              ...(parsed.faqs || []),
            ];
            cachedRemoteContent = allContent;
            cacheTimestamp = Date.now();
            resolve(allContent);
          } catch (e) {
            reject(e);
          }
        });
      })
      .on("error", reject);
  });
}

async function getAllContent() {
  const content = await loadRemoteContent();
  return content;
}

async function getContentBySlug(slug) {
  const content = await loadRemoteContent();
  return content.find((item) => item.slug === slug) || null;
}
