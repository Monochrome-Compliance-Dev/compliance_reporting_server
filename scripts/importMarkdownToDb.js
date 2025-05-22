const fs = require("fs");
const path = require("path");
const matter = require("gray-matter");
const db = require("../helpers/db");

async function importMarkdown(filePath, type) {
  const raw = fs.readFileSync(filePath, "utf-8");
  const { data, content } = matter(raw);

  const title = data.title || path.basename(filePath, ".md");
  const slug = data.slug || path.basename(filePath, ".md");

  const existing = await db.AdminContent.findOne({ where: { slug } });

  if (existing) {
    await existing.update({ title, content, updatedBy: "import-script" });
    console.log(`✅ Updated: ${slug}`);
  } else {
    await db.AdminContent.create({
      type,
      title,
      slug,
      content,
      createdBy: "import-script",
    });
    console.log(`✅ Created: ${slug}`);
  }
}

async function run() {
  const faqPath = path.resolve(__dirname, "../import/faq.md");
  if (fs.existsSync(faqPath)) {
    await importMarkdown(faqPath, "faq");
  }

  const blogDir = path.resolve(__dirname, "../import/blog");
  if (fs.existsSync(blogDir)) {
    const files = fs.readdirSync(blogDir).filter((f) => f.endsWith(".md"));
    for (const file of files) {
      await importMarkdown(path.join(blogDir, file), "blog");
    }
  }

  console.log("🎉 Import complete!");
  process.exit();
}

run().catch((err) => {
  console.error("❌ Import failed", err);
  process.exit(1);
});
