const fs = require("fs");
const path = require("path");
const matter = require("gray-matter");
const db = require("../helpers/db");

async function importMarkdown(filePath, type) {
  const raw = fs.readFileSync(filePath, "utf-8");
  const { data, content: fullContent } = matter(raw);

  const title = data.title || path.basename(filePath, ".md");
  const slug = data.slug || path.basename(filePath, ".md");
  const description = data.description || "";
  const date =
    data.date && !isNaN(Date.parse(data.date))
      ? new Date(Date.parse(data.date))
      : undefined;
  const tags = Array.isArray(data.tags) ? data.tags.join(",") : data.tags || "";

  // Remove header lines (lines starting with # title, # date, etc.)
  const filteredLines = fullContent
    .split("\n")
    .filter(
      (line) => !/^#\s*(title|date|summary|author|published):/i.test(line)
    )
    .join("\n")
    .trim();

  const existing = await db.AdminContent.findOne({ where: { slug } });

  if (existing) {
    await existing.update({
      title,
      content: filteredLines,
      description,
      ...(date ? { date } : {}),
      tags,
      updatedBy: "import-script",
    });
    console.log(`✅ Updated: ${slug}`);
  } else {
    await db.AdminContent.create({
      type,
      title,
      slug,
      content: filteredLines,
      description,
      ...(date ? { date } : {}),
      tags,
      createdBy: "import-script",
    });
    console.log(`✅ Created: ${slug}`);
  }
}

async function run() {
  const faqPath = path.resolve(__dirname, "../import/faq.md");
  if (fs.existsSync(faqPath)) {
    const raw = fs.readFileSync(faqPath, "utf-8");
    const { content: fullContent, data } = matter(raw);
    const lines = fullContent.split("\n");

    let currentTitle = null;
    let currentBody = [];

    const flush = async () => {
      if (!currentTitle || !currentBody.length) return;
      const title = currentTitle.trim();
      const slug = title
        .toLowerCase()
        .replace(/\s+/g, "-")
        .replace(/[^\w-]/g, "");
      const content = currentBody.join("\n").trim();

      const existing = await db.AdminContent.findOne({ where: { slug } });

      if (existing) {
        await existing.update({
          title,
          content,
          type: "faq",
          updatedBy: "import-script",
        });
        console.log(`✅ Updated: ${slug}`);
      } else {
        await db.AdminContent.create({
          type: "faq",
          title,
          slug,
          content,
          createdBy: "import-script",
        });
        console.log(`✅ Created: ${slug}`);
      }
    };

    for (const line of lines) {
      if (line.startsWith("## ")) {
        await flush();
        currentTitle = line.replace(/^## /, "");
        currentBody = [];
      } else {
        currentBody.push(line);
      }
    }
    await flush();

    // Create legacy full FAQ entry
    const legacy = await db.AdminContent.findOne({ where: { slug: "faq" } });
    if (legacy) {
      await legacy.update({
        title: data.title || "FAQ (Full)",
        content: fullContent,
        updatedBy: "import-script",
      });
      console.log(`✅ Updated: faq`);
    } else {
      await db.AdminContent.create({
        type: "faq",
        title: data.title || "FAQ (Full)",
        slug: "faq",
        content: fullContent,
        createdBy: "import-script",
      });
      console.log(`✅ Created: faq`);
    }
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
