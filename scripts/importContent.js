require("dotenv").config({ path: ".env.development" });
const fs = require("fs");
const path = require("path");
const matter = require("gray-matter");
const { Sequelize } = require("sequelize");

const sequelize = new Sequelize(
  "compliance_reporting", // DB name
  "appuser", // DB user
  "nFqE5u#VULQsP^&", // Replace with your actual password
  {
    host: "localhost",
    port: 5432,
    dialect: "postgres",
    logging: false,
  }
);

const defineAdminContent = require("../admin/admin.model");
const db = {
  sequelize,
  Sequelize,
  AdminContent: defineAdminContent(sequelize),
};
console.log("Available models:", Object.keys(db));
let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
  await run(); // moved run() inside the async wrapper
})();

// Helper to import a single file
async function importMarkdownFile(filePath, type) {
  const file = fs.readFileSync(filePath, "utf8");
  const { data, content } = matter(file);

  const slug =
    (data.slug ??
      data.title
        ?.toLowerCase()
        .replace(/[^a-z0-9]+/g, "-")
        .replace(/(^-|-$)/g, "") ??
      nanoid(8)) +
    "-" +
    nanoid(5);

  const existing = await db.AdminContent.findOne({ where: { slug } });
  if (existing) {
    console.warn(`Skipping duplicate: ${slug}`);
    return;
  }

  await db.AdminContent.create({
    id: nanoid(10),
    type,
    title: data.title,
    slug,
    description: data.description || content.substring(0, 160),
    content,
    date:
      data.date && !isNaN(new Date(data.date).getTime())
        ? new Date(data.date)
        : null,
    tags: Array.isArray(data.tags) ? data.tags : [],
    createdBy: "migration",
  });
}

// Main function
async function run() {
  console.log("Connecting to:", {
    db: process.env.DB_NAME,
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
  });
  await db.sequelize.sync(); // ensure connection

  const blogDir = path.join(__dirname, "../import/blog");
  const faqDir = path.join(__dirname, "../import/faq");

  const blogFiles = fs.readdirSync(blogDir).filter((f) => f.endsWith(".md"));
  const faqFiles = fs.readdirSync(faqDir).filter((f) => f.endsWith(".md"));

  for (const file of blogFiles) {
    await importMarkdownFile(path.join(blogDir, file), "blog");
  }
  for (const file of faqFiles) {
    await importMarkdownFile(path.join(faqDir, file), "faq");
  }

  console.log("All content imported successfully!");
}
