require("dotenv").config({ path: "../.env.development" });
const db = require("../db/database");

console.log("Loaded models:", Object.keys(db.sequelize.models));

async function seedESGTemplates() {
  await db.sequelize.sync();
  const { nanoid } = await import("nanoid");
  const { Template } = db.sequelize.models;

  console.log("Loaded Template model:", !!Template);
  if (!Template) {
    console.error(
      "Template model is not loaded. Check your db/database.js exports."
    );
    process.exit(1);
  }

  try {
    console.log("Database synced, models:", Object.keys(db.sequelize.models));

    // Delete all existing templates
    await Template.destroy({ where: {}, truncate: true, cascade: true });

    // Prepare the templates
    const templates = [
      // Indicators
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Scope 1 Emissions",
        description: "Direct GHG emissions from owned or controlled sources.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Scope 2 Emissions",
        description: "Indirect GHG emissions from purchased electricity.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Scope 3 Emissions",
        description: "Other indirect emissions in value chain.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Energy Consumption",
        description: "Total energy consumed.",
        category: "environment",
        defaultUnit: "MWh",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Water Usage",
        description: "Total water withdrawn.",
        category: "environment",
        defaultUnit: "m³",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Waste Generation",
        description: "Total waste generated.",
        category: "environment",
        defaultUnit: "tons",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Women in Leadership",
        description: "% women in leadership positions.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Board Diversity %",
        description: "% diversity on board.",
        category: "governance",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Employee Turnover Rate",
        description: "Annual employee turnover rate.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Anti-Corruption Incidents",
        description: "Number of anti-corruption incidents.",
        category: "governance",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Data Breaches",
        description: "Number of data breaches.",
        category: "governance",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Health & Safety Incidents",
        description: "Workplace health & safety incidents.",
        category: "social",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Community Engagement Hours",
        description: "Hours spent on community engagement.",
        category: "social",
        defaultUnit: "hours",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Employee Training Hours",
        description: "Hours of employee training.",
        category: "social",
        defaultUnit: "hours",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Supplier ESG Rating",
        description: "Percentage of sustainable suppliers.",
        category: "governance",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "% Renewable Energy Used",
        description: "Percentage of energy from renewable sources.",
        category: "environment",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "GHG Emissions Intensity",
        description: "GHG emissions intensity per unit output.",
        category: "environment",
        defaultUnit: "kg CO₂e/unit",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "% Waste Recycled",
        description: "Percentage of waste recycled.",
        category: "environment",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Workforce Diversity %",
        description: "% employee diversity.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "indicator",
        fieldName: "Customer Satisfaction Score",
        description: "Customer satisfaction score.",
        category: "social",
        defaultUnit: "score",
      },

      // Metrics
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Scope 1 Emissions",
        description: "Direct emissions metric.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Scope 2 Emissions",
        description: "Indirect emissions metric.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Scope 3 Emissions",
        description: "Other indirect emissions metric.",
        category: "environment",
        defaultUnit: "kg CO₂e",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Energy Consumption",
        description: "Energy consumption metric.",
        category: "environment",
        defaultUnit: "MWh",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Water Usage",
        description: "Water usage metric.",
        category: "environment",
        defaultUnit: "m³",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Waste Generation",
        description: "Waste generation metric.",
        category: "environment",
        defaultUnit: "tons",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Women in Leadership",
        description: "Gender balance metric.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Board Diversity %",
        description: "Board diversity metric.",
        category: "governance",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Employee Turnover Rate",
        description: "Employee turnover metric.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Anti-Corruption Incidents",
        description: "Anti-corruption metric.",
        category: "governance",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Data Breaches",
        description: "Data breaches metric.",
        category: "governance",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Health & Safety Incidents",
        description: "Health & safety metric.",
        category: "social",
        defaultUnit: "count",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Community Engagement Hours",
        description: "Community engagement metric.",
        category: "social",
        defaultUnit: "hours",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Employee Training Hours",
        description: "Employee training metric.",
        category: "social",
        defaultUnit: "hours",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Supplier ESG Rating",
        description: "Supplier sustainability metric.",
        category: "governance",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "% Renewable Energy Used",
        description: "Renewable energy usage metric.",
        category: "environment",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "GHG Emissions Intensity",
        description: "GHG intensity metric.",
        category: "environment",
        defaultUnit: "kg CO₂e/unit",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "% Waste Recycled",
        description: "Waste recycling rate metric.",
        category: "environment",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Workforce Diversity %",
        description: "Employee diversity metric.",
        category: "social",
        defaultUnit: "%",
      },
      {
        id: nanoid(10),
        clientId: null,
        fieldType: "metric",
        fieldName: "Customer Satisfaction Score",
        description: "Customer satisfaction metric.",
        category: "social",
        defaultUnit: "score",
      },
    ];

    await Template.bulkCreate(templates);
    console.log(
      `Seeded ${templates.length} ESG templates (20 indicators + 20 metrics) with descriptive names.`
    );
  } catch (err) {
    console.error("Error seeding ESG templates:", err);
  }
}

(async () => {
  await seedESGTemplates();
})();
