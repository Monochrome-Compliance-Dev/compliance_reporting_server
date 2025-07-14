require("dotenv").config({ path: "../.env.development" });
const db = require("../db/database");

console.log("Loaded models:", Object.keys(db.sequelize.models));

async function seedESGTemplates() {
  const { nanoid } = await import("nanoid");

  try {
    await db.sequelize.sync();
    await db.sequelize.query(`SET app.current_client_id = 'TESTCLIENT'`);
    console.log("Database synced, models:", Object.keys(db.sequelize.models));

    const { ESGIndicator, ESGMetric, Unit } = db.sequelize.models;

    const indicators = await ESGIndicator.bulkCreate([
      // Environmental
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "SCOPE_1",
        name: "Scope 1 Emissions",
        description: "Direct GHG emissions.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "SCOPE_2",
        name: "Scope 2 Emissions",
        description: "Indirect GHG emissions.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "SCOPE_3",
        name: "Scope 3 Emissions",
        description: "Value chain GHG emissions.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "ENERGY_USE",
        name: "Total Energy Consumption",
        description: "Energy used in operations.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "RENEWABLE_ENERGY",
        name: "% Renewable Energy Used",
        description: "Portion from renewables.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "WATER_USE",
        name: "Water Withdrawal",
        description: "Total water use.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "WASTE_GEN",
        name: "Waste Generated",
        description: "Total waste generated.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "WASTE_RECYCLED",
        name: "% Waste Recycled",
        description: "Portion of waste recycled.",
        category: "environment",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "ENV_FINES",
        name: "Environmental Fines",
        description: "Monetary penalties.",
        category: "environment",
        isTemplate: true,
      },

      // Social
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "WOMEN_LEADERSHIP",
        name: "Women in Leadership",
        description: "% women in leadership roles.",
        category: "social",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "TURNOVER",
        name: "Workforce Turnover Rate",
        description: "Annual employee turnover.",
        category: "social",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "LTIFR",
        name: "Lost Time Injury Frequency Rate",
        description: "Safety incidents metric.",
        category: "social",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "TRAINING_HOURS",
        name: "Employee Training Hours",
        description: "Avg hours of training per FTE.",
        category: "social",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "DIVERSITY",
        name: "Workforce Diversity %",
        description: "% of underrepresented groups.",
        category: "social",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "COMMUNITY_INVEST",
        name: "Community Investment",
        description: "Spending on community programs.",
        category: "social",
        isTemplate: true,
      },

      // Governance
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "SUPPLIER_RATING",
        name: "Supplier ESG Rating",
        description: "Average supplier ESG scores.",
        category: "governance",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "BOARD_DIVERSITY",
        name: "Board Diversity %",
        description: "% diversity on board.",
        category: "governance",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "DATA_BREACHES",
        name: "Data Breaches",
        description: "Reported security incidents.",
        category: "governance",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "ANTI_CORRUPTION",
        name: "Anti-Corruption Training Coverage",
        description: "% of employees trained.",
        category: "governance",
        isTemplate: true,
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        code: "GOV_FINES",
        name: "Governance Fines",
        description: "Fines from compliance failures.",
        category: "governance",
        isTemplate: true,
      },
    ]);

    console.log(`Seeded ${indicators.length} ESG Indicator templates.`);

    // Seed a zero-value metric for each template indicator tied to common units
    const unitMap = {};
    const unitRecords = await Unit.findAll();
    unitRecords.forEach((unit) => {
      unitMap[unit.symbol] = unit.id;
    });

    const metrics = indicators.map((ind) => {
      let unitId = null;
      switch (ind.code) {
        case "SCOPE_1":
        case "SCOPE_2":
        case "SCOPE_3":
          unitId = unitMap["kg CO₂e"];
          break;
        case "ENERGY_USE":
          unitId = unitMap["MWh"];
          break;
        case "RENEWABLE_ENERGY":
        case "WASTE_RECYCLED":
        case "WOMEN_LEADERSHIP":
        case "DIVERSITY":
        case "ANTI_CORRUPTION":
        case "BOARD_DIVERSITY":
          unitId = unitMap["%"];
          break;
        case "WATER_USE":
        case "WASTE_GEN":
          unitId = unitMap["m³"];
          break;
        case "ENV_FINES":
        case "COMMUNITY_INVEST":
        case "GOV_FINES":
          unitId = unitMap["$"];
          break;
        case "TRAINING_HOURS":
          unitId = unitMap["hrs"];
          break;
        case "DATA_BREACHES":
        case "LTIFR":
        case "TURNOVER":
          unitId = unitMap["count"];
          break;
        case "SUPPLIER_RATING":
          unitId = null; // could be score or letter, TBD
          break;
      }

      const metric = {
        id: nanoid(10),
        indicatorId: ind.id,
        clientId: "TESTCLIENT",
        reportingPeriodId: "TEMPLATE",
        value: 0,
        unitId,
        isTemplate: true,
      };
      console.log("Prepared metric:", metric);
      return metric;
    });

    await ESGMetric.bulkCreate(metrics);
    console.log(
      `Seeded ${metrics.length} ESG Metric templates with linked units.`
    );

    console.log("Done seeding expanded ESG templates.");
  } catch (err) {
    console.error("Error seeding ESG templates:", err);
  }
}

// Seed units first, then ESG templates
(async () => {
  await seedUnits();
  await seedESGTemplates();
})();

// --------------------------
// Common Measurement Units Seed
// --------------------------

async function seedUnits() {
  const { nanoid } = await import("nanoid");

  try {
    await db.sequelize.sync();
    await db.sequelize.query(`SET app.current_client_id = 'TESTCLIENT'`);
    console.log("Database synced, models:", Object.keys(db.sequelize.models));

    const { Unit } = db.sequelize.models;

    console.log("Seeding common units...");

    await Unit.destroy({ where: {}, truncate: true, cascade: true });

    const units = await Unit.bulkCreate([
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Percentage",
        symbol: "%",
        description:
          "Represents a portion of 100, commonly used for ratios and shares.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Kilograms of CO₂ equivalent",
        symbol: "kg CO₂e",
        description:
          "Standard unit to express greenhouse gas emissions as CO₂ equivalents.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Megawatt Hours",
        symbol: "MWh",
        description:
          "Unit of energy representing one million watts consumed or generated for one hour.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Cubic Meters",
        symbol: "m³",
        description:
          "Volume measurement typically used for water withdrawal or waste.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Dollars",
        symbol: "$",
        description: "Monetary value, usually reported in local currency.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Hours",
        symbol: "hrs",
        description:
          "Used for time-based metrics such as employee training hours.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Number of incidents",
        symbol: "count",
        description:
          "Simple count of occurrences like data breaches or safety incidents.",
      },
      {
        id: nanoid(10),
        clientId: "TESTCLIENT",
        name: "Metric Tons CO₂ equivalent",
        symbol: "t CO₂e",
        description: "Commonly used for aggregated GHG emissions reporting.",
      },
    ]);

    console.log(`Seeded ${units.length} common units.`);
  } catch (err) {
    console.error("Error seeding units:", err);
  }
}
