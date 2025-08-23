let nanoid;
(async () => {
  const { nanoid: importedNanoid } = await import("nanoid");
  nanoid = importedNanoid;
})();
require("dotenv").config({ path: "../.env.development" });
const db = require("../db/database");

(async () => {
  await db.sequelize.sync();
  // Set the current customer id
  await db.sequelize.query(`SET app.current_customer_id = 'rahfwOLxLN'`);
  // Load models
  const { MSSupplierRisk, MSTraining, MSGrievance } = db.sequelize.models;

  // Use multiple reporting periods for dev seeding
  const reportingPeriodIds = ["CjscD1oIL_", "SNQ8whsoYD", "SHlUUiHq1d"];
  const customerId = "rahfwOLxLN";
  const createdBy = "j3HJwUR_pi";
  const updatedBy = "j3HJwUR_pi";

  // Supplier Risks
  const supplierRisks = [
    {
      name: "Textiles Co",
      country: "Bangladesh",
      risk: "High",
      reviewed: new Date(),
    },
    {
      name: "ElectroSupply",
      country: "China",
      risk: "Medium",
      reviewed: new Date(),
    },
    {
      name: "AgriFarm Ltd",
      country: "Australia",
      risk: "Low",
      reviewed: new Date(),
    },
    {
      name: "Global Textiles",
      country: "India",
      risk: "High",
      reviewed: new Date(),
    },
    {
      name: "MicroElectronics",
      country: "Vietnam",
      risk: "Medium",
      reviewed: new Date(),
    },
    {
      name: "SteelPartners",
      country: "Turkey",
      risk: "Low",
      reviewed: new Date(),
    },
    {
      name: "FreshFoods",
      country: "Brazil",
      risk: "Medium",
      reviewed: new Date(),
    },
    {
      name: "EcoChemicals",
      country: "Germany",
      risk: "Low",
      reviewed: new Date(),
    },
    {
      name: "QuickTransport",
      country: "South Africa",
      risk: "High",
      reviewed: new Date(),
    },
    {
      name: "MegaPlastics",
      country: "Mexico",
      risk: "Medium",
      reviewed: new Date(),
    },
    {
      name: "AutoParts Inc",
      country: "USA",
      risk: "Low",
      reviewed: new Date(),
    },
    {
      name: "PrimeTextiles",
      country: "Pakistan",
      risk: "High",
      reviewed: new Date(),
    },
    {
      name: "Sunrise Mills",
      country: "Bangladesh",
      risk: "Medium",
      reviewed: new Date(),
    },
    {
      name: "HarvestAgro",
      country: "Australia",
      risk: "Low",
      reviewed: new Date(),
    },
  ];
  let supplierRiskCount = 0;

  // Trainings
  const trainings = [
    {
      employeeName: "Alice Smith",
      department: "Procurement",
      completed: true,
      completedAt: new Date("2024-03-15"),
    },
    {
      employeeName: "Bob Lee",
      department: "Finance",
      completed: false,
      completedAt: null,
    },
    {
      employeeName: "Carlos Diaz",
      department: "Operations",
      completed: true,
      completedAt: new Date("2024-02-28"),
    },
    {
      employeeName: "Diana Prince",
      department: "Legal",
      completed: true,
      completedAt: new Date("2024-01-15"),
    },
    {
      employeeName: "Evan Wright",
      department: "HR",
      completed: false,
      completedAt: null,
    },
    {
      employeeName: "Fiona Zhang",
      department: "Compliance",
      completed: true,
      completedAt: new Date("2024-04-10"),
    },
    {
      employeeName: "George Patel",
      department: "Logistics",
      completed: false,
      completedAt: null,
    },
    {
      employeeName: "Hannah Kim",
      department: "Procurement",
      completed: true,
      completedAt: new Date("2024-03-22"),
    },
    {
      employeeName: "Ivan Petrov",
      department: "Finance",
      completed: true,
      completedAt: new Date("2024-02-10"),
    },
    {
      employeeName: "Julia Rossi",
      department: "Legal",
      completed: false,
      completedAt: null,
    },
    {
      employeeName: "Karl Schmidt",
      department: "Operations",
      completed: true,
      completedAt: new Date("2024-01-28"),
    },
    {
      employeeName: "Lina Alvarez",
      department: "HR",
      completed: true,
      completedAt: new Date("2024-05-01"),
    },
    {
      employeeName: "Mohammed Ali",
      department: "Compliance",
      completed: false,
      completedAt: null,
    },
    {
      employeeName: "Nina Brown",
      department: "Logistics",
      completed: true,
      completedAt: new Date("2024-06-05"),
    },
  ];
  let trainingCount = 0;

  // Grievances
  const grievances = [
    {
      description: "Unsafe working conditions reported",
      status: "Open",
      date: new Date("2024-05-10"),
    },
    {
      description: "Wage payment delay",
      status: "Closed",
      date: new Date("2024-04-20"),
    },
    {
      description: "Harassment complaint",
      status: "Investigating",
      date: new Date("2024-03-05"),
    },
    {
      description: "Child labor concerns raised",
      status: "Investigating",
      date: new Date("2024-01-20"),
    },
    {
      description: "Forced overtime flagged",
      status: "Open",
      date: new Date("2024-06-01"),
    },
    {
      description: "Discrimination reported",
      status: "Closed",
      date: new Date("2024-05-18"),
    },
    {
      description: "Lack of safety equipment",
      status: "Open",
      date: new Date("2024-04-25"),
    },
    {
      description: "Verbal abuse allegation",
      status: "Investigating",
      date: new Date("2024-03-30"),
    },
    {
      description: "Unpaid overtime complaint",
      status: "Closed",
      date: new Date("2024-02-14"),
    },
    {
      description: "Freedom of association issue",
      status: "Open",
      date: new Date("2024-02-28"),
    },
    {
      description: "Lack of clean water",
      status: "Investigating",
      date: new Date("2024-01-08"),
    },
    {
      description: "Gender discrimination",
      status: "Closed",
      date: new Date("2024-05-28"),
    },
    {
      description: "Bullying by supervisor",
      status: "Open",
      date: new Date("2024-04-02"),
    },
    {
      description: "Excessive heat in factory",
      status: "Investigating",
      date: new Date("2024-06-07"),
    },
  ];
  let grievanceCount = 0;

  for (const [i, reportingPeriodId] of reportingPeriodIds.entries()) {
    for (const [j, risk] of supplierRisks.entries()) {
      const variedRisk = { ...risk };
      if (i === 1 && j % 3 === 0) variedRisk.risk = "High";
      if (i === 2 && j % 4 === 0) variedRisk.risk = "Medium";
      await MSSupplierRisk.create({
        id: nanoid(10),
        ...variedRisk,
        reportingPeriodId,
        customerId,
        createdBy,
        updatedBy,
      });
      supplierRiskCount++;
    }

    for (const [j, training] of trainings.entries()) {
      const variedTraining = { ...training };
      if (i === 1 && j % 2 === 0) {
        variedTraining.completed = true;
        variedTraining.completedAt = new Date("2025-05-15");
      }
      if (i === 2 && j % 3 === 0) {
        variedTraining.completed = false;
        variedTraining.completedAt = null;
      }
      await MSTraining.create({
        id: nanoid(10),
        ...variedTraining,
        reportingPeriodId,
        customerId,
        createdBy,
        updatedBy,
      });
      trainingCount++;
    }

    for (const [j, grievance] of grievances.entries()) {
      const variedGrievance = { ...grievance };
      if (i === 1 && j % 2 === 0) variedGrievance.status = "Open";
      if (i === 2 && j % 3 === 0) variedGrievance.status = "Closed";
      await MSGrievance.create({
        id: nanoid(10),
        ...variedGrievance,
        reportingPeriodId,
        customerId,
        createdBy,
        updatedBy,
      });
      grievanceCount++;
    }
  }

  console.log(
    `Seeded: ${supplierRiskCount} supplier risks, ${trainingCount} trainings, ${grievanceCount} grievances.`
  );
  process.exit();
})();
