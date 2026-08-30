jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: jest.fn(() => "model00001"),
}));

const {
  initPtrsV2Models,
  isPtrsV2RuntimeModelFile,
  listPtrsV2RuntimeModelFiles,
} = require("./ptrs_model_loader");

describe("PTRS v2 runtime model loader", () => {
  test.each([
    "ptrs_metrics_result.test.js",
    "ptrs_metrics_result.spec.js",
    "PTRS_METRICS_RESULT.TEST.JS",
  ])("excludes test module %s", (file) => {
    expect(isPtrsV2RuntimeModelFile(file)).toBe(false);
  });

  test("discovers runtime models without discovering colocated tests", () => {
    const files = listPtrsV2RuntimeModelFiles();

    expect(files).toContain("ptrs.js");
    expect(files).toContain("ptrs_metrics_result.js");
    expect(files).not.toContain("ptrs_metrics_result.test.js");
    expect(files).not.toContain("ptrs_model_loader.test.js");
    expect(files.every((file) => !/\.(test|spec)\.js$/i.test(file))).toBe(true);
  });

  test("continues to initialise legitimate PTRS models", () => {
    const sequelize = {
      define: jest.fn((name) => ({
        name,
        associate: null,
        belongsTo: jest.fn(),
        hasMany: jest.fn(),
        hasOne: jest.fn(),
      })),
    };

    const models = initPtrsV2Models(sequelize);

    expect(models.Ptrs).toBeDefined();
    expect(models.PtrsMetricsResult).toBeDefined();
    expect(sequelize.define).toHaveBeenCalled();
  });
});
