jest.mock("@/db/database", () => ({
  PtrsColumnMap: {
    create: jest.fn(),
    findOne: jest.fn(),
  },
}));
jest.mock("@/helpers/setCustomerIdRLS", () => ({
  beginTransactionWithCustomerContext: jest.fn(),
}));
jest.mock("@/helpers/ptrsTrackerLog", () => ({
  createPtrsTrace: jest.fn(() => null),
  hrMsSince: jest.fn(() => 1),
}));
jest.mock("@/helpers/nanoid_helper", () => ({
  getNanoid: jest.fn(() => "column-map"),
}));
jest.mock("@/v2/ptrs/services/ptrs.service", () => ({
  buildStableInputHash: jest.fn(() => "map-signature"),
  safeMeta: (value) => value,
  slog: {
    error: jest.fn(),
    info: jest.fn(),
  },
  toSnake: (value) => String(value || "").toLowerCase(),
}));

const db = require("@/db/database");
const {
  beginTransactionWithCustomerContext,
} = require("@/helpers/setCustomerIdRLS");
const definePtrsColumnMap = require("../models/ptrs_column_map");
const { saveJoins } = require("./joins.ptrs.service");
const { saveSupportConfig } = require("./maps.config.ptrs.service");

function makeTransaction() {
  return {
    finished: false,
    commit: jest.fn(async function commit() {
      this.finished = "commit";
    }),
    rollback: jest.fn(async function rollback() {
      this.finished = "rollback";
    }),
  };
}

function makeRow(values) {
  return {
    ...values,
    get: jest.fn(function get() {
      return { ...this };
    }),
  };
}

describe("PtrsColumnMap rowRules defaults", () => {
  beforeEach(() => {
    beginTransactionWithCustomerContext.mockImplementation(async () =>
      makeTransaction(),
    );
    db.PtrsColumnMap.findOne.mockReset();
    db.PtrsColumnMap.create.mockReset();
  });

  test("defines the model default as a required empty array", () => {
    const sequelize = {
      define: jest.fn((_name, attributes) => ({ rawAttributes: attributes })),
    };

    const model = definePtrsColumnMap(sequelize);

    expect(model.rawAttributes.rowRules).toMatchObject({
      allowNull: false,
      defaultValue: [],
    });
  });

  test("saveJoins creates a fresh config with empty row rules", async () => {
    db.PtrsColumnMap.findOne.mockResolvedValue(null);
    db.PtrsColumnMap.create.mockImplementation(async (values) =>
      makeRow(values),
    );

    await saveJoins({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      joins: { conditions: [] },
      customFields: [],
      profileId: "profile-1",
      userId: "user-1",
    });

    expect(db.PtrsColumnMap.create).toHaveBeenCalledWith(
      expect.objectContaining({ rowRules: [] }),
      expect.any(Object),
    );
  });

  test("saveJoins does not modify populated row rules", async () => {
    const rowRules = [{ id: "existing-rule" }];
    const existing = makeRow({
      rowRules,
      createdBy: "creator-1",
      updatedBy: "updater-1",
    });
    existing.update = jest.fn(async (values) =>
      Object.assign(existing, values),
    );
    db.PtrsColumnMap.findOne.mockResolvedValue(existing);

    await saveJoins({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      joins: { conditions: [] },
      customFields: [],
      profileId: "profile-1",
      userId: "user-1",
    });

    expect(existing.update.mock.calls[0][0]).not.toHaveProperty("rowRules");
    expect(existing.rowRules).toEqual(rowRules);
  });

  test("saveSupportConfig gives other fresh creation paths the same default", async () => {
    db.PtrsColumnMap.findOne.mockResolvedValue(null);
    db.PtrsColumnMap.create.mockImplementation(async (values) =>
      makeRow(values),
    );

    await saveSupportConfig({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      mappings: {},
      rowRules: null,
      userId: "user-1",
    });

    expect(db.PtrsColumnMap.create).toHaveBeenCalledWith(
      expect.objectContaining({ rowRules: [] }),
      expect.any(Object),
    );
  });

  test("saveSupportConfig preserves populated rules when rules are omitted", async () => {
    const rowRules = [{ id: "existing-rule" }];
    const existing = makeRow({
      mappings: {},
      extras: null,
      fallbacks: null,
      defaults: null,
      joins: { conditions: [] },
      rowRules,
      customFields: [],
      profileId: "profile-1",
      createdBy: "creator-1",
      updatedBy: "updater-1",
    });
    existing.update = jest.fn(async (values) =>
      Object.assign(existing, values),
    );
    db.PtrsColumnMap.findOne.mockResolvedValue(existing);

    await saveSupportConfig({
      customerId: "customer-1",
      ptrsId: "ptrs-1",
      mappings: {},
      rowRules: null,
      userId: "user-1",
    });

    expect(existing.update.mock.calls[0][0].rowRules).toEqual(rowRules);
    expect(existing.rowRules).toEqual(rowRules);
  });
});
