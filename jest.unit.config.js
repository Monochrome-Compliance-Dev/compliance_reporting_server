module.exports = {
  testEnvironment: "node",
  testMatch: ["<rootDir>/**/*.test.js"],
  testPathIgnorePatterns: ["/node_modules/"],
  moduleNameMapper: {
    "^@/(.*)$": "<rootDir>/$1",
  },
  collectCoverage: false,
};
