const config = require("config.json");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const crypto = require("crypto");
const { Op } = require("sequelize");
const sendEmail = require("../helpers/send-email");
const db = require("../helpers/db");
const Role = require("../helpers/role");

module.exports = {
  getAll,
  getByReportId,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  return await db.Submission.findAll();
}

async function getByReportId(id) {
  return await getSubmissionByReportId(id);
}

async function create(params) {
  // save submission
  const submission = await db.Submission.create(params);
  if (!submission) {
    throw "Submission creation failed";
  }
  return submission;
}

async function update(id, params) {
  const submission = await getSubmission(id);

  // copy params to submission and save
  Object.assign(submission, params);
  await submission.save();
  return submission;
}

async function _delete(id) {
  const submission = await getSubmission(id);
  await submission.destroy();
}

// helper functions
async function getSubmission(id) {
  const submission = await db.Submission.findByPk(id);
  if (!submission) throw "Submission not found";
  return submission;
}

async function getSubmissionByReportId(id) {
  const submission = await db.Submission.findOne({
    where: {
      reportId: id,
    },
  });
  if (!submission) throw "Submission not found";
  return submission;
}
