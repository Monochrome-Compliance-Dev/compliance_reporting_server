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
  return await db.Payment.findAll();
}

async function getByReportId(id) {
  return await getPaymentByReportId(id);
}

async function create(params) {
  // save payment
  const payment = await db.Payment.create(params);
  if (!payment) {
    throw "Payment creation failed";
  }
  // return saved payment
  return payment;
}

async function update(id, params) {
  const payment = await getPayment(id);
  console.log("Payment found:", payment); // Debugging line
  // copy params to payment and save
  Object.assign(payment, params);
  await payment.save();
  return payment;
}

async function _delete(id) {
  const payment = await getPayment(id);
  await payment.destroy();
}

// helper functions
async function getPayment(id) {
  const payment = await db.Payment.findByPk(id);
  if (!payment) throw "Payment not found";
  return payment;
}
async function getPaymentByReportId(id) {
  const payment = await db.Payment.findOne({
    where: {
      reportId: id,
    },
  });
  if (!payment) throw "Payment not found";
  return payment;
}
