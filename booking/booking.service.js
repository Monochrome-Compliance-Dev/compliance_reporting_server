const db = require("../db/database");
const { logger } = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  logger.logEvent("info", "Fetching all bookings", {
    action: "GetAllBookings",
  });
  return await db.Booking.findAll();
}

async function getById(id) {
  logger.logEvent("info", "Fetching booking by ID", {
    action: "GetBooking",
    bookingId: id,
  });
  return await getBooking(id);
}

async function create(params) {
  logger.logEvent("info", "Creating booking", {
    action: "CreateBooking",
  });
  // Ensure createdBy is not explicitly passed or logged
  if ("createdBy" in params) {
    delete params.createdBy;
  }
  const booking = await db.Booking.create(params);
  logger.logEvent("info", "Booking created", {
    action: "CreateBooking",
    bookingId: booking.id,
  });
  return booking;
}

async function update(id, params) {
  logger.logEvent("info", "Updating booking", {
    action: "UpdateBooking",
    bookingId: id,
  });
  // Ensure updatedBy is not explicitly passed or logged
  if ("updatedBy" in params) {
    delete params.updatedBy;
  }
  const booking = await getBooking(id);
  Object.assign(booking, params);
  await booking.save();
  logger.logEvent("info", "Booking updated", {
    action: "UpdateBooking",
    bookingId: id,
  });
  return booking;
}

async function _delete(id) {
  logger.logEvent("info", "Deleting booking", {
    action: "DeleteBooking",
    bookingId: id,
  });
  const booking = await getBooking(id);
  await booking.destroy();
  logger.logEvent("info", "Booking deleted", {
    action: "DeleteBooking",
    bookingId: id,
  });
}

// helper function
async function getBooking(id) {
  const booking = await db.Booking.findByPk(id);
  if (!booking) throw { status: 404, message: "Booking not found" };
  return booking;
}
