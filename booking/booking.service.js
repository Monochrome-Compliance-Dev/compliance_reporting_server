const db = require("../helpers/db");
const logger = require("../helpers/logger");

module.exports = {
  getAll,
  getById,
  create,
  update,
  delete: _delete,
};

async function getAll() {
  logger.info("[BookingService] Fetching all bookings");
  return await db.Booking.findAll();
}

async function getById(id) {
  logger.info(`[BookingService] Fetching booking by ID: ${id}`);
  return await getBooking(id);
}

async function create(params) {
  logger.info(`[BookingService] Creating booking`);
  // Ensure createdBy is not explicitly passed or logged
  if ("createdBy" in params) {
    delete params.createdBy;
  }
  const booking = await db.Booking.create(params);
  logger.info(`Booking created with ID ${booking.id}`);
  return booking;
}

async function update(id, params) {
  logger.info(`[BookingService] Updating booking ID: ${id}`);
  // Ensure updatedBy is not explicitly passed or logged
  if ("updatedBy" in params) {
    delete params.updatedBy;
  }
  const booking = await getBooking(id);
  Object.assign(booking, params);
  await booking.save();
  logger.info(`Booking with ID ${id} updated`);
  return booking;
}

async function _delete(id) {
  logger.info(`[BookingService] Deleting booking ID: ${id}`);
  const booking = await getBooking(id);
  await booking.destroy();
  logger.info(`Booking with ID ${id} deleted`);
}

// helper function
async function getBooking(id) {
  const booking = await db.Booking.findByPk(id);
  if (!booking) throw { status: 404, message: "Booking not found" };
  return booking;
}
