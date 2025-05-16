const express = require("express");
const router = express.Router();
const validateRequest = require("../middleware/validate-request");
const bookingService = require("./booking.service");
const logger = require("../helpers/logger");
const { bookingSchema, bookingUpdateSchema } = require("./booking.validator");

// routes
router.get("/", getAll);
router.get("/:id", getById);
router.post("/", validateRequest(bookingSchema), create); // no authorisation required
router.put("/:id", validateRequest(bookingUpdateSchema), update);
router.delete("/:id", _delete);

module.exports = router;

function getAll(req, res, next) {
  logger.info(
    `[BookingController] Retrieved all bookings by user ${req?.user?.id || "unknown"}`
  );
  bookingService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  logger.info(
    `[BookingController] Retrieved booking by ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  bookingService
    .getById(req.params.id)
    .then((booking) => (booking ? res.json(booking) : res.sendStatus(404)))
    .catch(next);
}

function create(req, res, next) {
  logger.info(`[BookingController] Created new booking`);
  bookingService
    .create(req.body)
    .then((booking) => {
      logger.info(`Booking created via API: ${booking.id}`);
      res.json(booking);
    })
    .catch((error) => {
      logger.error("Error creating booking:", error);
      next(error);
    });
}

function update(req, res, next) {
  logger.info(
    `[BookingController] Updated booking ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  bookingService
    .update(req.params.id, req.body)
    .then((booking) => {
      logger.info(`Booking updated via API: ${req.params.id}`);
      res.json(booking);
    })
    .catch(next);
}

function _delete(req, res, next) {
  logger.info(
    `[BookingController] Deleted booking ID: ${req.params.id} by user ${req?.user?.id || "unknown"}`
  );
  bookingService
    .delete(req.params.id)
    .then(() => {
      logger.info(`Booking deleted via API: ${req.params.id}`);
      res.json({ message: "Booking deleted successfully" });
    })
    .catch((error) => {
      logger.error("Error deleting booking:", error);
      next(error);
    });
}
