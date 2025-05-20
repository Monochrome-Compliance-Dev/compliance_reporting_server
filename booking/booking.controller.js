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
  logger.logEvent("info", "Retrieved all bookings", {
    action: "GetAllBookings",
    userId: req?.user?.id || "unknown",
    ip: req.ip,
  });
  bookingService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getById(req, res, next) {
  logger.logEvent("info", "Retrieved booking by ID", {
    action: "GetBooking",
    bookingId: req.params.id,
    userId: req?.user?.id || "unknown",
    ip: req.ip,
  });
  bookingService
    .getById(req.params.id)
    .then((booking) => (booking ? res.json(booking) : res.sendStatus(404)))
    .catch(next);
}

function create(req, res, next) {
  logger.logEvent("info", "Creating new booking", {
    action: "CreateBooking",
    userAgent: req.headers["user-agent"],
  });
  bookingService
    .create(req.body)
    .then((booking) => {
      logger.logEvent("info", "Booking created", {
        action: "CreateBooking",
        bookingId: booking.id,
      });
      res.json(booking);
    })
    .catch((error) => {
      logger.logEvent("error", "Error creating booking", {
        action: "CreateBooking",
        error: error.message,
      });
      next(error);
    });
}

function update(req, res, next) {
  logger.logEvent("info", "Updating booking", {
    action: "UpdateBooking",
    bookingId: req.params.id,
    userId: req?.user?.id || "unknown",
  });
  bookingService
    .update(req.params.id, req.body)
    .then((booking) => {
      logger.logEvent("info", "Booking updated", {
        action: "UpdateBooking",
        bookingId: req.params.id,
      });
      res.json(booking);
    })
    .catch(next);
}

function _delete(req, res, next) {
  logger.logEvent("info", "Deleting booking", {
    action: "DeleteBooking",
    bookingId: req.params.id,
    userId: req?.user?.id || "unknown",
  });
  bookingService
    .delete(req.params.id)
    .then(() => {
      logger.logEvent("info", "Booking deleted", {
        action: "DeleteBooking",
        bookingId: req.params.id,
      });
      res.json({ message: "Booking deleted successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error deleting booking", {
        action: "DeleteBooking",
        error: error.message,
      });
      next(error);
    });
}
