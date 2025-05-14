const express = require("express");
const router = express.Router();
const Joi = require("joi");
const { sendEmail } = require("../helpers/send-email");
const logger = require("../helpers/logger");

// routes
router.post("/send-email", sendEmailPrep);

module.exports = router;

function sendEmailPrep(req, res) {
  const schema = Joi.object({
    name: Joi.string().required(),
    email: Joi.string().email().required(),
    subject: Joi.string().required(),
    message: Joi.string().required(),
  });

  const { error } = schema.validate(req.body);
  if (error) return res.status(400).send(error.details[0].message);

  sendEmail({
    to: "darryll@stillproud.com",
    subject: req.body.subject,
    html: `<p>Email sent from  : ${req.body.email}. ${req.body.message}</p>`,
  })
    .then(() => {
      logger.info("Email sent successfully", {
        email: req.body.email,
        subject: req.body.subject,
      });
      res.json({ status: 200, message: "Email sent successfully" });
    })
    .catch((error) => {
      logger.error("Error sending email", {
        error: error.message,
        stack: error.stack,
      });
      res.status(500).json({ status: 500, message: "Failed to send email" });
    });
}
