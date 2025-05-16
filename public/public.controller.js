const express = require("express");
const router = express.Router();
const Joi = require("joi");
const { sendEmail } = require("../helpers/send-email");
const logger = require("../helpers/logger");
const generateTrackingPixel = require("../helpers/generateTrackingPixel");

const contactSchema = Joi.object({
  name: Joi.string().required(),
  email: Joi.string().email().required(),
  subject: Joi.string().required(),
  message: Joi.string().required(),
  to: Joi.string().email().required(),
  from: Joi.string().email().optional(),
  cc: Joi.string().email().optional(),
  bcc: Joi.string().email().optional(),
});

const bookingSchema = Joi.object({
  name: Joi.string().optional(),
  email: Joi.string().email().optional(),
  subject: Joi.string().required(),
  message: Joi.string().required(),
  to: Joi.string().email().required(),
  from: Joi.string().email().optional(),
  cc: Joi.string().email().optional(),
  bcc: Joi.string().email().optional(),
});

// routes
router.post("/send-email", sendEmailPrep);

module.exports = router;

function sendEmailPrep(req, res) {
  console.log("Received email request", req.body);
  const isBooking = req.body.subject?.toLowerCase().includes("booking");
  const schema = isBooking ? bookingSchema : contactSchema;

  const { error } = schema.validate(req.body);
  if (error) {
    console.error("error", error);
    return res.status(400).send(error.details[0].message);
  }

  const trackingPixel = generateTrackingPixel(req.body.email, "contact-form");

  sendEmail({
    to: req.body.to,
    subject: req.body.subject,
    html: `
      <div style="font-family: sans-serif; line-height: 1.6; color: #333; padding: 1rem;">
        <img src="https://monochrome-compliance.com/assets/logo.png" alt="Monochrome Logo" style="margin-bottom: 1rem;" />
        <h2>${req.body.subject}</h2>
       ${
         req.body.name || req.body.email
           ? `<p><strong>From:</strong> ${req.body.name || "Not provided"} (${req.body.email || "Not provided"})</p>`
           : ""
       }
        <p>${req.body.message.replace(/\n/g, "<br>")}</p>
        <hr style="margin: 2rem 0;" />
        <footer style="font-size: 0.85rem; color: #666;">
          <p>This message was sent via the Monochrome Compliance platform.</p>
          <p><em>Disclaimer: This email and any attachments are confidential and intended solely for the use of the individual or entity to whom they are addressed. If you have received this email in error, please notify the sender and delete it from your system.</em></p>
          <p style="margin-top: 1rem;">
            If you no longer wish to receive these emails, you can <a href="https://monochrome-compliance.com/unsubscribe" style="color: #666;">unsubscribe here</a>.
          </p>
          <p style="margin-top: 1rem;">
          We use tracking technology to understand when our messages are opened. This helps us improve communication and customer experience.
          </p>
        </footer>
        <img src="${trackingPixel}" alt="" width="1" height="1" style="opacity: 0;" />
      </div>
    `,
    from: req.body.from || req.body.email,
    cc: req.body.cc,
    bcc: req.body.bcc,
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
