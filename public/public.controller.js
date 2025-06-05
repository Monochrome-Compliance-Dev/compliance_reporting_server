const express = require("express");
const router = express.Router();
const Joi = require("joi");
const upload = require("../middleware/upload");
const {
  sendEmail,
  sendAttachmentEmail,
  sendSes,
} = require("../helpers/send-email");
const { scanFile } = require("../middleware/virus-scan");
const path = require("path");
const fs = require("fs");
const { logger } = require("../helpers/logger");
const generateTrackingPixel = require("../helpers/generateTrackingPixel");

const contactSchema = Joi.object({
  topic: Joi.string().required(),
  name: Joi.string().required(),
  email: Joi.string().email().required(),
  subject: Joi.string().required(),
  company: Joi.string().required(),
  message: Joi.string().optional().required(),
  to: Joi.string().email().required(),
  from: Joi.string().email().required(),
  cc: Joi.string().email().optional(),
  bcc: Joi.string().email().optional(),
});

const bookingSchema = Joi.object({
  topic: Joi.string().required(),
  name: Joi.string().required(),
  email: Joi.string().email().required(),
  company: Joi.string().required(),
  subject: Joi.string().required(),
  date: Joi.string().isoDate().required(),
  time: Joi.string().required(),
  reason: Joi.string().required(),
  to: Joi.string().email().required(),
  from: Joi.string().email().optional(),
  cc: Joi.string().email().optional(),
  bcc: Joi.string().email().optional(),
});

// Ensure tmpUploads directory exists before handling uploads
const tmpUploadPath = path.join(__dirname, "../tmpUploads");
if (!fs.existsSync(tmpUploadPath)) {
  fs.mkdirSync(tmpUploadPath, { recursive: true });
}

// routes
router.post("/send-email", sendEmailPrep);
router.post("/send-ses-email", sendEmailSesPrep);

router.post(
  "/send-attachment-email",
  upload.single("attachment"),
  async (req, res, next) => {
    try {
      // console.log("[DEBUG] File received by route:", req.file);
      if (!req.file || !req.file.path) {
        logger.logEvent("warn", "Missing file or file path in request", {
          action: "SendAttachmentEmail",
        });
      }
      const filePath = req.file.path;
      const ext = path.extname(filePath).toLowerCase();
      await scanFile(filePath, ext, req.file.originalname);
      // await sendAttachmentEmail(req, res);
      await sendSes({
        to: req.body.to,
        subject:
          req.body.subject || req.body.topic + " " + new Date().toISOString(),
        html: `
      <ul style="font-family: sans-serif; font-size: 1rem; color: #333;">
        <li><strong>subject:</strong> ${req.body.subject || "Not provided"}</li>
        <li><strong>message:</strong> ${req.body.message || "Not provided"}</li>
        <li><strong>name:</strong> ${req.body.name || "Not provided"}</li>
        <li><strong>email:</strong> ${req.body.email || "Not provided"}</li>
        <li><strong>date:</strong> ${req.body.date || "Not provided"}</li>
        <li><strong>time:</strong> ${req.body.time || "Not provided"}</li>
        <li><strong>company:</strong> ${req.body.company || "Not provided"}</li>
        <li><strong>from:</strong> ${req.body.from || req.body.email || "Not provided"}</li>
        <li><strong>cc:</strong> ${req.body.cc || "Not provided"}</li>
        <li><strong>bcc:</strong> ${req.body.bcc || "Not provided"}</li>
      </ul>
    `,
        from: req.body.from || req.body.email,
        cc: req.body.cc,
        bcc: req.body.bcc,
        attachments: [
          {
            filename: req.file.originalname,
            path: req.file.path, // Use the file path saved by multer
            contentType: req.file.mimetype,
          },
        ],
      });

      // Delete file after scan and successful email send
      fs.unlink(filePath, (err) => {
        if (err)
          logger.logEvent("warn", "Failed to delete temp file", {
            action: "SendAttachmentEmail",
            file: filePath,
            error: err.message,
          });
      });
      if (!res.headersSent) {
        res.json({ message: "Email sent successfully" });
      }
    } catch (error) {
      logger.logEvent("error", "Error sending email with attachment", {
        action: "SendAttachmentEmail",
        error: error.message,
        stack: error.stack,
      });
      if (!res.headersSent) {
        next(error);
      }
    }
  }
);

module.exports = router;

function sendEmailSesPrep(req, res) {
  const isBooking = req.body.subject?.toLowerCase().includes("booking");
  const schema = isBooking ? bookingSchema : contactSchema;

  const { error } = schema.validate(req.body);
  if (error) {
    console.error("error", error);
    return res.status(400).send(error.details[0].message);
  }

  const trackingPixel = generateTrackingPixel(req.body.email, "contact-form");

  sendSes({
    to: req.body.to,
    subject: req.body.topic + " " + new Date().toISOString(),
    html: `
      <ul style="font-family: sans-serif; font-size: 1rem; color: #333;">
        <li><strong>subject:</strong> ${req.body.subject || "Not provided"}</li>
        <li><strong>message:</strong> ${req.body.message || "Not provided"}</li>
        <li><strong>name:</strong> ${req.body.name || "Not provided"}</li>
        <li><strong>email:</strong> ${req.body.email || "Not provided"}</li>
        <li><strong>date:</strong> ${req.body.date || "Not provided"}</li>
        <li><strong>time:</strong> ${req.body.time || "Not provided"}</li>
        <li><strong>company:</strong> ${req.body.company || "Not provided"}</li>
        <li><strong>from:</strong> ${req.body.from || req.body.email || "Not provided"}</li>
        <li><strong>cc:</strong> ${req.body.cc || "Not provided"}</li>
        <li><strong>bcc:</strong> ${req.body.bcc || "Not provided"}</li>
      </ul>
    `,
    from: req.body.from || req.body.email,
    cc: req.body.cc,
    bcc: req.body.bcc,
  })
    .then(() => {
      logger.logEvent("info", "Email trigger sent to SES successfully", {
        action: "SendEmail",
        email: req.body.email,
        subject: req.body.subject,
      });
      res.json({ status: 200, message: "Email sent successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error sending email", {
        action: "SendEmail",
        error: error.message,
        stack: error.stack,
      });
      res.status(500).json({ status: 500, message: "Failed to send email" });
    });
}

function sendEmailPrep(req, res) {
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
      logger.logEvent("info", "Email sent successfully", {
        action: "SendEmail",
        email: req.body.email,
        subject: req.body.subject,
      });
      res.json({ status: 200, message: "Email sent successfully" });
    })
    .catch((error) => {
      logger.logEvent("error", "Error sending email", {
        action: "SendEmail",
        error: error.message,
        stack: error.stack,
      });
      res.status(500).json({ status: 500, message: "Failed to send email" });
    });
}
