const nodemailer = require("nodemailer");
const config = require("../config.json");
const { logger } = require("../helpers/logger");

module.exports = { sendEmail, sendAttachmentEmail };

// Updated to support optional cc and bcc
async function sendEmail({
  to,
  subject,
  html,
  from = config.emailFrom,
  cc,
  bcc,
}) {
  const transporter = nodemailer.createTransport(config.smtpOptions);
  try {
    await transporter.sendMail({
      from,
      to,
      cc,
      bcc,
      subject,
      html,
    });
    logger.logEvent("info", "Email sent", {
      action: "SendEmail",
      to,
      subject,
    });
    return null;
  } catch (error) {
    logger.logEvent("error", "Email send failed", {
      action: "SendEmail",
      to,
      subject,
      error: error.message,
    });
    console.error("Error sending email:", error);
    return error;
  }
}

async function sendAttachmentEmail(req, res) {
  // Parse req.body fields explicitly
  const to = req.body.to;
  const from = req.body.from || config.emailFrom;
  const subject = req.body.subject;
  const html = req.body.html;

  if (!to || !subject || !html || !req.file) {
    return res
      .status(400)
      .json({ message: "Missing required fields or attachment" });
  }

  const transporter = nodemailer.createTransport(config.smtpOptions);

  await transporter.sendMail({
    from,
    to,
    subject,
    html,
    attachments: [
      {
        filename: req.file.originalname,
        path: req.file.path, // Use the file path saved by multer
        contentType: req.file.mimetype,
      },
    ],
  });
  logger.logEvent("info", "Attachment email sent", {
    action: "SendAttachmentEmail",
    to,
    subject,
    fileName: req.file?.originalname,
  });

  res.json({ message: "Email sent successfully" });
}
