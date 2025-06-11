const nodemailer = require("nodemailer");
const { logger } = require("../helpers/logger");

module.exports = { sendEmail, sendAttachmentEmail, sendSes };

const smtpOptions = {
  host: process.env.SMTP_HOST,
  port: Number(process.env.SMTP_PORT),
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS,
  },
  secure: true, // true for port 465, false for other ports
};

const emailFrom = process.env.SMTP_FROM;

async function sendSes({
  to,
  subject,
  html,
  from,
  cc,
  bcc,
  name,
  attachments,
}) {
  const transporter = nodemailer.createTransport(smtpOptions);
  try {
    console.log("Sending SES email to:", to);
    transporter.sendMail({
      from: from || emailFrom,
      name: name || "Not provided",
      to: "contact@monochrome-compliance.com",
      subject: subject,
      html,
      attachments,
    });
    logger.logEvent("info", "SES Email sent", {
      action: "SendSesEmail",
      to,
      subject,
    });
    return null;
  } catch (error) {
    logger.logEvent("error", "SES Email send failed", {
      action: "SendSesEmail",
      to,
      subject,
      error: error.message,
    });
    console.error("Error sending SES email:", error);
    return error;
  }
}

// Updated to support optional cc and bcc
async function sendEmail({ to, subject, html, from, cc, bcc }) {
  const transporter = nodemailer.createTransport(smtpOptions);
  try {
    await transporter.sendMail({
      from: from || emailFrom,
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
  const from = req.body.from || emailFrom;
  const subject = req.body.subject;
  const html = req.body.html;

  if (!to || !subject || !html || !req.file) {
    return res
      .status(400)
      .json({ message: "Missing required fields or attachment" });
  }

  const transporter = nodemailer.createTransport(smtpOptions);

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
