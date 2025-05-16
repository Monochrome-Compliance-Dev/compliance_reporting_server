const nodemailer = require("nodemailer");
const config = require("../config.json");
const winston = require("../helpers/logger");

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
    winston.info(`Email sent to ${to} with subject "${subject}"`);
    return null;
  } catch (error) {
    winston.error("Email send failed", { error });
    console.error("Error sending email:", error);
    return error;
  }
}

async function sendAttachmentEmail(req, res) {
  console.log("Processing email with attachment...");
  console.log("Form data (req.body):", req.body);
  console.log("Uploaded file (req.file):", req.file);

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

  winston.info(`Attachment email sent to ${to} with subject "${subject}"`);

  res.json({ message: "Email sent successfully" });
}
