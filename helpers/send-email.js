const nodemailer = require("nodemailer");
const config = require("../config.json");

module.exports = { sendEmail, sendAttachmentEmail };

async function sendEmail({ to, subject, html, from = config.emailFrom }) {
  const transporter = nodemailer.createTransport(config.smtpOptions);
  await transporter.sendMail({ from, to, subject, html });
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

  res.json({ message: "Email sent successfully" });
}
