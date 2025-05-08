const nodemailer = require("nodemailer");
const config = require("../config.json");

module.exports = { sendEmail, sendAttachmentEmail };

async function sendEmail({ to, subject, html, from = config.emailFrom }) {
  const transporter = nodemailer.createTransport(config.smtpOptions);
  await transporter.sendMail({ from, to, subject, html });
}

async function sendAttachmentEmail(req, res) {
  const { to, from, subject, html } = req.body;
  const pdfBuffer = req.file.buffer;

  const transporter = nodemailer.createTransport(config.smtpOptions);

  await transporter.sendMail({
    from,
    to,
    subject,
    html,
    attachments: [
      {
        filename: "entity-report-summary.pdf",
        content: pdfBuffer,
      },
    ],
  });

  res.json({ message: "Email sent" });
}
