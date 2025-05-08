const multer = require("multer");

// Use memory storage so file buffer is available directly (for email attachments)
const storage = multer.memoryStorage();

const upload = multer({ storage });

module.exports = upload;
