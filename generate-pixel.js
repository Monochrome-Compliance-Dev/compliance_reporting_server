// generate-pixel.js
const fs = require("fs");
const path = require("path");

const base64 =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR4nGNgYAAAAAMAASsJTYQAAAAASUVORK5CYII=";

// Adjust to your actual project structure
const outputPath = path.resolve(__dirname, "/public/pixel.png");

fs.writeFileSync(outputPath, Buffer.from(base64, "base64"));
console.log("✅ Pixel written to:", outputPath);
