const { logger } = require("../helpers/logger");
function generateTrackingPixel(email, campaignId = "") {
  const baseUrl = "https://monochrome-compliance.com/api/tracking/pixel";
  const params = new URLSearchParams({
    email,
    ...(campaignId && { campaign: campaignId }),
  });

  logger.logEvent("info", "Tracking pixel generated", {
    action: "GenerateTrackingPixel",
    email,
    campaignId,
    pixelUrl: `${baseUrl}?${params.toString()}`,
  });
  return `${baseUrl}?${params.toString()}`;
}

module.exports = generateTrackingPixel;
