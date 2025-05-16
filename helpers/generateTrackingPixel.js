function generateTrackingPixel(email, campaignId = "") {
  const baseUrl = "https://monochrome-compliance.com/api/tracking/pixel";
  const params = new URLSearchParams({
    email,
    ...(campaignId && { campaign: campaignId }),
  });

  return `${baseUrl}?${params.toString()}`;
}

module.exports = generateTrackingPixel;
