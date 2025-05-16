const express = require("express");
const router = express.Router();
const Joi = require("joi");
const logger = require("../helpers/logger");
const path = require("path");
const db = require("../helpers/db");

// routes
// Log pixel loads to logger
router.get("/pixel", async (req, res) => {
  const { email } = req.query;
  const ip =
    req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown";
  const ua = req.headers["user-agent"] || "unknown";
  const referer = req.headers["referer"] || "unknown";
  const origin = req.headers["origin"] || "unknown";
  const host = req.headers["host"] || "unknown";
  const campaignId = req.query.campaign || null;
  const time = new Date().toISOString();

  const axios = require("axios");
  let geo = {};

  try {
    const { data } = await axios.get(`https://ipapi.co/${ip}/json`);
    geo = {
      geoCountry: data.country_name,
      geoRegion: data.region,
      geoCity: data.city,
    };
  } catch (err) {
    logger.warn(`Geo lookup failed for ${ip}: ${err.message}`);
  }

  logger.info(
    `[TrackingPixel] Open tracked for ${email || "unknown"} from IP ${ip} at ${time}`
  );

  await db.Tracking.create({
    email,
    ipAddress: ip,
    userAgent: ua,
    referer,
    origin,
    host,
    campaignId,
    ...geo,
  });

  res.sendFile(path.resolve(__dirname, "../public/pixel.png"));
});

module.exports = router;
