const express = require("express");
const router = express.Router();
const { logger } = require("../helpers/logger");
const path = require("path");
const db = require("../db/database");

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
    logger.logEvent("warn", "Geo lookup failed", {
      action: "GeoLookup",
      ip,
      error: err.message,
    });
  }

  logger.logEvent("info", "Email open tracked", {
    action: "EmailOpen",
    email: email || "unknown",
    ip,
    userAgent: ua,
    referer,
    origin,
    host,
    campaignId,
    time,
  });

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

router.post("/honeypot", async (req, res) => {
  const ip =
    req.headers["x-forwarded-for"] || req.socket.remoteAddress || "unknown";
  const ua = req.headers["user-agent"] || "unknown";
  const referer = req.headers["referer"] || "unknown";
  const origin = req.headers["origin"] || "unknown";
  const host = req.headers["host"] || "unknown";

  try {
    await db.Tracking.create({
      ipAddress: ip,
      userAgent: ua,
      referer,
      origin,
      host,
      campaignId: "honeypot",
    });

    res.status(204).end();
  } catch (err) {
    logger.logEvent("error", "Failed to log honeypot attempt", {
      action: "LogHoneypotFail",
      error: err.message,
      ip,
    });
    res.status(500).json({ message: "Failed to record honeypot event" });
  }
});

module.exports = router;
