const { xeroApi } = require("./xeroApi");
require("dotenv").config();

const refreshToken = async () => {
  const refreshToken = process.env.XERO_REFRESH_TOKEN; // store this somewhere safe!

  try {
    const response = await xeroApi.post(
      "/connect/token",
      new URLSearchParams({
        grant_type: "refresh_token",
        refresh_token: refreshToken,
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
      }
    );

    console.log("New Access Token:", response.data.access_token);
    console.log("New Refresh Token:", response.data.refresh_token);

    // Store the new tokens somewhere safe (.env, DB, etc.)
    // e.g., console.log to copy for now:
    console.log("💡 Save the new refresh token in your .env file!");
  } catch (err) {
    console.error("Error refreshing token:", err.response?.data || err.message);
  }
};

refreshToken();
