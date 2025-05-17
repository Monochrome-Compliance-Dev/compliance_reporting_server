require("dotenv").config();
const config = require("./helpers/config");

if (!process.env.JWT_SECRET) {
  console.warn(
    "⚠️  JWT_SECRET is not set in the .env file. Authentication may fail."
  );
}
if (!process.env.DB_HOST || !process.env.DB_USER || !process.env.DB_NAME) {
  console.warn(
    "⚠️  Database credentials (DB_HOST, DB_USER, DB_NAME) are missing."
  );
}

require("rootpath")();
const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const cookieParser = require("cookie-parser");
const cors = require("cors");
const errorHandler = require("./middleware/error-handler");
const helmet = require("helmet");
const rateLimit = require("express-rate-limit");

const allowedOrigins = [
  "https://monochrome-compliance.com",
  "http://localhost:3000",
];
app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed by CORS"));
      }
    },
    credentials: true,
  })
);

const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
  standardHeaders: true,
  legacyHeaders: false,
});

app.use("/api/", apiLimiter); // Apply to all API routes

const loginLimiter = rateLimit({
  windowMs: 10 * 60 * 1000,
  max: 5,
  message: "Too many login attempts from this IP, please try again later.",
});

// app.use("/api/users/authenticate", loginLimiter);
app.use("/api/users/forgot-password", loginLimiter);
app.use("/api/users/reset-password", loginLimiter);
// app, use("/api/booking", loginLimiter);

// app.get("*", (req, res) => {
//   res.sendFile(path.join(__dirname, "build", "index.html"));
// });

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
app.use(cookieParser());

const winston = require("./helpers/logger");

app.use(helmet());

if (config.env === "production") {
  app.use(
    helmet.hsts({
      maxAge: 63072000, // 2 years
      includeSubDomains: true,
      preload: true,
    })
  );
}

app.use(
  helmet.contentSecurityPolicy({
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:"],
      fontSrc: ["'self'", "data:"],
      connectSrc: ["'self'", "https://monochrome-compliance.com"],
    },
  })
);
app.disable("x-powered-by");
app.use((req, res, next) => {
  if (
    config.env === "production" &&
    req.headers["x-forwarded-proto"] !== "https"
  ) {
    return res.redirect("https://" + req.headers.host + req.url);
  }
  next();
});

// Add the /api prefix to all routes
app.use("/api/users", require("./users/users.controller"));
app.use("/api/clients", require("./clients/clients.controller"));
app.use("/api/reports", require("./reports/reports.controller"));
app.use("/api/tcp", require("./tcp/tcp.controller"));
app.use("/api/tat", require("./tat/tat.controller"));
app.use("/api/entities", require("./entities/entity.controller"));
app.use("/api/public", require("./public/public.controller"));
app.use("/api/booking", require("./booking/booking.controller"));
app.use("/api/tracking", require("./tracking/tracking.controller"));

// Middleware to log all registered routes
app._router.stack.forEach((middleware) => {
  if (middleware.route) {
    console.log(
      `Route: ${middleware.route.path}, Methods: ${Object.keys(middleware.route.methods).join(", ")}`
    );
  } else if (middleware.name === "router") {
    middleware.handle.stack.forEach((handler) => {
      if (handler.route) {
        console.log(
          `Route: ${handler.route.path}, Methods: ${Object.keys(handler.route.methods).join(", ")}`
        );
      }
    });
  }
});

// swagger docs route
// app.use("/api-docs", require("helpers/swagger"));

// global error handler
app.use(errorHandler);

// start server
const port = config.port;
app.listen(port, () => {
  const message = `✅ Server running in ${config.env} mode on port ${port}`;
  console.log(message);
  winston.info(message);
});

// run this when you need to find the pid to kill
// sudo lsof -i -P | grep LISTEN | grep :$PORT
// mysql.server restart
