const axios = require("axios");

const trackedFields = [
  "isTcp",
  "tcpExclusionComment",
  "peppolEnabled",
  "rcti",
  "creditCardPayment",
  "creditCardNumber",
  "partialPayment",
  "paymentTerm",
  "excludedTcp",
  "explanatoryComments1",
  "isSb",
  "paymentTime",
  "explanatoryComments2",
];

async function auditTracking(req, res, next) {
  console.warn(
    "Audit tracking middleware called",
    req.method,
    req.path,
    req.body,
    req.auth
  );
  //   const userId = req.auth?.id;
  //   const clientId = req.auth?.clientId;

  //   req.auditChanges = async ({ tcpId, step, oldData, newData }) => {
  //     const changes = [];

  //     trackedFields.forEach((field) => {
  //       if (oldData[field] !== newData[field]) {
  //         changes.push({
  //           tcpId,
  //           field_name: field,
  //           old_value: oldData[field] ? String(oldData[field]) : null,
  //           new_value: newData[field] ? String(newData[field]) : null,
  //           step,
  //           user_id: userId,
  //         });
  //       }
  //     });

  //     if (!changes.length) return;

  try {
    await axios.post(
      `${process.env.API_BASE_URL || "http://localhost:4000/api"}/audit`,
      req,
      {
        headers: {
          Authorization: `Bearer ${req.headers.authorization?.split(" ")[1]}`,
        },
      }
    );
  } catch (error) {
    console.error("Audit API insert failed", error.message);
  }
  //   };

  next();
}

module.exports = auditTracking;
