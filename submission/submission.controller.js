const express = require("express");
const router = express.Router();
const Joi = require("joi");
const validateRequest = require("../middleware/validate-request");
const authorise = require("../middleware/authorise");
const Role = require("../helpers/role");
const submissionService = require("./submission.service");

// routes
router.get("/", authorise(), getAll);
router.get("/:id", authorise(), getByReportId);
router.post("/", authorise(), createSchema, create);
router.put("/:id", authorise(), updateSchema, update);
router.delete("/:id", authorise(), _delete);

module.exports = router;

function getAll(req, res, next) {
  submissionService
    .getAll()
    .then((entities) => res.json(entities))
    .catch(next);
}

function getByReportId(req, res, next) {
  submissionService
    .getByReportId(req.params.id)
    .then((submission) =>
      submission ? res.json(submission) : res.sendStatus(404)
    )
    .catch(next);
}

function createSchema(req, res, next) {
  const schema = Joi.object({
    ReportComments: Joi.string(),
    SubmitterFirstName: Joi.string(),
    SubmitterLastName: Joi.string(),
    SubmitterPosition: Joi.string(),
    SubmitterPhoneNumber: Joi.string(),
    SubmitterEmail: Joi.string(),
    ApproverFirstName: Joi.string(),
    ApproverLastName: Joi.string(),
    ApproverPosition: Joi.string(),
    ApproverPhoneNumber: Joi.string(),
    ApproverEmail: Joi.string(),
    ApprovalDate: Joi.string(),
    PrincipalGoverningBodyName: Joi.string(),
    PrincipalGoverningBodyDescription: Joi.string(),
    ResponsibleMemberDeclaration: Joi.string(),
    createdBy: Joi.number().required(),
    updatedBy: Joi.number(),
    submittedDate: Joi.string(),
    submittedBy: Joi.number(),
    statusUpdatedDate: Joi.string(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function create(req, res, next) {
  submissionService
    .create(req.body)
    .then((submission) => res.json(submission))
    .catch((error) => {
      console.error("Error creating submission:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}

function updateSchema(req, res, next) {
  const schema = Joi.object({
    ReportComments: Joi.string(),
    SubmitterFirstName: Joi.string(),
    SubmitterLastName: Joi.string(),
    SubmitterPosition: Joi.string(),
    SubmitterPhoneNumber: Joi.string(),
    SubmitterEmail: Joi.string(),
    ApproverFirstName: Joi.string(),
    ApproverLastName: Joi.string(),
    ApproverPosition: Joi.string(),
    ApproverPhoneNumber: Joi.string(),
    ApproverEmail: Joi.string(),
    ApprovalDate: Joi.string(),
    PrincipalGoverningBodyName: Joi.string(),
    PrincipalGoverningBodyDescription: Joi.string(),
    ResponsibleMemberDeclaration: Joi.string(),
    createdBy: Joi.number().required(),
    updatedBy: Joi.number().required(),
    submittedDate: Joi.string(),
    submittedBy: Joi.number().required(),
    statusUpdatedDate: Joi.string(),
    reportId: Joi.number().required(),
  });
  validateRequest(req, next, schema);
}

function update(req, res, next) {
  submissionService
    .update(req.params.id, req.body)
    .then((submission) => res.json(submission))
    .catch(next);
}

function _delete(req, res, next) {
  submissionService
    .delete(req.params.id)
    .then(() => res.json({ message: "Submission deleted successfully" }))
    .catch((error) => {
      console.error("Error deleting submission:", error); // Log the error details
      next(error); // Pass the error to the global error handler
    });
}
