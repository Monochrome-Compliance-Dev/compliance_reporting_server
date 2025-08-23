const express = require("express");
const router = express.Router();
const authorise = require("../middleware/authorise");
const validateRequest = require("../middleware/validate-request");
const { productSchema, productUpdateSchema } = require("./product.validator");
const productService = require("./product.service");
const {
  logCreateAudit,
  logReadAudit,
  logUpdateAudit,
  logDeleteAudit,
} = require("../audit/auditHelpers");

// Routes
router.post("/", authorise(), validateRequest(productSchema), createProduct);
router.get("/", authorise(), getProducts);
router.get("/:id", authorise(), getProductById);
router.put(
  "/:id",
  authorise(),
  validateRequest(productUpdateSchema),
  updateProduct
);
router.delete("/:id", authorise(), deleteProduct);

module.exports = router;

// Controller functions
async function createProduct(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const product = await productService.createProduct({
      ...req.body,
      createdBy: userId,
    });
    const productData = product.get({ plain: true });

    await logCreateAudit({
      entity: "Product",
      customerId: null,
      userId,
      req,
      entityId: productData.id,
      reqBody: req.body,
      result: productData,
      details: { name: productData.name, type: productData.type },
      ip,
      device,
    });

    res.status(201).json(productData);
  } catch (err) {
    next(err);
  }
}

async function getProducts(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];

    const products = await productService.getProducts();
    const data = products.map((p) => p.get({ plain: true }));

    await logReadAudit({
      entity: "Product",
      customerId: null,
      userId,
      req,
      result: data,
      action: "Read",
      details: { count: data.length },
      ip,
      device,
    });

    res.json(data);
  } catch (err) {
    next(err);
  }
}

async function getProductById(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const product = await productService.getProductById(id);
    if (!product) return res.status(404).json({ message: "Product not found" });

    const productData = product.get({ plain: true });

    await logReadAudit({
      entity: "Product",
      customerId: null,
      userId,
      req,
      result: productData,
      entityId: id,
      action: "Read",
      details: { name: productData.name, type: productData.type },
      ip,
      device,
    });

    res.json(productData);
  } catch (err) {
    next(err);
  }
}

async function updateProduct(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const before = await productService.getProductById(id);
    if (!before) return res.status(404).json({ message: "Product not found" });
    const beforeData = before.get({ plain: true });

    const updated = await productService.updateProduct(id, {
      ...req.body,
      updatedBy: userId,
    });
    const afterData = updated.get({ plain: true });

    await logUpdateAudit({
      entity: "Product",
      customerId: null,
      userId,
      req,
      reqBody: req.body,
      before: beforeData,
      after: afterData,
      entityId: id,
      details: { updatedFields: Object.keys(req.body) },
      ip,
      device,
    });

    res.json(afterData);
  } catch (err) {
    next(err);
  }
}

async function deleteProduct(req, res, next) {
  try {
    const userId = req.auth.id;
    const ip = req.ip;
    const device = req.headers["user-agent"];
    const id = req.params.id;

    const before = await productService.getProductById(id);
    if (!before) return res.status(404).json({ message: "Product not found" });
    const beforeData = before.get({ plain: true });

    await productService.deleteProduct(id);

    await logDeleteAudit({
      entity: "Product",
      customerId: null,
      userId,
      req,
      before: beforeData,
      entityId: id,
      details: { name: beforeData.name, type: beforeData.type },
      ip,
      device,
    });

    res.status(204).send();
  } catch (err) {
    next(err);
  }
}
