const auditService = require("@/audit/audit.service");
const { logger } = require("@/helpers/logger");
const xeroService = require("./xero.service");

module.exports = {
  connect,
  callback,
  getOrganisations,
  selectOrganisations,
  removeOrganisation,
  startImport,
  getStatus,
  getReadiness,
  getImportExceptions,
  getImportExceptionsSummary,
  downloadImportExceptionsCsv,
};
/**
 * GET /api/v2/ptrs/:id/xero/import/exceptions/summary
 */
async function getImportExceptionsSummary(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const ptrsId = req.params.id;

  try {
    const count = await xeroService.getImportExceptionsSummary({
      customerId,
      ptrsId,
    });

    return res.status(200).json({
      status: "success",
      data: { count },
    });
  } catch (err) {
    return next(err);
  }
}
/**
 * GET /api/v2/ptrs/:id/xero/import/exceptions
 */
async function getImportExceptions(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const ptrsId = req.params.id;

  try {
    const rows = await xeroService.getImportExceptions({ customerId, ptrsId });
    return res.status(200).json({
      status: "success",
      data: {
        count: rows.length,
        rows,
      },
    });
  } catch (err) {
    return next(err);
  }
}

/**
 * GET /api/v2/ptrs/:id/xero/import/exceptions.csv
 */
async function downloadImportExceptionsCsv(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const ptrsId = req.params.id;

  try {
    const csv = await xeroService.getImportExceptionsCsv({
      customerId,
      ptrsId,
    });

    res.setHeader("Content-Type", "text/csv");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=ptrs_${ptrsId}_import_exceptions.csv`,
    );

    return res.status(200).send(csv);
  } catch (err) {
    return next(err);
  }
}

/**
 * POST /api/v2/ptrs/:id/xero/connect
 * Response: { authUrl }
 */
async function connect(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const result = await xeroService.connect({
      customerId,
      ptrsId,
      userId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroConnect",
      entity: "PtrsXeroConnection",
      entityId: ptrsId,
      details: { ptrsId },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.("error", "Error connecting to Xero (PTRS v2)", {
      action: "PtrsV2XeroConnect",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/app/xero/callback
 * (legacy/dev) GET /api/app/ptrs/:id/xero/callback
 *
 * Xero redirects here after consent (or cancel/error). We must exchange code -> tokens,
 * fetch connections, persist tokens per tenant, then redirect back to FE.
 *
 * IMPORTANT: OAuth redirect URIs must be static, so ptrsId is derived from `state`
 * when not present as a route param.
 */
async function callback(req, res, next) {
  const { code, state, error, error_description } = req.query || {};

  // If invoked via legacy/dev route, we might get :id. Static callback has no :id.
  const ptrsIdFromParams = req.params?.id || null;

  // Parse state as best-effort to obtain ptrsId/customerId for redirect, even on error/cancel.
  let parsedState = null;
  try {
    parsedState = state
      ? JSON.parse(Buffer.from(String(state), "base64url").toString("utf8"))
      : null;
  } catch (_) {
    parsedState = null;
  }

  const ptrsIdFromState = parsedState?.ptrsId || null;
  const effectivePtrsId = ptrsIdFromParams || ptrsIdFromState || null;

  const frontEndBase = process.env.FRONTEND_URL || "http://localhost:3000";
  const feCallbackBase = `${frontEndBase}/app/ptrs/xero/callback`;

  try {
    // Xero sends `error=access_denied` when the user cancels.
    if (error) {
      const qs = new URLSearchParams();
      if (effectivePtrsId) qs.set("ptrsId", effectivePtrsId);
      qs.set("error", String(error));
      if (error_description)
        qs.set("error_description", String(error_description));
      return res.redirect(`${feCallbackBase}?${qs.toString()}`);
    }

    if (!code) {
      const qs = new URLSearchParams();
      if (effectivePtrsId) qs.set("ptrsId", effectivePtrsId);
      qs.set("error", "missing_code");
      qs.set("error_description", "Missing authorisation code");
      return res.redirect(`${feCallbackBase}?${qs.toString()}`);
    }

    const result = await xeroService.handleCallback({
      ptrsId: effectivePtrsId,
      code,
      state,
    });

    // Service returns a redirectUrl (typically FE tenant selection page)
    return res.redirect(result.redirectUrl);
  } catch (err) {
    logger?.logEvent?.("error", "Error handling Xero callback (PTRS v2)", {
      action: "PtrsV2XeroCallback",
      ptrsId: effectivePtrsId,
      error: err?.message,
      statusCode: err?.statusCode || 500,
    });

    // Always route the user back to FE callback panel with the error in the query string.
    try {
      const qs = new URLSearchParams();
      if (effectivePtrsId) qs.set("ptrsId", effectivePtrsId);
      qs.set("error", "callback_failed");
      qs.set("error_description", err?.message || "Xero callback failed");
      return res.redirect(`${feCallbackBase}?${qs.toString()}`);
    } catch (_) {
      return next(err);
    }
  }
}

/**
 * GET /api/v2/ptrs/:id/xero/organisations
 */
async function getOrganisations(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const result = await xeroService.getOrganisations({ customerId, ptrsId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroOrganisationsGet",
      entity: "PtrsXeroConnection",
      entityId: ptrsId,
      details: {
        ptrsId,
        count: Array.isArray(result?.organisations)
          ? result.organisations.length
          : 0,
      },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.("error", "Error fetching Xero organisations (PTRS v2)", {
      action: "PtrsV2XeroOrganisationsGet",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * POST /api/v2/ptrs/:id/xero/organisations
 * Body: { tenantIds: string[] }
 */
async function selectOrganisations(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  const tenantIds = req.body?.tenantIds || [];

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const result = await xeroService.selectOrganisations({
      customerId,
      ptrsId,
      tenantIds: Array.isArray(tenantIds) ? tenantIds : [],
      userId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroOrganisationsSelect",
      entity: "PtrsXeroConnection",
      entityId: ptrsId,
      details: { ptrsId, tenantIds: result?.selectedTenantIds || [] },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.(
      "error",
      "Error selecting Xero organisations (PTRS v2)",
      {
        action: "PtrsV2XeroOrganisationsSelect",
        ptrsId,
        customerId,
        userId,
        error: error?.message,
        statusCode: error?.statusCode || 500,
      },
    );
    return next(error);
  }
}

/**
 * DELETE /api/v2/ptrs/:id/xero/organisations/:tenantId
 */
async function removeOrganisation(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const tenantId = req.params.tenantId;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }
    if (!tenantId) {
      return res
        .status(400)
        .json({ status: "error", message: "tenantId is required" });
    }

    const result = await xeroService.removeOrganisation({
      customerId,
      ptrsId,
      tenantId,
      userId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroOrganisationRemove",
      entity: "PtrsXeroConnection",
      entityId: ptrsId,
      details: { ptrsId, tenantId },
    });

    return res.status(200).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.("error", "Error removing Xero organisation (PTRS v2)", {
      action: "PtrsV2XeroOrganisationRemove",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * POST /api/v2/ptrs/:id/xero/import
 * Body: { forceRefresh?: boolean }
 */
async function startImport(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;
  const { forceRefresh } = req.body || {};

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const result = await xeroService.startImport({
      customerId,
      ptrsId,
      forceRefresh: Boolean(forceRefresh),
      userId: userId || null,
    });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroImportStart",
      entity: "PtrsXeroImport",
      entityId: ptrsId,
      details: {
        ptrsId,
        forceRefresh: Boolean(forceRefresh),
        status: result?.status || null,
      },
    });

    return res.status(201).json({ status: "success", data: result });
  } catch (error) {
    logger?.logEvent?.("error", "Error starting PTRS v2 Xero import", {
      action: "PtrsV2XeroImportStart",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/:id/xero/status
 */
async function getStatus(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const status = await xeroService.getStatus({ customerId, ptrsId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroImportStatus",
      entity: "PtrsXeroImport",
      entityId: ptrsId,
      details: {
        ptrsId,
        status: status?.status || null,
      },
    });

    return res.status(200).json({ status: "success", data: status });
  } catch (error) {
    logger?.logEvent?.("error", "Error fetching PTRS v2 Xero import status", {
      action: "PtrsV2XeroImportStatus",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}

/**
 * GET /api/v2/ptrs/:id/xero/readiness
 * Returns whether the stored Xero connection is still valid, and whether selected orgs are still present.
 */
async function getReadiness(req, res, next) {
  const customerId = req.effectiveCustomerId;
  const userId = req.auth?.id;
  const ip = req.ip;
  const device = req.headers["user-agent"];
  const ptrsId = req.params.id;

  try {
    if (!customerId) {
      return res
        .status(400)
        .json({ status: "error", message: "Customer ID missing" });
    }
    if (!ptrsId) {
      return res
        .status(400)
        .json({ status: "error", message: "ptrsId is required" });
    }

    const readiness = await xeroService.getReadiness({ customerId, ptrsId });

    await auditService.logEvent({
      customerId,
      userId,
      ip,
      device,
      action: "PtrsV2XeroReadinessGet",
      entity: "PtrsXeroConnection",
      entityId: ptrsId,
      details: {
        ptrsId,
        connected: Boolean(readiness?.connected),
        selectedTenantCount: Array.isArray(readiness?.selectedTenantIds)
          ? readiness.selectedTenantIds.length
          : 0,
        missingSelectedTenantCount: Array.isArray(
          readiness?.missingSelectedTenantIds,
        )
          ? readiness.missingSelectedTenantIds.length
          : 0,
      },
    });

    return res.status(200).json({ status: "success", data: readiness });
  } catch (error) {
    logger?.logEvent?.("error", "Error checking Xero readiness (PTRS v2)", {
      action: "PtrsV2XeroReadinessGet",
      ptrsId,
      customerId,
      userId,
      error: error?.message,
      statusCode: error?.statusCode || 500,
    });
    return next(error);
  }
}
