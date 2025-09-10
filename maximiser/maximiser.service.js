// maximiser.service.js
// First-cut service for Maximiser Compare. Transparent, explainable metrics.
// NOTE: This ships with a pluggable seam `fetchBaseSignals` to be swapped with
// real SQL/view logic once the table/view contracts are final. Keep formulas simple.

const dayjs = require("dayjs");
const {
  beginTransactionWithCustomerContext,
} = require("../helpers/setCustomerIdRLS");
const db = require("../db/database");
const { logger } = require("../helpers/logger");

/**
 * Placeholder: fetch base signals for a set of teams and window.
 * Replace this with real SQL/view calls once available.
 * Expected shape per team:
 * {
 *   teamId, teamName,
 *   hours: { total, billable, nonBillable, afterHours },
 *   estimation: [ { category, planned, actual } ],
 *   streaksOver10h: number,
 *   contextSwitchRate: number, // avg distinct engagements/categories per person-day
 *   tasksPerPersonPerWeek: number
 * }
 */
async function fetchBaseSignals({
  customerId,
  teamIds = [],
  from,
  to,
  includeNonBillable = true,
}) {
  const windowFrom = dayjs(
    from || dayjs().subtract(28, "day").format("YYYY-MM-DD")
  ).format("YYYY-MM-DD");
  const windowTo = dayjs(to || dayjs().format("YYYY-MM-DD")).format(
    "YYYY-MM-DD"
  );

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    // 1) Base hours by team (windowed) + 2) Context switching via view (averaged per team)
    const [baseRows] = await db.sequelize.query(
      `WITH rows AS (
         SELECT tr.*, r.team AS "teamId"
           FROM public.tbl_pulse_timesheet_row tr
           JOIN public.tbl_pulse_timesheet ts
             ON ts.id = tr."timesheetId" AND ts."customerId" = tr."customerId"
           JOIN public.tbl_pulse_resource r
             ON r.id = ts."resourceId" AND r."customerId" = ts."customerId"
          WHERE tr."customerId" = :customerId
            AND r.team IN (:teamIds)
            AND tr."date" BETWEEN :from AND :to
       ), agg AS (
         SELECT
           "teamId",
           SUM(CASE WHEN billable THEN hours ELSE 0 END) AS billable_hours,
           SUM(CASE WHEN NOT billable THEN hours ELSE 0 END) AS non_billable_hours,
           SUM(hours) AS total_hours
         FROM rows
         GROUP BY "teamId"
       ), csr AS (
         SELECT
           "teamId",
           AVG(context_switch_rate)::numeric(10,2) AS context_switch_rate
         FROM public.v_pulse_context_switching
         WHERE "customerId" = :customerId AND "teamId" IN (:teamIds)
         GROUP BY "teamId"
       )
       SELECT a."teamId",
              COALESCE(a.billable_hours,0)      AS billable,
              COALESCE(a.non_billable_hours,0)  AS non_billable,
              COALESCE(a.total_hours,0)         AS total,
              0                                  AS after_hours,
              COALESCE(c.context_switch_rate,0) AS context_switch_rate
         FROM agg a
         LEFT JOIN csr c USING ("teamId")`,
      {
        replacements: { customerId, teamIds, from: windowFrom, to: windowTo },
        transaction: t,
      }
    );

    // 3) Estimation by category (date-filtered via raw query to match view semantics)
    const [estRows] = await db.sequelize.query(
      `SELECT r.team AS "teamId",
              COALESCE(bi."sectionName", 'Uncategorised') AS category,
              SUM(COALESCE(bi.hours, 0)) AS planned,
              SUM(COALESCE(tr.hours, 0)) AS actual
         FROM public.tbl_pulse_timesheet_row tr
         JOIN public.tbl_pulse_timesheet ts
           ON ts.id = tr."timesheetId" AND ts."customerId" = tr."customerId"
         JOIN public.tbl_pulse_resource r
           ON r.id = ts."resourceId" AND r."customerId" = ts."customerId"
         LEFT JOIN public.tbl_pulse_budget_item bi
           ON bi."customerId" = tr."customerId" AND bi."engagementId" = tr."engagementId" AND bi.id = tr."budgetItemId"
        WHERE tr."customerId" = :customerId
          AND r.team IN (:teamIds)
          AND tr."date" BETWEEN :from AND :to
        GROUP BY r.team, COALESCE(bi."sectionName", 'Uncategorised')`,
      {
        replacements: { customerId, teamIds, from: windowFrom, to: windowTo },
        transaction: t,
      }
    );

    // 4) Long-day streaks (days with >=10h per team within window)
    const [streakRows] = await db.sequelize.query(
      `WITH per_day AS (
         SELECT r.team AS "teamId",
                tr."date" AS d,
                SUM(tr.hours) AS daily_hours
           FROM public.tbl_pulse_timesheet_row tr
           JOIN public.tbl_pulse_timesheet ts
             ON ts.id = tr."timesheetId" AND ts."customerId" = tr."customerId"
           JOIN public.tbl_pulse_resource r
             ON r.id = ts."resourceId" AND r."customerId" = ts."customerId"
          WHERE tr."customerId" = :customerId
            AND r.team IN (:teamIds)
            AND tr."date" BETWEEN :from AND :to
          GROUP BY r.team, tr."date"
       )
       SELECT "teamId",
              COUNT(*) FILTER (WHERE daily_hours >= 10) AS streaks_over_10h
         FROM per_day
        GROUP BY "teamId"`,
      {
        replacements: { customerId, teamIds, from: windowFrom, to: windowTo },
        transaction: t,
      }
    );

    await t.commit();

    // Assemble per-team payloads
    const hoursByTeam = new Map(baseRows.map((r) => [r.teamId, r]));
    const streakByTeam = new Map(
      streakRows.map((r) => [r.teamId, Number(r.streaks_over_10h) || 0])
    );

    const estByTeam = new Map();
    for (const row of estRows) {
      const arr = estByTeam.get(row.teamId) || [];
      arr.push({
        category: row.category,
        planned: Number(row.planned) || 0,
        actual: Number(row.actual) || 0,
      });
      estByTeam.set(row.teamId, arr);
    }

    return teamIds.map((id) => {
      const h = hoursByTeam.get(id) || {
        billable: 0,
        non_billable: 0,
        total: 0,
        after_hours: 0,
        context_switch_rate: 0,
      };
      const total =
        Number(h.billable) + (includeNonBillable ? Number(h.non_billable) : 0);
      const nonBillable = includeNonBillable ? Number(h.non_billable) : 0;
      return {
        teamId: id,
        teamName: id, // view provides team name == id for now
        hours: {
          total,
          billable: Number(h.billable) || 0,
          nonBillable,
          afterHours: Number(h.after_hours) || 0,
        },
        estimation: estByTeam.get(id) || [],
        streaksOver10h: streakByTeam.get(id) || 0,
        contextSwitchRate: Number(h.context_switch_rate) || 0,
        tasksPerPersonPerWeek: 0,
        window: { from: windowFrom, to: windowTo },
      };
    });
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

async function fetchBenchmarkFlags({ customerId, teamIds = [] }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    const [rows] = await db.sequelize.query(
      `SELECT s."teamId", s."resourceId", r.name AS resource_name, s.category, s.n_rows, s.avg_hours,
              m.team_median_avg_hours, m.contributors
         FROM public.v_pulse_task_category_stats s
         JOIN public.v_pulse_task_category_medians m
           ON m."customerId" = s."customerId"
          AND m."teamId"     = s."teamId"
          AND m.category      = s.category
         JOIN public.tbl_pulse_resource r
           ON r.id = s."resourceId" AND r."customerId" = s."customerId"
        WHERE s."customerId" = :customerId
          AND s."teamId" IN (:teamIds)`,
      { replacements: { customerId, teamIds }, transaction: t }
    );

    await t.commit();

    // Build positive/constructive flags by team
    const flagsByTeam = new Map();
    const MIN_ROWS = 2; // per resource & category
    const HIGH = 1.3; // >=130% of team median ⇒ support opportunity
    const LOW = 0.75; // <=75%  of team median ⇒ mentor candidate

    for (const r of rows) {
      if (
        !r ||
        r.contributors < 2 ||
        r.n_rows < MIN_ROWS ||
        !r.team_median_avg_hours
      )
        continue;
      const ratio = Number(r.avg_hours) / Number(r.team_median_avg_hours);
      const arr = flagsByTeam.get(r.teamId) || [];
      if (ratio >= HIGH) {
        arr.push({
          severity: "warning",
          key: `support-${r.resourceId}-${r.category}`,
          message: `Support opportunity in ${r.category}: ${r.resource_name || r.resourceId} averages ${ratio.toFixed(1)}× team median (${Number(r.avg_hours).toFixed(1)}h vs ${Number(r.team_median_avg_hours).toFixed(1)}h).`,
        });
      }
      if (ratio <= LOW) {
        arr.push({
          severity: "success",
          key: `mentor-${r.resourceId}-${r.category}`,
          message: `Mentor candidate in ${r.category}: ${r.resource_name || r.resourceId} averages ${ratio.toFixed(1)}× team median (${Number(r.avg_hours).toFixed(1)}h vs ${Number(r.team_median_avg_hours).toFixed(1)}h).`,
        });
      }
      if (arr.length) flagsByTeam.set(r.teamId, arr);
    }

    return flagsByTeam; // Map(teamId -> flags[])
  } catch (error) {
    await t.rollback();
    throw error;
  }
}

function titleCase(s) {
  if (!s) return s;
  return s
    .toLowerCase()
    .split(/\s+/)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function extractCategoryFromKey(key) {
  // support-<resourceId>-<category> or mentor-<resourceId>-<category>
  if (!key) return null;
  const parts = String(key).split("-");
  if (parts.length < 3) return null;
  return parts.slice(2).join("-");
}

function extractNameFromMessage(msg) {
  // e.g., "Support opportunity in Delivery: Natasha averages ..." or
  // "Mentor candidate in Control testing: Peter averages ..."
  const m = /:\s*([^:]+?)\s+averages/i.exec(msg || "");
  return m ? m[1].trim() : null;
}

function synthesizeStory({ teamName, scores, flags, estimation, hours }) {
  const story = {
    summary: "",
    wins: [],
    opportunities: [],
    watchouts: [],
    next: "",
    confidence: "medium",
  };

  // Confidence heuristic: more data => higher confidence
  const planCats = Array.isArray(estimation) ? estimation.length : 0;
  const totalH = Number(hours?.total || 0);
  if (planCats >= 4 && totalH >= 80) story.confidence = "high";
  else if (planCats >= 2 && totalH >= 20) story.confidence = "medium";
  else story.confidence = "low";

  const succ = []; // celebrate
  const warn = []; // heads-up / support
  const risk = []; // at risk

  const mentors = new Map(); // category -> name
  const supports = new Map(); // category -> name

  for (const f of flags || []) {
    const key = f.key || "";
    const cat = extractCategoryFromKey(key);
    if (key.startsWith("mentor-")) {
      const who = extractNameFromMessage(f.message);
      if (cat) mentors.set(cat, who || "Mentor");
      succ.push(
        `Mentor Candidate${cat ? ` in ${cat}` : ""}: ${f.message.replace(/^[^:]+:\s*/, "")}`
      );
    } else if (key.startsWith("support-")) {
      const who = extractNameFromMessage(f.message);
      if (cat) supports.set(cat, who || "Teammate");
      warn.push(
        `Support Opportunity${cat ? ` in ${cat}` : ""}: ${f.message.replace(/^[^:]+:\s*/, "")}`
      );
    } else if (f.severity === "success") {
      succ.push(f.message);
    } else if (f.severity === "warning") {
      warn.push(f.message);
    } else if (f.severity === "error") {
      risk.push(f.message);
    }
  }

  // Pairings (mentor -> support) by category
  const pairings = [];
  for (const [cat, mentorName] of mentors.entries()) {
    if (supports.has(cat)) {
      pairings.push({
        category: cat,
        mentor: mentorName,
        support: supports.get(cat),
      });
    }
  }

  // Summary sentence
  const parts = [];
  if (scores?.pace >= 90) parts.push("Pace steady");
  if (scores?.valueMix >= 90) parts.push("healthy value mix");
  if (scores?.predictability < 70) parts.push("planning variance to address");
  if (pairings.length)
    parts.push(
      `${pairings[0].mentor} can mentor ${pairings[0].support} in ${pairings[0].category}`
    );
  story.summary = parts.length
    ? `${teamName}: ${parts.join(", ")}.`
    : `${teamName}: steady overall.`;

  // Wins / Opportunities / Watch-outs (cap to keep it readable)
  story.wins = succ.slice(0, 2);
  story.opportunities = warn.slice(0, 3);
  story.watchouts = risk.slice(0, 3);

  // Next steps
  if (pairings.length) {
    story.next = `Pair ${pairings[0].mentor} → ${pairings[0].support} for ${pairings[0].category} (1h), then re-estimate hotspots.`;
  } else if (risk.length) {
    story.next = `Address top risk: ${risk[0]}`;
  } else if (warn.length) {
    story.next = `Tackle opportunity: ${warn[0]}`;
  } else if (succ.length) {
    story.next = `Share practice: ${succ[0]}`;
  } else {
    story.next = "No immediate actions.";
  }

  return story;
}

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function computeMAPEByCategory(rows) {
  // MAPE per category; ignore zero planned
  return rows.map((r) => {
    const denom = r.planned === 0 ? 1 : r.planned;
    const mape = (Math.abs(r.actual - r.planned) / denom) * 100;
    return { category: r.category, mape };
  });
}

function weightedMAPE(rows) {
  const filtered = rows.filter((r) => r.planned > 0);
  if (filtered.length === 0) return 0;
  const totalPlanned = filtered.reduce((s, r) => s + r.planned, 0);
  const sum = filtered.reduce(
    (s, r) =>
      s +
      (Math.abs(r.actual - r.planned) / r.planned) *
        100 *
        (r.planned / totalPlanned),
    0
  );
  return sum;
}

function scoreTeam(metrics) {
  const { estimation, hours, streaksOver10h, contextSwitchRate } = metrics;
  const mapeW = weightedMAPE(estimation);
  const afterHoursPct =
    hours.total > 0 ? (hours.afterHours / hours.total) * 100 : 0;
  const nonBillablePct =
    hours.total > 0 ? (hours.nonBillable / hours.total) * 100 : 0;
  const α = 1.2,
    β = 3,
    γ = 10,
    δ = 1;
  const predictability = clamp(100 - mapeW, 0, 100);
  const pace = clamp(100 - (afterHoursPct * α + streaksOver10h * β), 0, 100);
  const focus = clamp(100 - contextSwitchRate * γ, 0, 100);
  const valueMix = clamp(100 - nonBillablePct * δ, 0, 100);
  return {
    predictability: Math.round(predictability),
    pace: Math.round(pace),
    focus: Math.round(focus),
    valueMix: Math.round(valueMix),
  };
}

function deriveFlags(metrics) {
  const flags = [];
  const mapeByCat = computeMAPEByCategory(metrics.estimation);
  const hours = metrics.hours || { total: 0, afterHours: 0, nonBillable: 0 };
  const nonBillablePct =
    hours.total > 0 ? (hours.nonBillable / hours.total) * 100 : 0;
  const afterHoursPct =
    hours.total > 0 ? (hours.afterHours / hours.total) * 100 : 0;

  const disc = mapeByCat.find((x) => x.category === "Discovery");
  if (disc && disc.mape > 30) {
    flags.push({
      severity: "error",
      key: "estimation_discovery_overrun",
      message: `Discovery work overshoots plan by ${disc.mape.toFixed(0)}%.`,
    });
  }
  if (afterHoursPct > 12) {
    flags.push({
      severity: "error",
      key: "after_hours_load",
      message: `After-hours time at ${afterHoursPct.toFixed(1)}% — workload leveling recommended.`,
    });
  }
  if (metrics.streaksOver10h >= 3) {
    flags.push({
      severity: "error",
      key: "streaks_over_10h",
      message: `${metrics.streaksOver10h} recent ≥10h day streaks — risk of burnout.`,
    });
  }
  if (nonBillablePct > 25) {
    flags.push({
      severity: "warning",
      key: "non_billable_drift",
      message: `Non-billable work is ${nonBillablePct.toFixed(0)}% — review admin/process overhead.`,
    });
  }
  if (metrics.contextSwitchRate > 3) {
    flags.push({
      severity: "warning",
      key: "context_switching_high",
      message: `High context switching rate (${metrics.contextSwitchRate.toFixed(1)}).`,
    });
  }
  return flags;
}

async function listTeams({ customerId }) {
  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: maximiser listTeams", {
      action: "PulseMaximiserListTeams",
      customerId,
    });

    const [rows] = await db.sequelize.query(
      `SELECT DISTINCT "teamId", "teamName"
         FROM public.v_pulse_team_map
        WHERE "customerId" = :customerId
        ORDER BY "teamName" ASC`,
      { replacements: { customerId }, transaction: t }
    );

    await t.commit();
    return rows.map((r) => ({ teamId: r.teamId, teamName: r.teamName }));
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: maximiser listTeams failed", {
      action: "PulseMaximiserListTeams",
      customerId,
      error: error.message,
    });
    throw error;
  }
}

async function compareTeams({
  customerId,
  teamIds = [],
  from,
  to,
  includeNonBillable = true,
}) {
  // Ownership check (defence in depth): all requested teamIds must belong to tenant
  const allowed = await listTeams({ customerId });
  const allowedIds = new Set((allowed || []).map((t) => t.teamId));
  const invalid = teamIds.filter((id) => !allowedIds.has(id));
  if (invalid.length) {
    const err = new Error(
      `One or more teamIds are not accessible: ${invalid.join(",")}`
    );
    err.status = 403;
    throw err;
  }

  const t = await beginTransactionWithCustomerContext(customerId);
  try {
    logger.logEvent("info", "Pulse: maximiser compare compute", {
      action: "PulseMaximiserCompareCompute",
      customerId,
      teamIds,
      from,
      to,
      includeNonBillable,
    });

    // Placeholder — later replace with RLS-backed SELECTs within this transaction
    const base = await fetchBaseSignals({
      customerId,
      teamIds,
      from,
      to,
      includeNonBillable,
    });

    // Positive, people-first opportunities (mentor/training) based on medians
    const benchFlagsMap = await fetchBenchmarkFlags({ customerId, teamIds });

    await t.commit();

    // ===== Assemble FE‑friendly team objects (flat shape consumed by FE) =====
    const teams = base.map((b) => {
      const scores = scoreTeam({
        estimation: b.estimation,
        hours: b.hours,
        streaksOver10h: b.streaksOver10h,
        contextSwitchRate: b.contextSwitchRate,
      });

      // Compose flags: baseline (burnout/value mix/context switching/after hours) + benchmark (mentor/support)
      const baseFlags = deriveFlags({
        estimation: b.estimation,
        hours: b.hours,
        streaksOver10h: b.streaksOver10h,
        contextSwitchRate: b.contextSwitchRate,
      });
      const benchFlags = benchFlagsMap.get(b.teamId) || [];

      return {
        teamId: b.teamId,
        teamName: b.teamName,
        scores,
        hours: b.hours, // { billable, nonBillable, total, afterHours }
        contextSwitchRate: b.contextSwitchRate,
        streaksOver10h: b.streaksOver10h,
        story: synthesizeStory({
          teamName: b.teamName,
          scores,
          flags: [...baseFlags, ...benchFlags],
          estimation: b.estimation,
          hours: b.hours,
        }),
        flags: [...baseFlags, ...benchFlags],
      };
    });

    // Leaderboard = sum of scores (simple composite)
    const leaderboard = [...teams]
      .sort((a, b) => {
        const as =
          a.scores.predictability +
          a.scores.pace +
          a.scores.focus +
          a.scores.valueMix;
        const bs =
          b.scores.predictability +
          b.scores.pace +
          b.scores.focus +
          b.scores.valueMix;
        return bs - as;
      })
      .map((t) => t.teamId);

    const result = {
      window: {
        from: base[0]?.window?.from || from,
        to: base[0]?.window?.to || to,
      },
      teams,
      leaderboard,
    };

    logger.logEvent("info", "Pulse: maximiser compare complete", {
      action: "PulseMaximiserCompareCompute",
      customerId,
      teams: result.teams.length,
    });

    return result;
  } catch (error) {
    await t.rollback();
    logger.logEvent("error", "Pulse: maximiser compare failed", {
      action: "PulseMaximiserCompareCompute",
      customerId,
      error: error.message,
    });
    throw error;
  }
}

module.exports = {
  listTeams,
  compareTeams,
  // exported for unit tests
  _internals: {
    fetchBaseSignals,
    fetchBenchmarkFlags,
    scoreTeam,
    deriveFlags,
    computeMAPEByCategory,
    weightedMAPE,
  },
};
