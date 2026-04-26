const fs = require("fs");
const path = require("path");
const PptxGenJS = require("pptxgenjs");
const {
  calcTextBoxHeightSimple,
} = require("../output/sales_director_monthly_deck_2026-03-31/pptxgenjs_helpers/text");
const {
  imageSizingContain,
} = require("../output/sales_director_monthly_deck_2026-03-31/pptxgenjs_helpers/image");
const {
  warnIfSlideHasOverlaps,
  warnIfSlideElementsOutOfBounds,
} = require("../output/sales_director_monthly_deck_2026-03-31/pptxgenjs_helpers/layout");

const REPO_ROOT = path.resolve(__dirname, "..");
const DEFAULT_CONTRACT = path.join(
  REPO_ROOT,
  "config",
  "sales_director_monthly_shell.json",
);
const DEFAULT_OUTPUT = path.join(
  REPO_ROOT,
  "output",
  "sales_director_monthly_shells",
  "2026-04-10",
  "Sales Director Monthly Shell - Sarah Pittroff (Central Europe).pptx",
);
const WORDMARK = path.join(
  REPO_ROOT,
  "output",
  "sales_director_monthly_deck_2026-03-31",
  "assets",
  "simcorp_wordmark.png",
);
const ENABLE_LAYOUT_VALIDATION = process.env.SD_SHELL_VALIDATE_LAYOUT !== "0";

const COLORS = {
  paper: "F5F6F8",
  white: "FFFFFF",
  ink: "1A1D31",
  charcoal: "1A1D31",
  muted: "6B7280",
  border: "D1D5DB",
  teal: "083EA7",
  tealDeep: "062D7A",
  tealSoft: "E8EEF8",
  rust: "EF3E4A",
  rustSoft: "FDE8E9",
  gold: "FB9B2A",
  goldSoft: "FEF3E2",
  plum: "4B17B6",
  plumSoft: "EDE5F7",
  green: "2F7D57",
  greenSoft: "E4F2E9",
  blueSoft: "E8EEF8",
};

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) {
      args[key] = "true";
      continue;
    }
    args[key] = next;
    i += 1;
  }
  return args;
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function toBulletLines(items = [], fallback = "") {
  const values = Array.isArray(items) ? items.filter(Boolean) : [];
  if (!values.length && fallback) return [fallback];
  return values;
}

function addWordmark(slide, x = 10.75, y = 0.3, w = 1.2, h = 0.38) {
  if (!fs.existsSync(WORDMARK)) return;
  slide.addImage({
    path: WORDMARK,
    ...imageSizingContain(WORDMARK, x, y, w, h),
  });
}

function addFooter(slide, pageNum, snapshotDate) {
  slide.addText(String(pageNum), {
    x: 12.42,
    y: 7.02,
    w: 0.35,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 8.5,
    color: COLORS.muted,
    align: "right",
  });
  slide.addText(`Director operating review | ${snapshotDate}`, {
    x: 0.55,
    y: 7.02,
    w: 3.4,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 8.5,
    color: COLORS.muted,
  });
}

function validateSlide(slide, pptx) {
  if (!ENABLE_LAYOUT_VALIDATION) return;
  warnIfSlideHasOverlaps(slide, pptx);
  warnIfSlideElementsOutOfBounds(slide, pptx);
}

function addTopRule(slide, color, y = 0.74) {
  slide.addShape("rect", {
    x: 0.58,
    y,
    w: 1.35,
    h: 0.045,
    line: { color, transparency: 100 },
    fill: { color, transparency: 0 },
  });
}

function addSlideHeader(
  slide,
  title,
  subtitle,
  managementQuestion,
  accent = COLORS.teal,
  options = {},
) {
  const showQuestionBox = options.showQuestionBox !== false;
  addTopRule(slide, accent);
  const titleFontSize = title.length > 68 ? 18.5 : title.length > 52 ? 20 : 22;
  const titleLines = title.length > 68 ? 3 : title.length > 52 ? 2 : 1;
  const titleHeight = titleLines === 1 ? 0.34 : titleLines === 2 ? 0.66 : 0.92;
  slide.addText(title, {
    x: 0.58,
    y: 0.84,
    w: 8.6,
    h: titleHeight,
    fontFace: "Avenir Next",
    fontSize: titleFontSize,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  const subtitleY = 0.84 + titleHeight + 0.05;
  slide.addText(subtitle || "", {
    x: 0.58,
    y: subtitleY,
    w: 8.8,
    h: 0.24,
    fontFace: "Avenir Next",
    fontSize: 10,
    color: COLORS.muted,
    margin: 0,
  });
  let contentY = subtitleY + 0.34;
  if (showQuestionBox && managementQuestion) {
    const boxY = subtitleY + 0.26;
    const boxH = 0.34;
    slide.addShape("rect", {
      x: 0.58,
      y: boxY,
      w: 11.65,
      h: boxH,
      line: { color: COLORS.border, pt: 0.5 },
      fill: { color: COLORS.paper },
    });
    slide.addShape("rect", {
      x: 0.58,
      y: boxY,
      w: 0.08,
      h: boxH,
      line: { color: accent, transparency: 100 },
      fill: { color: accent },
    });
    slide.addText(managementQuestion, {
      x: 0.78,
      y: boxY + 0.12,
      w: 11.08,
      h: Math.max(0.18, boxH - 0.14),
      fontFace: "Avenir Next",
      fontSize: 9.7,
      color: COLORS.ink,
      margin: 0,
    });
    contentY = boxY + boxH + 0.18;
  }
  addWordmark(slide);
  return { contentY };
}

function addPanel(slide, x, y, w, h, fill, line = COLORS.border) {
  slide.addShape("rect", {
    x,
    y,
    w,
    h,
    line: { color: line, pt: 1 },
    fill: { color: fill },
  });
}

function formatTableValue(value, fallback = "—") {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "string") return value.trim() || fallback;
  return String(value);
}

function payloadForSlide(fillPayload, slideId) {
  if (!fillPayload || !Array.isArray(fillPayload.slides)) return null;
  return fillPayload.slides.find((slide) => slide.id === slideId) || null;
}

function quarterlyPipelineLabel(slots) {
  return formatTableValue(slots.quarterly_pipeline_label, "Q2");
}

function quarterlyPipelineTitle(slots) {
  return formatTableValue(
    slots.quarterly_pipeline_title,
    quarterlyPipelineLabel(slots),
  );
}

function watchlistTableRows(rows, columns) {
  const dataRows = Array.isArray(rows) ? rows : [];
  const mapped = dataRows.slice(0, 5).map((row) =>
    columns.map((header) => {
      switch (header) {
        case "Opportunity":
          return formatTableValue(row.opportunity);
        case "ARR":
        case "ACV":
          return formatTableValue(
            row.arr_eur !== "—" ? row.arr_eur : row.renewal_acv_eur,
          );
        case "Owner":
          return formatTableValue(row.owner);
        case "Stage":
          return formatTableValue(row.stage);
        case "Action":
        case "Follow-up":
        case "Next step":
          return formatTableValue(row.next_action || row.reason);
        case "Close date":
          return formatTableValue(row.close_date);
        case "Reason gap":
          return formatTableValue(row.reason || "Missing");
        default:
          return "—";
      }
    }),
  );
  while (mapped.length < 5) {
    mapped.push(
      columns.map((header) =>
        header === "ARR" || header === "ACV"
          ? "—"
          : header === "Close date"
            ? "—"
            : "—",
      ),
    );
  }
  return [columns, ...mapped];
}

function ownerSummaryRows(rows) {
  const ranked = Array.isArray(rows) ? rows.slice(0, 5) : [];
  if (!ranked.length) return [];
  return ranked.map((row) => ({
    owner: formatTableValue(row.owner),
    record_count: formatTableValue(row.record_count),
  }));
}

function inferCardPlaceholder(card) {
  const metric = (card.metric_hint || "").toLowerCase();
  const label = (card.label || "").toLowerCase();
  if (metric.includes("eur arr") || metric.includes("eur acv")) return "€x.xM";
  if (metric.includes("%") || label.includes("rate")) return "xx%";
  if (metric.includes("count + eur arr")) return "xx | €x.xM";
  if (metric.includes("deals + eur arr") || metric.includes("deals + eur acv"))
    return "xx | €x.xM";
  if (
    metric.includes("stage 3+ count") ||
    metric.includes("count") ||
    label.includes("book")
  )
    return "xx";
  if (metric.includes("issue-points")) return "xx";
  if (metric.includes("qualified statement")) return "Qualified read";
  if (metric.includes("summary statement")) return "Headline read";
  if (metric.includes("largest deals")) return "Top 3-5";
  if (metric.includes("risk pressure")) return "Pressure read";
  if (metric.includes("risk mix")) return "Risk mix";
  return "Current read";
}

function inferCardValue(slideDef, slidePayload, index) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "executive-summary":
      return [
        slots.headline_pipeline_arr_all_open,
        slots.headline_pipeline_arr_fy26,
        slots.headline_pipeline_arr_q2,
        slots.headline_renewal_acv,
      ][index];
    case "q1-review":
      return [
        `${formatTableValue(slots.q1_won_count)} | ${formatTableValue(slots.q1_won_arr)}`,
        `${formatTableValue(slots.q1_lost_count)} | ${formatTableValue(slots.q1_lost_arr)}`,
        `${formatTableValue(slots.q1_slipped_count)} | ${formatTableValue(slots.q1_slipped_arr)}`,
        "Qualified read",
      ][index];
    case "quarterly-pipeline":
      return [
        slots.headline_pipeline_arr_q2,
        slots.q2_commit_arr,
        slots.q2_best_case_arr,
        slots.q2_omitted_arr,
      ][index];
    case "commercial-approval-overview":
      return [
        slots.approval_rate_stage3_plus,
        `${formatTableValue(slots.approved_deal_count)} | ${formatTableValue(slots.approved_arr)}`,
        `${formatTableValue(slots.pending_missing_approval_count)} | ${formatTableValue(slots.pending_missing_approval_arr)}`,
        slots.missing_approval_candidate_count,
      ][index];
    case "renewals-retention":
      return [
        `${formatTableValue(slots.renewal_open_deal_count)} | ${formatTableValue(slots.renewal_open_acv)}`,
        "Risk mix",
        `${Array.isArray(slots.renewal_watchlist) ? slots.renewal_watchlist.length : 0} named`,
      ][index];
    case "salesforce-hygiene-activity":
      return [
        slots.no_activity_count,
        slots.overdue_close_count,
        slots.missing_next_step_count,
        slots.total_data_quality_issues,
      ][index];
    default:
      return null;
  }
}

function inferCardLabel(slideDef, slidePayload, index, card) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "quarterly-pipeline":
      return (
        [
          `${quarterlyPipelineLabel(slots)} Active Pipeline`,
          "Commit",
          "Best Case",
          "Omitted",
        ][index] || card.label
      );
    default:
      return card.label;
  }
}

function inferCardBody(slideDef, slidePayload, index, card) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "executive-summary":
      return (
        [
          "All close dates in the open book.",
          "FY26 active pipeline, excluding Omitted.",
          "Current-quarter active ARR for inspection.",
          "Open renewal ACV across the director book.",
        ][index] || card.body
      );
    case "q1-review":
      return (
        [
          "Closed won in Q1.",
          "Closed lost in Q1.",
          "Deals slipped out of Q1.",
          "Promise baseline is directional — formal targets not yet integrated.",
        ][index] || card.body
      );
    case "quarterly-pipeline":
      {
        const quarterTitle = quarterlyPipelineTitle(slots);
      return (
        [
          `${quarterTitle} active pipeline, excluding Omitted.`,
          `${quarterTitle} commit forecast category.`,
          `${quarterTitle} best-case forecast category.`,
          "Visible but kept outside active headline pipeline.",
        ][index] || card.body
      );
      }
    case "commercial-approval-overview":
      return (
        [
          "Stage 3+ land deals with commercial approval.",
          "Deals approved in 2026.",
          "Pending or missing approval exposure.",
          "Land stage 3+ candidates needing action.",
        ][index] || card.body
      );
    case "renewals-retention":
      return (
        [
          "Open renewal ACV across the director book.",
          formatTableValue(slots.renewal_risk_bucket_summary, card.body),
          "Named renewals that need leadership attention.",
        ][index] || card.body
      );
    case "salesforce-hygiene-activity":
      return (
        [
          "Open opportunities without recent activity.",
          "Open opportunities already past close date.",
          "Open opportunities missing next-step discipline.",
          "Aggregated hygiene burden across the book.",
        ][index] || card.body
      );
    default:
      return card.body;
  }
}

function inferSlideTitle(slideDef, slidePayload) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "executive-summary":
      return slots.headline_pipeline_arr_q2 && slots.headline_renewal_acv
        ? `Q2 active ARR is ${slots.headline_pipeline_arr_q2} with ${slots.headline_renewal_acv} in open renewal ACV`
        : slideDef.title;
    case "q1-review":
      return slots.q1_slipped_arr && slots.q1_won_arr
        ? `Q1 delivered ${slots.q1_won_arr} won ARR while ${slots.q1_slipped_arr} slipped out`
        : slideDef.title;
    case "quarterly-pipeline":
      return slots.q2_commit_arr && slots.q2_best_case_arr
        ? `${quarterlyPipelineTitle(slots)} is light in commit at ${slots.q2_commit_arr} against ${slots.q2_best_case_arr} in best case`
        : slideDef.title;
    case "pipeline-coverage-intel":
      return slots.weighted_pipeline_arr && slots.stale_arr
        ? `Coverage stays qualified with ${slots.weighted_pipeline_arr} weighted ARR and ${slots.stale_arr} stale ARR`
        : slideDef.title;
    case "commercial-approval-overview":
      return slots.missing_approval_candidate_count
        ? `${slots.missing_approval_candidate_count} stage 3+ candidates still need approval action`
        : slideDef.title;
    case "missing-commercial-approvals": {
      const first = Array.isArray(slots.missing_approval_candidates)
        ? slots.missing_approval_candidates[0]
        : null;
      return first && first.opportunity
        ? `Approval backlog is concentrated in ${first.opportunity}`
        : slideDef.title;
    }
    case "renewals-retention":
      return slots.renewal_open_acv && slots.renewal_open_deal_count
        ? `Renewal exposure is ${slots.renewal_open_acv} ACV across ${slots.renewal_open_deal_count} open deals`
        : slideDef.title;
    case "slipped-deals":
      return slots.q1_slipped_arr && slots.q1_slipped_count
        ? `${slots.q1_slipped_arr} slipped out of Q1 across ${slots.q1_slipped_count} deals`
        : slideDef.title;
    case "salesforce-hygiene-activity":
      return slots.total_data_quality_issues && slots.no_activity_count
        ? `Hygiene backlog is ${slots.total_data_quality_issues} issue-points with ${slots.no_activity_count} no-activity opps`
        : slideDef.title;
    case "missing-win-loss-reason":
      return slots.missing_win_loss_reason_count === "0"
        ? "Win/loss reason hygiene is clean outside accepted 0 - No Opportunity cases"
        : `${slots.missing_win_loss_reason_count} outcome rows still lack valid win/loss reasons`;
    case "overdue-close-open-opps": {
      const owners = Array.isArray(slots.overdue_close_owner_summary)
        ? slots.overdue_close_owner_summary
        : [];
      const topOwners = owners
        .slice(0, 2)
        .map((row) => row.owner)
        .filter(Boolean);
      return slots.overdue_close_count
        ? `${slots.overdue_close_count} open opps are past close date${topOwners.length ? `, led by ${topOwners.join(" and ")}` : ""}`
        : slideDef.title;
    }
    default:
      return slideDef.title;
  }
}

function inferSlideSubtitle(slideDef, slidePayload) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "executive-summary":
      return slots.top_risk || slideDef.subtitle;
    case "q1-review":
      return "Use director-scoped won, lost, and slipped figures. Keep promise language explicitly qualified.";
    case "quarterly-pipeline":
      if (slots.quarterly_pipeline_display_reason === "forward_quarter_fallback") {
        return `Showing ${quarterlyPipelineTitle(slots)} because the current quarter is empty.`;
      }
      if (slots.quarterly_pipeline_display_reason === "empty_current_and_forward") {
        return `No current or forward-quarter pipeline is available; keep the empty-state note explicit.`;
      }
      return `${quarterlyPipelineTitle(slots)} active view, forecast mix, and omitted separation.`;
    case "pipeline-coverage-intel":
      return `Opportunity view for ${quarterlyPipelineTitle(slots)} only. Keep the target-gap caveat visible until quota data is integrated.`;
    case "commercial-approval-overview":
      return "Use approved 2026 and current candidate controls; keep the missing-approval backlog explicit.";
    case "renewals-retention":
      return formatTableValue(
        slots.renewal_risk_bucket_summary,
        slideDef.subtitle,
      );
    case "slipped-deals":
      return formatTableValue(slots.slip_root_cause_summary, slideDef.subtitle);
    case "salesforce-hygiene-activity":
      return formatTableValue(
        slots.rep_concentration_summary,
        slideDef.subtitle,
      );
    case "missing-win-loss-reason":
      return formatTableValue(
        slots.missing_win_loss_reason_rule_note,
        slideDef.subtitle,
      );
    case "overdue-close-open-opps":
      return "Owner summary must stay sorted by largest record count, not alphabetically.";
    default:
      return slideDef.subtitle;
  }
}

function addCard(
  slide,
  x,
  y,
  w,
  h,
  card,
  accent,
  valueOverride = null,
  bodyOverride = null,
  labelOverride = null,
) {
  addPanel(slide, x, y, w, h, COLORS.white);
  slide.addShape("rect", {
    x,
    y,
    w,
    h: 0.05,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  const valueText = valueOverride || inferCardPlaceholder(card);
  const hasRealValue = Boolean(valueOverride && valueOverride !== "—");
  const showHint = !hasRealValue;
  if (showHint) {
    slide.addText(card.metric_hint || "Key metric", {
      x: x + 0.18,
      y: y + 0.18,
      w: w - 0.35,
      h: 0.18,
      fontFace: "Avenir Next",
      fontSize: 9,
      bold: true,
      color: COLORS.muted,
      margin: 0,
    });
  }
  slide.addText(labelOverride || card.label || "", {
    x: x + 0.18,
    y: y + (showHint ? 0.48 : 0.28),
    w: w - 0.35,
    h: 0.36,
    fontFace: "Avenir Next",
    fontSize: 16,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  const isLongValue = valueText.length > 22;
  const valueFontSize = hasRealValue ? (isLongValue ? 11.2 : 17.5) : 19;
  const valueHeight = hasRealValue ? (isLongValue ? 0.62 : 0.34) : 0.42;
  const valueY = hasRealValue ? (showHint ? 0.86 : 0.72) : 0.86;
  slide.addText(valueText, {
    x: x + 0.18,
    y: y + valueY,
    w: w - 0.35,
    h: valueHeight,
    fontFace: "Avenir Next",
    fontSize: valueFontSize,
    bold: true,
    color: hasRealValue ? COLORS.ink : "B7C5CD",
    margin: 0,
  });
  slide.addText(bodyOverride || card.body || "", {
    x: x + 0.18,
    y:
      y +
      (hasRealValue
        ? isLongValue
          ? showHint
            ? 1.58
            : 1.42
          : showHint
            ? 1.34
            : 1.2
        : 1.42),
    w: w - 0.35,
    h:
      h -
      (hasRealValue
        ? isLongValue
          ? showHint
            ? 1.74
            : 1.58
          : showHint
            ? 1.5
            : 1.34
        : 1.59),
    fontFace: "Avenir Next",
    fontSize: 10.5,
    color: COLORS.charcoal,
    margin: 0,
    breakLine: false,
    valign: "top",
  });
}

function addSmallRuleBox(slide, x, y, w, title, lines, fill) {
  addPanel(slide, x, y, w, 0.68 + lines.length * 0.16, fill);
  slide.addText(title, {
    x: x + 0.14,
    y: y + 0.1,
    w: w - 0.28,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 8.7,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  slide.addText(lines.join("\n"), {
    x: x + 0.14,
    y: y + 0.28,
    w: w - 0.28,
    h: 0.34 + lines.length * 0.125,
    fontFace: "Avenir Next",
    fontSize: 8.95,
    color: COLORS.ink,
    margin: 0,
    breakLine: false,
  });
}

function addSummaryChip(
  slide,
  x,
  y,
  w,
  title,
  lines,
  accent,
  fill = COLORS.white,
) {
  const bodyLines = toBulletLines(lines);
  const h = 0.78 + bodyLines.length * 0.16;
  addPanel(slide, x, y, w, h, fill);
  slide.addShape("rect", {
    x: x + 0.14,
    y: y + 0.14,
    w: 0.92,
    h: 0.05,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(title, {
    x: x + 0.14,
    y: y + 0.24,
    w: w - 0.28,
    h: 0.16,
    fontFace: "Avenir Next",
    fontSize: 9,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  slide.addText(bodyLines.join("\n"), {
    x: x + 0.14,
    y: y + 0.46,
    w: w - 0.28,
    h: h - 0.54,
    fontFace: "Avenir Next",
    fontSize: 9.35,
    color: COLORS.ink,
    margin: 0,
  });
}

function addAgendaBlock(slide, x, y, w, title, items, accent) {
  addPanel(slide, x, y, w, 4.1, COLORS.white);
  slide.addShape("rect", {
    x: x + 0.18,
    y: y + 0.18,
    w: 1.02,
    h: 0.06,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(title, {
    x: x + 0.18,
    y: y + 0.36,
    w: w - 0.36,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 10.6,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  items.forEach((item, idx) => {
    const top = y + 0.66 + idx * 0.46;
    slide.addShape("rect", {
      x: x + 0.18,
      y: top + 0.02,
      w: 0.32,
      h: 0.32,
      radius: 0.05,
      line: { color: accent, transparency: 100 },
      fill: { color: accent },
    });
    slide.addText(String(item.number), {
      x: x + 0.18,
      y: top + 0.03,
      w: 0.32,
      h: 0.16,
      fontFace: "Avenir Next",
      fontSize: 8.5,
      bold: true,
      color: COLORS.white,
      align: "center",
      margin: 0,
    });
    slide.addText(item.title, {
      x: x + 0.62,
      y: top,
      w: w - 0.84,
      h: 0.18,
      fontFace: "Avenir Next",
      fontSize: 9.8,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(item.subtext || "", {
      x: x + 0.62,
      y: top + 0.2,
      w: w - 0.84,
      h: 0.18,
      fontFace: "Avenir Next",
      fontSize: 8,
      color: COLORS.muted,
      margin: 0,
    });
  });
}

function coverSummaryCards(slots) {
  if (!slots) {
    return [
      {
        title: "Q2 active ARR",
        value: "€x.xM",
        note: "Current-quarter active pipeline",
      },
      {
        title: "FY26 active ARR",
        value: "€x.xM",
        note: "Active FY26 pipeline",
      },
      {
        title: "Open renewal ACV",
        value: "€x.xM",
        note: "Renewal exposure in book",
      },
      {
        title: "Approval backlog",
        value: "xx",
        note: "Stage 3+ candidates missing approval",
      },
    ];
  }
  return [
    {
      title: "Q2 active ARR",
      value: formatTableValue(slots.headline_pipeline_arr_q2, "—"),
      note: "Current-quarter active pipeline",
    },
    {
      title: "FY26 active ARR",
      value: formatTableValue(slots.headline_pipeline_arr_fy26, "—"),
      note: "Active FY26 pipeline",
    },
    {
      title: "Open renewal ACV",
      value: formatTableValue(slots.headline_renewal_acv, "—"),
      note: "Renewal exposure in book",
    },
    {
      title: "Approval backlog",
      value: formatTableValue(slots.missing_approval_candidate_count, "—"),
      note: "Stage 3+ candidates missing approval",
    },
  ];
}

function addCardGridSlide(
  slide,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
  accent,
) {
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    accent,
    { showQuestionBox: !populated },
  );
  const cards = slideDef.cards || [];
  const cols = cards.length === 3 ? 3 : 4;
  const cardW = cols === 3 ? 3.72 : 2.8;
  const gap = 0.18;
  let x = 0.58;
  const y = header.contentY;
  const cardH = populated ? 2.78 : 3.28;
  cards.forEach((card, idx) => {
    addCard(
      slide,
      x,
      y,
      cardW,
      cardH,
      card,
      accent,
      inferCardValue(slideDef, slidePayload, idx),
      inferCardBody(slideDef, slidePayload, idx, card),
      inferCardLabel(slideDef, slidePayload, idx, card),
    );
    x += cardW + gap;
  });
  const slots = (slidePayload && slidePayload.slots) || {};
  const interpretationLabel =
    slideDef.id === "q1-review"
      ? "Baseline note"
        : slideDef.id === "quarterly-pipeline"
          ? "Forecast read"
        : slideDef.id === "renewals-retention"
          ? "Renewal risk"
          : "Interpretation";
  const guardrailLabel =
    slideDef.id === "executive-summary" ? "Top risk" : "Guardrail";
  const noteLines =
    slideDef.id === "executive-summary"
      ? [
          slots.top_risk ||
            "Identify the most significant risk to this quarter's outcome.",
        ]
      : slideDef.id === "q1-review"
        ? [
            "Promise baseline is directional — formal targets not yet integrated.",
          ]
        : slideDef.id === "quarterly-pipeline"
          ? [
              ...(slots.quarterly_pipeline_footnote
                ? [slots.quarterly_pipeline_footnote]
                : []),
              `Keep Omitted visible outside the ${quarterlyPipelineLabel(slots)} active headline.`,
            ]
          : slideDef.id === "renewals-retention"
            ? [slots.renewal_risk_bucket_summary || slideDef.subtitle]
            : [
                slideDef.subtitle ||
                  (slideDef.anti_patterns || [])[0] ||
                  "Keep labels and units explicit.",
              ];
  if (populated && slideDef.id === "renewals-retention") {
    const renewalRows = watchlistTableRows(slots.renewal_watchlist || [], [
      "Opportunity",
      "ACV",
      "Close date",
      "Owner",
      "Next step",
    ]);
    addPanel(slide, 0.58, y + cardH + 0.16, 7.32, 2.18, COLORS.white);
    slide.addText("Named renewal watchlist", {
      x: 0.82,
      y: y + cardH + 0.38,
      w: 2.8,
      h: 0.2,
      fontFace: "Avenir Next",
      fontSize: 10.8,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(
      "Carry the named quarter renewals in view even when formal risk tagging is sparse.",
      {
        x: 2.72,
        y: y + cardH + 0.39,
        w: 4.72,
        h: 0.18,
        fontFace: "Avenir Next",
        fontSize: 8.6,
        color: COLORS.muted,
        margin: 0,
      },
    );
    slide.addTable(renewalRows, {
      x: 0.82,
      y: y + cardH + 0.72,
      w: 6.82,
      h: 1.26,
      border: { pt: 0.75, color: COLORS.border },
      fill: COLORS.white,
      color: COLORS.ink,
      fontFace: "Avenir Next",
      fontSize: 8.6,
      rowH: 0.28,
      colW: [2.76, 0.78, 0.92, 1.0, 1.36],
      bold: true,
      margin: 0.04,
    });
    addPanel(slide, 8.08, y + cardH + 0.16, 4.14, 2.18, COLORS.paper);
    slide.addText("Leadership action", {
      x: 8.34,
      y: y + cardH + 0.38,
      w: 2.2,
      h: 0.2,
      fontFace: "Avenir Next",
      fontSize: 10.6,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(formatTableValue(slots.top_action, slideDef.action_seam), {
      x: 8.34,
      y: y + cardH + 0.68,
      w: 3.58,
      h: 0.56,
      fontFace: "Avenir Next",
      fontSize: 10.1,
      color: COLORS.charcoal,
      margin: 0,
    });
    addSmallRuleBox(
      slide,
      8.34,
      y + cardH + 1.4,
      3.58,
      "Risk note",
      toBulletLines(noteLines),
      COLORS.rustSoft,
    );
    addFooter(slide, pageNum, snapshotDate);
    return;
  }
  if (populated && slideDef.id === "commercial-approval-overview") {
    const approvedRows = watchlistTableRows(slots.approved_deals_2026 || [], [
      "Opportunity",
      "ARR",
      "Stage",
      "Owner",
      "Approval date",
    ]);
    addPanel(slide, 0.58, y + cardH + 0.16, 7.32, 2.18, COLORS.white);
    slide.addText("Approved deals 2026", {
      x: 0.82,
      y: y + cardH + 0.38,
      w: 2.8,
      h: 0.2,
      fontFace: "Avenir Next",
      fontSize: 10.8,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(
      `${(slots.approved_deals_2026 || []).length} approved in this territory in 2026.`,
      {
        x: 2.72,
        y: y + cardH + 0.39,
        w: 4.72,
        h: 0.18,
        fontFace: "Avenir Next",
        fontSize: 8.6,
        color: COLORS.muted,
        margin: 0,
      },
    );
    slide.addTable(approvedRows, {
      x: 0.82,
      y: y + cardH + 0.72,
      w: 6.82,
      h: 1.26,
      border: { pt: 0.75, color: COLORS.border },
      fill: COLORS.white,
      color: COLORS.ink,
      fontFace: "Avenir Next",
      fontSize: 8.6,
      rowH: 0.28,
      colW: [2.42, 0.86, 1.1, 0.96, 1.0],
      bold: true,
      margin: 0.04,
    });
    const candidateRows = watchlistTableRows(
      slots.missing_approval_candidates_sf || [],
      ["Opportunity", "ARR", "Owner", "Age", "Close date"],
    );
    addPanel(slide, 8.08, y + cardH + 0.16, 4.14, 2.18, COLORS.paper);
    slide.addText("Missing approval candidates", {
      x: 8.34,
      y: y + cardH + 0.38,
      w: 3.5,
      h: 0.2,
      fontFace: "Avenir Next",
      fontSize: 10.6,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addTable(candidateRows, {
      x: 8.18,
      y: y + cardH + 0.68,
      w: 3.84,
      h: 1.26,
      border: { pt: 0.75, color: COLORS.border },
      fill: COLORS.paper,
      color: COLORS.ink,
      fontFace: "Avenir Next",
      fontSize: 7.8,
      rowH: 0.26,
      colW: [1.36, 0.62, 0.78, 0.42, 0.66],
      bold: true,
      margin: 0.04,
    });
    addFooter(slide, pageNum, snapshotDate);
    return;
  }
  if (populated) {
    addSmallRuleBox(
      slide,
      0.58,
      y + cardH + 0.16,
      5.64,
      "Leadership action",
      toBulletLines([slots.top_action || slideDef.action_seam]),
      COLORS.blueSoft,
    );
    addSmallRuleBox(
      slide,
      6.4,
      y + cardH + 0.16,
      5.82,
      slideDef.id === "executive-summary" ? "Top risk" : "Interpretation note",
      toBulletLines(noteLines),
      slideDef.id === "executive-summary" ? COLORS.rustSoft : COLORS.paper,
    );
    addFooter(slide, pageNum, snapshotDate);
    return;
  }
  addSmallRuleBox(
    slide,
    0.58,
    y + cardH + 0.16,
    4.04,
    "Action focus",
    toBulletLines([slots.top_action || slideDef.action_seam]),
    COLORS.blueSoft,
  );
  addSmallRuleBox(
    slide,
    4.8,
    y + cardH + 0.16,
    3.38,
    interpretationLabel,
    toBulletLines([slideDef.title_rewrite_rule]),
    COLORS.paper,
  );
  addSmallRuleBox(
    slide,
    8.36,
    y + cardH + 0.16,
    3.86,
    guardrailLabel,
    toBulletLines(
      slideDef.id === "executive-summary"
        ? [
            slots.top_risk ||
              "Identify the most significant risk to this quarter's outcome.",
          ]
        : (slideDef.anti_patterns || []).slice(0, 2),
      "Do not leave unlabeled metrics.",
    ),
    COLORS.rustSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addCoverageIntelSlide(
  slide,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const topOpps = watchlistTableRows(slots.top_opportunities || [], [
    "Opportunity",
    "ARR",
    "Stage",
    "Next step",
  ]);
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    COLORS.plum,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  addPanel(slide, 0.58, contentY, 3.78, 3.48, COLORS.white);
  slide.addText("Quarter read", {
    x: 0.78,
    y: contentY + 0.18,
    w: 3.4,
    h: 0.22,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(
    formatTableValue(
      slots.pipeline_coverage_statement,
      "State the qualified quarterly read only. Keep the target-gap caveat explicit until quota data is integrated.",
    ),
    {
      x: 0.78,
      y: contentY + 0.5,
      w: 3.18,
      h: 0.86,
      fontFace: "Avenir Next",
      fontSize: 10.5,
      color: COLORS.charcoal,
      margin: 0,
    },
  );
  addSmallRuleBox(
    slide,
    0.78,
    contentY + 1.92,
    3.2,
    "Inspection metrics",
    [
      `Weighted pipeline: ${formatTableValue(slots.weighted_pipeline_arr)}`,
      `Stale / aging ARR: ${formatTableValue(slots.stale_arr)} / ${formatTableValue(slots.aging_arr)}`,
      `Data backlog: ${formatTableValue(slots.data_quality_backlog)}`,
    ],
    COLORS.plumSoft,
  );
  addPanel(slide, 4.54, contentY, 4.48, 3.48, COLORS.white);
  slide.addText(`Top ${quarterlyPipelineLabel(slots)} opportunities`, {
    x: 4.76,
    y: contentY + 0.18,
    w: 2.5,
    h: 0.22,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addTable(topOpps, {
    x: 4.76,
    y: contentY + 0.54,
    w: 4.02,
    h: 2.72,
    border: { pt: 0.75, color: COLORS.border },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Avenir Next",
    fontSize: 9,
    rowH: 0.42,
    colW: [1.38, 0.72, 0.78, 1.14],
    bold: true,
    margin: 0.05,
  });
  addPanel(slide, 9.2, contentY, 3.02, 3.48, COLORS.white);
  slide.addText("Execution pressure", {
    x: 9.38,
    y: contentY + 0.18,
    w: 2.2,
    h: 0.22,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  const hygieneBullets = [
    `Stale ARR: ${formatTableValue(slots.stale_arr)}`,
    `Aging ARR: ${formatTableValue(slots.aging_arr)}`,
    `Data backlog: ${formatTableValue(slots.data_quality_backlog)}`,
    `Competitive watchlist: ${(slots.competitive_loss_watchlist || []).length || 0} rows`,
  ];
  hygieneBullets.forEach((line, idx) => {
    slide.addText(`• ${line}`, {
      x: 9.38,
      y: contentY + 0.54 + idx * 0.48,
      w: 2.44,
      h: 0.22,
      fontFace: "Avenir Next",
      fontSize: 9.8,
      color: COLORS.charcoal,
      margin: 0,
    });
  });
  addSmallRuleBox(
    slide,
    0.58,
    contentY + 3.66,
    6.92,
    "Action focus",
    toBulletLines([slideDef.action_seam]),
    COLORS.plumSoft,
  );
  addSmallRuleBox(
    slide,
    7.68,
    contentY + 3.66,
    4.54,
    "Read carefully",
    toBulletLines((slideDef.anti_patterns || []).slice(0, 2)),
    COLORS.rustSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addWatchlistSlide(
  slide,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
  columns,
  accent,
) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  let rows = [];
  if (slideDef.id === "missing-commercial-approvals")
    rows = slots.missing_approval_candidates || [];
  if (slideDef.id === "slipped-deals")
    rows = slots.slipped_deal_watchlist || [];
  if (slideDef.id === "missing-win-loss-reason")
    rows = slots.missing_win_loss_reason_rows || [];
  const tableRows = watchlistTableRows(rows, columns);
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    accent,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  addSummaryChip(
    slide,
    0.58,
    contentY,
    3.72,
    slideDef.id === "slipped-deals"
      ? "Slip summary"
      : slideDef.id === "missing-win-loss-reason"
        ? "Control summary"
        : "Backlog summary",
    populated
      ? slideDef.id === "slipped-deals"
        ? [`Slipped count and ARR drive the follow-up actions.`]
        : slideDef.id === "missing-win-loss-reason"
          ? [`Accepted exception remains 0 - No Opportunity with no reason.`]
          : [`Backlog is ranked by ARR and limited to the highest-value cases.`]
      : (slideDef.body_guidance || []).slice(0, 2),
    accent,
    COLORS.paper,
  );
  addSummaryChip(
    slide,
    4.46,
    contentY,
    3.72,
    populated ? "Decision" : "Decision rule",
    [
      populated
        ? slideDef.subtitle || slideDef.title_rewrite_rule
        : slideDef.title_rewrite_rule,
    ],
    accent,
    COLORS.white,
  );
  addSummaryChip(
    slide,
    8.34,
    contentY,
    3.88,
    "Action now",
    [
      slideDef.id === "slipped-deals"
        ? slots.slip_root_cause_summary || slideDef.action_seam
        : slideDef.action_seam,
    ],
    accent,
    COLORS.blueSoft,
  );
  const hasRows = Array.isArray(rows) && rows.length > 0;
  const panelY = contentY + 1.68;
  const panelH = hasRows ? 2.78 : 2.42;
  addPanel(slide, 0.58, panelY, 11.64, panelH, COLORS.white);
  slide.addShape("rect", {
    x: 0.58,
    y: panelY + 0.18,
    w: 11.64,
    h: 0.06,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText("Operating watchlist", {
    x: 0.84,
    y: panelY + 0.3,
    w: 2.0,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(
    hasRows
      ? "Top exception rows for this month’s leadership discussion."
      : "Explicit zero-state for the monthly control.",
    {
      x: 2.72,
      y: panelY + 0.3,
      w: 6.2,
      h: 0.18,
      fontFace: "Avenir Next",
      fontSize: 8.8,
      color: COLORS.muted,
      margin: 0,
    },
  );
  if (hasRows) {
    slide.addTable(tableRows, {
      x: 0.84,
      y: panelY + 0.7,
      w: 11.12,
      h: 1.86,
      border: { pt: 0.75, color: COLORS.border },
      fill: COLORS.white,
      color: COLORS.ink,
      fontFace: "Avenir Next",
      fontSize: 9,
      rowH: 0.36,
      colW: Array(columns.length).fill(11.12 / columns.length),
      bold: true,
      margin: 0.05,
    });
  } else {
    const zeroState =
      slideDef.id === "missing-win-loss-reason"
        ? "No outcome rows currently require reason follow-up after applying the accepted 0 - No Opportunity exception."
        : slideDef.id === "missing-commercial-approvals"
          ? "No stage 3+ candidates currently missing commercial approval."
          : "No current watchlist rows require escalation this month.";
    slide.addText(zeroState, {
      x: 0.92,
      y: panelY + 0.94,
      w: 10.8,
      h: 0.6,
      fontFace: "Avenir Next",
      fontSize: 12,
      bold: true,
      color: COLORS.ink,
      align: "center",
      valign: "mid",
      margin: 0,
    });
    slide.addText(
      "Keep the control explicit, but do not force a blank watchlist into the operating narrative.",
      {
        x: 1.42,
        y: panelY + 1.56,
        w: 9.8,
        h: 0.26,
        fontFace: "Avenir Next",
        fontSize: 9.4,
        color: COLORS.muted,
        align: "center",
        margin: 0,
      },
    );
  }
  if (populated) {
    addSmallRuleBox(
      slide,
      0.58,
      5.9,
      5.24,
      hasRows ? "Rows in scope" : "Current state",
      [
        hasRows
          ? `Rows: ${rows.length}.`
          : "No active exception rows require follow-up this month.",
      ],
      COLORS.paper,
    );
    addSmallRuleBox(
      slide,
      5.98,
      5.9,
      6.24,
      "Guardrail",
      [
        slideDef.id === "slipped-deals"
          ? "Do not promise root-cause precision without owner commentary."
          : slideDef.id === "missing-win-loss-reason"
            ? "Do not count accepted 0 - No Opportunity rows as defects."
            : formatTableValue(
                (slideDef.anti_patterns || [])[0],
                "Top exceptions by ARR.",
              ),
      ],
      COLORS.rustSoft,
    );
    addFooter(slide, pageNum, snapshotDate);
    return;
  }
  addSmallRuleBox(
    slide,
    0.58,
    contentY + 4.76,
    3.28,
    "Read carefully",
    toBulletLines((slideDef.anti_patterns || []).slice(0, 2)),
    COLORS.rustSoft,
  );
  addSmallRuleBox(
    slide,
    4.04,
    contentY + 4.76,
    3.52,
    "Review rule",
    [
      "Do not paste an exported report.",
      "Keep the table to the top exceptions only.",
    ],
    COLORS.paper,
  );
  addSmallRuleBox(
    slide,
    7.74,
    contentY + 4.76,
    4.48,
    "Data in view",
    [
      `Rows: ${Array.isArray(rows) ? rows.length : 0}`,
      "Owner, value, stage, and required follow-up.",
    ],
    COLORS.blueSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addOverdueSlide(slide, slideDef, slidePayload, pageNum, snapshotDate) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const tableRows = watchlistTableRows(slots.overdue_close_watchlist || [], [
    "Opportunity",
    "ARR",
    "Close date",
    "Owner",
  ]);
  const ownerRows = ownerSummaryRows(slots.overdue_close_owner_summary || []);
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    COLORS.rust,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  addSummaryChip(
    slide,
    0.58,
    contentY,
    3.56,
    "Operating read",
    [
      `Overdue open count: ${formatTableValue(slots.overdue_close_count)}`,
      "Keep the ranking by record count visible.",
    ],
    COLORS.rust,
    COLORS.paper,
  );
  addSummaryChip(
    slide,
    4.32,
    contentY,
    3.56,
    "Executive read",
    [slideDef.title_rewrite_rule],
    COLORS.rust,
    COLORS.white,
  );
  addSummaryChip(
    slide,
    8.06,
    contentY,
    4.16,
    "Action focus",
    [slideDef.action_seam],
    COLORS.rust,
    COLORS.rustSoft,
  );
  addPanel(slide, 0.58, contentY + 1.74, 7.45, 2.78, COLORS.white);
  slide.addText("Ranked overdue watchlist", {
    x: 0.82,
    y: contentY + 2.0,
    w: 2.4,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addTable(tableRows, {
    x: 0.82,
    y: contentY + 2.4,
    w: 6.95,
    h: 1.68,
    border: { pt: 0.75, color: COLORS.border },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Avenir Next",
    fontSize: 9.2,
    rowH: 0.48,
    colW: [2.84, 1.0, 1.46, 1.65],
    bold: true,
    margin: 0.05,
  });
  addPanel(slide, 8.28, contentY + 1.74, 3.94, 2.78, COLORS.rustSoft);
  slide.addText("Owner concentration", {
    x: 8.52,
    y: contentY + 2.0,
    w: 2.4,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  if (!ownerRows.length) {
    slide.addText(
      "No owner concentration is shown because no overdue open records are in scope this month.",
      {
        x: 8.52,
        y: 4.58,
        w: 3.2,
        h: 0.7,
        fontFace: "Avenir Next",
        fontSize: 9.6,
        color: COLORS.charcoal,
        margin: 0,
      },
    );
  } else {
    ownerRows.forEach((row, idx) => {
      slide.addShape("rect", {
        x: 8.54,
        y: contentY + 2.4 + idx * 0.34,
        w: 2.9 - idx * 0.28,
        h: 0.12,
        line: { color: COLORS.rust, transparency: 100 },
        fill: { color: COLORS.rust },
      });
      slide.addText(`${row.owner}  ${row.record_count}`, {
        x: 8.54,
        y: contentY + 2.2 + idx * 0.34,
        w: 2.9,
        h: 0.16,
        fontFace: "Avenir Next",
        fontSize: 9.2,
        color: COLORS.charcoal,
        margin: 0,
      });
    });
  }
  addSmallRuleBox(
    slide,
    0.58,
    contentY + 4.68,
    3.4,
    "Read carefully",
    toBulletLines((slideDef.anti_patterns || []).slice(0, 2)),
    COLORS.paper,
  );
  addSmallRuleBox(
    slide,
    4.14,
    contentY + 4.68,
    3.44,
    "Sort order",
    [
      "Show the owner summary sorted by largest record count.",
      "Do not sort the summary table alphabetically.",
    ],
    COLORS.blueSoft,
  );
  addSmallRuleBox(
    slide,
    7.74,
    contentY + 4.68,
    4.48,
    "Action focus",
    toBulletLines([slideDef.action_seam]),
    COLORS.rustSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addHygieneSlide(slide, slideDef, slidePayload, pageNum, snapshotDate) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    COLORS.gold,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  const kpis = [
    ["No Activity", "Open opps without recent activity"],
    ["Overdue Close", "Open opps already past close date"],
    ["Missing Next Step", "Current missing next-step discipline"],
    ["Total Hygiene Load", "Total issue-points across the book"],
  ];
  let x = 0.58;
  kpis.forEach(([label, body], idx) => {
    addCard(
      slide,
      x,
      contentY,
      2.82,
      1.92,
      { metric_hint: "", label, body },
      COLORS.gold,
      [
        slots.no_activity_count,
        slots.overdue_close_count,
        slots.missing_next_step_count,
        slots.total_data_quality_issues,
      ][idx] || null,
    );
    x += 2.92;
  });
  addPanel(slide, 0.58, contentY + 2.18, 5.78, 2.2, COLORS.white);
  slide.addText("Issue-carrying reps", {
    x: 0.82,
    y: contentY + 2.34,
    w: 2.0,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addTable(
    [
      ["Rep", "Issue load", "No activity", "Overdue"],
      ...(slots.top_issue_reps || [])
        .slice(0, 3)
        .map((row) => [
          formatTableValue(row.rep),
          formatTableValue(row.total_issues),
          formatTableValue(row.no_activity),
          formatTableValue(row.overdue_close),
        ]),
    ],
    {
      x: 0.82,
      y: contentY + 2.7,
      w: 5.26,
      h: 1.18,
      border: { pt: 0.75, color: COLORS.border },
      fill: COLORS.white,
      color: COLORS.ink,
      fontFace: "Avenir Next",
      fontSize: 9.1,
      rowH: 0.38,
      colW: [1.7, 1.2, 1.1, 1.0],
      bold: true,
      margin: 0.05,
    },
  );
  addPanel(slide, 6.52, contentY + 2.18, 5.7, 2.2, COLORS.white);
  slide.addText("Risk-register anchors", {
    x: 6.76,
    y: contentY + 2.34,
    w: 2.4,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(
    formatTableValue(
      slots.rep_concentration_summary,
      "Show the few largest ARR deals with high activity gap, push count, or long stage duration so the slide drives inspection rather than generic hygiene reporting.",
    ),
    {
      x: 6.76,
      y: contentY + 2.7,
      w: 5.0,
      h: 0.76,
      fontFace: "Avenir Next",
      fontSize: 9.9,
      color: COLORS.charcoal,
      margin: 0,
    },
  );
  addSmallRuleBox(
    slide,
    6.76,
    contentY + 3.52,
    4.7,
    "Read carefully",
    toBulletLines((slideDef.anti_patterns || []).slice(0, 2)),
    COLORS.goldSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addPlaceholderStatusSlide(
  slide,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const nextStepLines = slots.finance_churn_owner
    ? [slots.finance_churn_owner, ...(slideDef.body_guidance || [])]
    : toBulletLines(slideDef.body_guidance);
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    COLORS.rust,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  addPanel(slide, 1.0, contentY + 0.12, 10.8, 3.8, COLORS.paper, COLORS.border);
  slide.addText(
    formatTableValue(
      slots.finance_churn_inputs_status,
      "Finance source not yet integrated",
    ),
    {
      x: 1.28,
      y: contentY + 0.54,
      w: 6.0,
      h: 0.42,
      fontFace: "Avenir Next",
      fontSize: 22,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    },
  );
  slide.addText(
    formatTableValue(
      slots.churn_placeholder_notes,
      "Hold this slide as a controlled status read until Finance churn inputs are wired into the monthly contract.",
    ),
    {
      x: 1.28,
      y: contentY + 1.1,
      w: 5.7,
      h: 0.88,
      fontFace: "Avenir Next",
      fontSize: 11.2,
      color: COLORS.charcoal,
      margin: 0,
    },
  );
  addSmallRuleBox(
    slide,
    1.28,
    contentY + 2.38,
    5.68,
    "Current rule",
    [
      "Keep churn outside the leadership headline until the Finance feed is integrated.",
    ],
    COLORS.blueSoft,
  );
  addSmallRuleBox(
    slide,
    7.38,
    contentY + 0.62,
    3.96,
    "Finance owner",
    nextStepLines,
    COLORS.white,
  );
  addSmallRuleBox(
    slide,
    7.38,
    contentY + 2.56,
    3.96,
    "Guardrail",
    toBulletLines((slideDef.anti_patterns || []).slice(0, 2)),
    COLORS.rustSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function addAppendixSlide(
  slide,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
) {
  const slots = (slidePayload && slidePayload.slots) || {};
  const populated = Boolean(
    slidePayload &&
    slidePayload.slots &&
    Object.keys(slidePayload.slots).length,
  );
  const header = addSlideHeader(
    slide,
    inferSlideTitle(slideDef, slidePayload),
    inferSlideSubtitle(slideDef, slidePayload),
    slideDef.management_question,
    COLORS.tealDeep,
    { showQuestionBox: !populated },
  );
  const contentY = header.contentY;
  const blocks = [
    {
      x: 0.58,
      y: contentY,
      w: 5.7,
      title: "Metric rules",
      lines: [
        "Pipeline metrics are ARR in EUR converted.",
        "Renewal metrics are ACV in EUR converted.",
        "Omitted stays separate from active headline pipeline.",
      ],
      fill: COLORS.white,
    },
    {
      x: 6.5,
      y: contentY,
      w: 5.72,
      title: "Interpretation limits",
      lines: [
        "Q1 promise baseline remains qualified.",
        "Coverage remains a proxy until target inputs exist.",
        "Churn and KYC stay explicit source gaps.",
      ],
      fill: COLORS.white,
    },
    {
      x: 0.58,
      y: contentY + 1.74,
      w: 5.7,
      title: "Source tabs",
      lines: [
        "Scorecard, Pipeline Detail, Q1 Review, Won-Lost",
        "Q2 Outlook, Commercial Approval, Renewals & Retention",
        "Risk Register, Data Quality, Sources & Lineage",
      ],
      fill: COLORS.paper,
    },
    {
      x: 6.5,
      y: contentY + 1.74,
      w: 5.72,
      title: "How to use",
      lines: [
        formatTableValue(
          (slots.metric_definition_notes || [])[0],
          "The deck should be concise enough for monthly discussion.",
        ),
        formatTableValue(
          (slots.metric_definition_notes || [])[1],
          "Use the appendix to prevent misreadings, not to restate the whole deck.",
        ),
      ],
      fill: COLORS.paper,
    },
  ];
  blocks.forEach((block) =>
    addSmallRuleBox(
      slide,
      block.x,
      block.y,
      block.w,
      block.title,
      block.lines,
      block.fill,
    ),
  );
  addSmallRuleBox(
    slide,
    0.58,
    contentY + 3.72,
    11.64,
    "Meeting use",
    [
      "Use this page to settle metric disputes quickly and keep unsupported claims out of the live discussion.",
    ],
    COLORS.tealSoft,
  );
  addFooter(slide, pageNum, snapshotDate);
}

function renderSlideByFamily(
  pptx,
  slideDef,
  slidePayload,
  pageNum,
  snapshotDate,
) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };

  switch (slideDef.visual_family) {
    case "four-card-kpi-strip":
    case "four-card-outcome-strip":
    case "forecast-mix-strip":
    case "governance-kpi-strip":
      addCardGridSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        COLORS.teal,
      );
      break;
    case "three-panel-renewal-watchlist":
      addCardGridSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        COLORS.green,
      );
      break;
    case "hero-watchlist-plus-controls":
      addCoverageIntelSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
      );
      break;
    case "ranked-watchlist-table":
      addWatchlistSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        ["Opportunity", "ARR", "Owner", "Stage", "Action"],
        COLORS.plum,
      );
      break;
    case "ranked-watchlist-with-summary":
      addWatchlistSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        ["Opportunity", "ARR", "Close date", "Owner", "Follow-up"],
        COLORS.rust,
      );
      break;
    case "control-kpi-plus-watchlist":
      addHygieneSlide(slide, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    case "control-exception-table":
      addWatchlistSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        ["Opportunity", "ARR", "Stage", "Owner", "Reason gap"],
        COLORS.gold,
      );
      break;
    case "watchlist-plus-owner-summary":
      addOverdueSlide(slide, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    case "placeholder-status":
      addPlaceholderStatusSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
      );
      break;
    case "appendix-notes":
      addAppendixSlide(slide, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    default:
      addWatchlistSlide(
        slide,
        slideDef,
        slidePayload,
        pageNum,
        snapshotDate,
        ["Opportunity", "Value", "Owner", "Status"],
        COLORS.teal,
      );
      break;
  }

  validateSlide(slide, pptx);
}

function addCover(pptx, directorName, territory, snapshotDate, fillPayload) {
  const executiveSlide = payloadForSlide(fillPayload, "executive-summary");
  const approvalSlide = payloadForSlide(
    fillPayload,
    "commercial-approval-overview",
  );
  const execSlots = (executiveSlide && executiveSlide.slots) || {};
  const approvalSlots = (approvalSlide && approvalSlide.slots) || {};
  const coverSlots = { ...execSlots, ...approvalSlots };
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.white };
  slide.addShape("rect", {
    x: 0,
    y: 0,
    w: 4.72,
    h: 7.2,
    line: { color: COLORS.tealDeep, transparency: 100 },
    fill: { color: COLORS.tealDeep },
  });
  slide.addText("Sales Director\nOperating Review", {
    x: 0.64,
    y: 0.94,
    w: 3.48,
    h: 1.0,
    fontFace: "Avenir Next",
    fontSize: 26,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(`${directorName}\n${territory}`, {
    x: 0.64,
    y: 2.2,
    w: 3.48,
    h: 0.84,
    fontFace: "Avenir Next",
    fontSize: 16.4,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(`Monthly director operating review | ${snapshotDate}`, {
    x: 0.64,
    y: 3.34,
    w: 3.42,
    h: 0.24,
    fontFace: "Avenir Next",
    fontSize: 11,
    color: "D7EFF1",
    margin: 0,
  });
  slide.addText("Monthly operating review", {
    x: 0.64,
    y: 3.7,
    w: 2.86,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 9.4,
    bold: true,
    color: "D7EFF1",
    margin: 0,
  });
  slide.addText(
    "Pipeline, approvals, renewals, slippage, and Salesforce controls in one leadership pack.",
    {
      x: 0.64,
      y: 3.9,
      w: 3.42,
      h: 0.5,
      fontFace: "Avenir Next",
      fontSize: 10.1,
      color: "D7EFF1",
      margin: 0,
    },
  );
  addWordmark(slide, 0.64, 0.5, 1.15, 0.36);
  const summaryCards = coverSummaryCards(coverSlots);
  summaryCards.forEach((card, idx) => {
    const x = 4.98 + idx * 1.78;
    addPanel(slide, x, 0.86, 1.62, 1.46, COLORS.paper, COLORS.border);
    slide.addShape("rect", {
      x,
      y: 0.86,
      w: 1.62,
      h: 0.05,
      line: {
        color:
          idx === 0
            ? COLORS.teal
            : idx === 1
              ? COLORS.green
              : idx === 2
                ? COLORS.gold
                : COLORS.rust,
        transparency: 100,
      },
      fill: {
        color:
          idx === 0
            ? COLORS.teal
            : idx === 1
              ? COLORS.green
              : idx === 2
                ? COLORS.gold
                : COLORS.rust,
      },
    });
    slide.addText(card.title, {
      x: x + 0.12,
      y: 1.03,
      w: 1.36,
      h: 0.24,
      fontFace: "Avenir Next",
      fontSize: 8.7,
      bold: true,
      color: COLORS.muted,
      margin: 0,
    });
    slide.addText(card.value, {
      x: x + 0.12,
      y: 1.34,
      w: 1.36,
      h: 0.28,
      fontFace: "Avenir Next",
      fontSize: 16.4,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(card.note, {
      x: x + 0.12,
      y: 1.72,
      w: 1.36,
      h: 0.28,
      fontFace: "Avenir Next",
      fontSize: 8.1,
      color: COLORS.muted,
      margin: 0,
    });
  });
  addPanel(slide, 4.98, 2.52, 7.24, 2.62, COLORS.paper, COLORS.border);
  slide.addShape("rect", {
    x: 4.98,
    y: 2.52,
    w: 0.1,
    h: 2.62,
    line: { color: COLORS.teal, transparency: 100 },
    fill: { color: COLORS.teal },
  });
  slide.addText("Operating headline", {
    x: 5.28,
    y: 2.78,
    w: 2.6,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 10.8,
    bold: true,
    color: COLORS.tealDeep,
    margin: 0,
  });
  slide.addText(
    formatTableValue(
      execSlots.top_action,
      "Use this cover to state the operating posture in one sentence: quarter quality, approval burden, and the one action leadership needs to sponsor.",
    ),
    {
      x: 5.28,
      y: 3.04,
      w: 6.34,
      h: 1.02,
      fontFace: "Avenir Next",
      fontSize: 15.2,
      bold: true,
      color: COLORS.charcoal,
      margin: 0,
    },
  );
  addSmallRuleBox(
    slide,
    5.28,
    4.26,
    3.08,
    "Primary risk",
    [
      formatTableValue(
        execSlots.top_risk,
        "Top risk to this quarter's outcome.",
      ),
    ],
    COLORS.rustSoft,
  );
  addSmallRuleBox(
    slide,
    8.54,
    4.26,
    3.08,
    "Data discipline",
    [
      "Salesforce and workbook controls.",
      "ARR, ACV, and Omitted stay separate.",
    ],
    COLORS.white,
  );
  addFooter(slide, 1, snapshotDate);
  validateSlide(slide, pptx);
}

function addAgenda(pptx, contract, snapshotDate, fillPayload) {
  const execSlide = payloadForSlide(fillPayload, "executive-summary");
  const execSlots = (execSlide && execSlide.slots) || {};
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  addTopRule(slide, COLORS.teal);
  slide.addText("Review sequence", {
    x: 0.58,
    y: 0.84,
    w: 8.8,
    h: 0.4,
    fontFace: "Avenir Next",
    fontSize: 22,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText("Monthly sequence for every MD-1 review", {
    x: 0.58,
    y: 1.22,
    w: 6.2,
    h: 0.22,
    fontFace: "Avenir Next",
    fontSize: 10,
    color: COLORS.muted,
    margin: 0,
  });
  addWordmark(slide);
  addAgendaBlock(
    slide,
    0.58,
    1.64,
    5.8,
    "Performance and quarter read",
    [
      {
        number: 1,
        title: "Executive Summary",
        subtext: "Headline posture, pipeline, renewals, action.",
      },
      {
        number: 2,
        title: "Q1 Promised vs Delivered",
        subtext: "Qualified promise baseline and delivery gap.",
      },
      {
        number: 3,
        title: "Quarterly Pipeline and Forecast",
        subtext: "Active Q2 pipeline, commit, best case, omitted.",
      },
      {
        number: 4,
        title: "Quarterly Opportunity Intel and Coverage Proxy",
        subtext: "Current-quarter watchlist and pressure points.",
      },
      {
        number: 5,
        title: "Commercial Approval Overview",
        subtext: "Approval throughput and exposure.",
      },
      {
        number: 6,
        title: "Missing Commercial Approval Candidates",
        subtext: "Curated candidate watchlist.",
      },
    ],
    COLORS.teal,
  );
  addAgendaBlock(
    slide,
    6.42,
    1.64,
    5.8,
    "Controls and source gaps",
    [
      {
        number: 7,
        title: "Renewals and Retention",
        subtext: "Open renewal ACV and likelihood read.",
      },
      {
        number: 8,
        title: "Slipped Deals and Follow-up",
        subtext: "Largest moved deals and next actions.",
      },
      {
        number: 9,
        title: "Salesforce Hygiene and Activity Controls",
        subtext: "Inspection backlog and issue-carrying reps.",
      },
      {
        number: 10,
        title: "Missing Win/Loss Reason",
        subtext: "Reason discipline with explicit exceptions.",
      },
      {
        number: 11,
        title: "Overdue Close Date Open Opportunities",
        subtext: "Past-close burden and owner concentration.",
      },
      {
        number: 12,
        title: "Churn Risk and Finance Inputs",
        subtext: "Controlled placeholder until Finance is wired.",
      },
      {
        number: 13,
        title: "Appendix and Factual Notes",
        subtext: "Definitions, caveats, and lineage.",
      },
    ],
    COLORS.gold,
  );
  addSmallRuleBox(
    slide,
    8.56,
    5.32,
    3.1,
    "Read this deck",
    [
      "One decision per slide.",
      "Message title on populated deck.",
      "No raw report dumps in main body.",
    ],
    COLORS.tealSoft,
  );
  addSmallRuleBox(
    slide,
    0.84,
    5.32,
    4.92,
    "Current posture",
    [
      `Q2 active ARR: ${formatTableValue(execSlots.headline_pipeline_arr_q2, "€x.xM")}`,
      `Open renewal ACV: ${formatTableValue(execSlots.headline_renewal_acv, "€x.xM")}`,
      "Stale and aging ARR still sit alongside approval backlog.",
    ],
    COLORS.white,
  );
  addFooter(slide, 2, snapshotDate);
  validateSlide(slide, pptx);
}

function buildDeck({
  contractPath,
  outputPath,
  directorName,
  territory,
  snapshotDate,
  fillPayloadPath,
}) {
  const contract = loadJson(contractPath);
  const fillPayload =
    fillPayloadPath && fs.existsSync(fillPayloadPath)
      ? loadJson(fillPayloadPath)
      : null;
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";
  pptx.author = "OpenAI Codex";
  pptx.company = "SimCorp";
  pptx.subject = "Sales Director monthly operating review";
  pptx.title = `Sales Director Monthly Shell - ${directorName} (${territory})`;
  pptx.lang = "en-US";
  pptx.theme = {
    headFontFace: "Avenir Next",
    bodyFontFace: "Avenir Next",
    lang: "en-US",
  };

  addCover(pptx, directorName, territory, snapshotDate, fillPayload);
  addAgenda(pptx, contract, snapshotDate, fillPayload);
  let page = 3;
  for (const slideDef of contract.slides) {
    renderSlideByFamily(
      pptx,
      slideDef,
      payloadForSlide(fillPayload, slideDef.id),
      page,
      snapshotDate,
    );
    page += 1;
  }

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  return pptx.writeFile({ fileName: outputPath }).then(() => ({
    outputPath,
    slideCount: contract.slides.length + 2,
    contractPath,
    fillPayloadPath: fillPayloadPath || null,
  }));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const result = await buildDeck({
    contractPath: args.contract || DEFAULT_CONTRACT,
    outputPath: args.output || DEFAULT_OUTPUT,
    directorName: args["director-name"] || "Sarah Pittroff",
    territory: args.territory || "Central Europe",
    snapshotDate: args["snapshot-date"] || "2026-04-10",
    fillPayloadPath: args["fill-payload"] || null,
  });
  console.log(JSON.stringify(result, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
