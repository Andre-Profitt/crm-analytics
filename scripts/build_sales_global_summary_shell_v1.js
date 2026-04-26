const fs = require("fs");
const path = require("path");
const PptxGenJS = require("pptxgenjs");
const HELPER_DIR = path.resolve(__dirname, "../output/sales_director_monthly_deck_2026-03-31/pptxgenjs_helpers");
const { calcTextBoxHeightSimple } = require(path.join(HELPER_DIR, "text"));
const { warnIfSlideHasOverlaps, warnIfSlideElementsOutOfBounds } = require(path.join(HELPER_DIR, "layout"));

const DEFAULT_CONTRACT = path.resolve(__dirname, "../config/sales_global_summary_shell.json");

const COLORS = {
  paper: "F7FAFB",
  white: "FFFFFF",
  ink: "123040",
  charcoal: "0F2430",
  muted: "5C7482",
  border: "D7E2E8",
  teal: "0A6C74",
  tealDeep: "0A4D57",
  tealSoft: "DCEEF0",
  rust: "B45A43",
  rustSoft: "F7E3DD",
  gold: "A7852C",
  goldSoft: "F6EBCB",
  plum: "7A308F",
  plumSoft: "F1E7F5",
  green: "2F7D57",
  greenSoft: "E4F2E9",
  blueSoft: "E8F0F5",
};
const SHOULD_VALIDATE_LAYOUT = process.env.SD_GLOBAL_SHELL_VALIDATE_LAYOUT !== "0";

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

function payloadForSlide(fillPayload, slideId) {
  if (!fillPayload || !Array.isArray(fillPayload.slides)) return null;
  return fillPayload.slides.find((slide) => slide.id === slideId) || null;
}

function normalizeText(value, fallback = "—") {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function ensureArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function regionQuarterLabel(slots) {
  return normalizeText(slots.quarterly_pipeline_label, "Q2");
}

function regionQuarterTitle(slots) {
  return normalizeText(slots.quarterly_pipeline_title, regionQuarterLabel(slots));
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
  slide.addText(`Validated baseline | ${snapshotDate}`, {
    x: 0.55,
    y: 7.02,
    w: 4.2,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 8.5,
    color: COLORS.muted,
  });
}

function addTopRule(slide, color, y = 0.74) {
  slide.addShape("rect", {
    x: 0.58,
    y,
    w: 1.35,
    h: 0.045,
    line: { color, transparency: 100 },
    fill: { color },
  });
}

function addSlideHeader(slide, title, subtitle, managementQuestion, accent = COLORS.teal, options = {}) {
  const showQuestionBox = options.showQuestionBox !== false;
  addTopRule(slide, accent);
  const titleFontSize = title.length > 72 ? 18.5 : title.length > 56 ? 20 : 22;
  const titleLines = title.length > 72 ? 3 : title.length > 56 ? 2 : 1;
  const titleHeight = titleLines === 1 ? 0.34 : titleLines === 2 ? 0.66 : 0.92;
  slide.addText(title, {
    x: 0.58,
    y: 0.88,
    w: 8.7,
    h: titleHeight,
    fontFace: "Avenir Next",
    fontSize: titleFontSize,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  const subtitleY = 0.88 + titleHeight + 0.05;
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
    const qHeight = calcTextBoxHeightSimple(9.2, 10.5, 2, 0.12);
    const boxY = subtitleY + 0.32;
    const boxH = Math.max(0.34, qHeight);
    slide.addShape("rect", {
      x: 0.58,
      y: boxY,
      w: 11.65,
      h: boxH,
      line: { color: accent, transparency: 100 },
      fill: { color: COLORS.paper },
    });
    slide.addText(managementQuestion, {
      x: 0.76,
      y: boxY + 0.09,
      w: 11.2,
      h: Math.max(0.2, boxH - 0.1),
      fontFace: "Avenir Next",
      fontSize: 10.1,
      color: COLORS.ink,
      margin: 0,
    });
    contentY = boxY + boxH + 0.18;
  }
  slide.addText("SIMCORP", {
    x: 11.0,
    y: 0.34,
    w: 1.1,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 10,
    bold: true,
    color: COLORS.teal,
    align: "right",
    margin: 0,
  });
  return { contentY };
}

function addPanel(slide, x, y, w, h, fill = COLORS.white, line = COLORS.border) {
  slide.addShape("rect", {
    x,
    y,
    w,
    h,
    line: { color: line, pt: 1 },
    fill: { color: fill },
  });
}

function addKpiCard(slide, x, y, w, h, label, value, body, accent) {
  addPanel(slide, x, y, w, h, COLORS.white);
  slide.addShape("rect", {
    x,
    y,
    w,
    h: 0.05,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(label, {
    x: x + 0.18,
    y: y + 0.2,
    w: w - 0.3,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 9.2,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  slide.addText(normalizeText(value), {
    x: x + 0.18,
    y: y + 0.48,
    w: w - 0.3,
    h: 0.35,
    fontFace: "Avenir Next",
    fontSize: 19,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(body || "", {
    x: x + 0.18,
    y: y + 0.94,
    w: w - 0.3,
    h: h - 1.08,
    fontFace: "Avenir Next",
    fontSize: 10.5,
    color: COLORS.charcoal,
    margin: 0,
    valign: "top",
  });
}

function addTextPanel(slide, x, y, w, h, title, bodyLines, accent, fill = COLORS.white) {
  addPanel(slide, x, y, w, h, fill);
  slide.addShape("rect", {
    x: x + 0.18,
    y: y + 0.16,
    w: 1.0,
    h: 0.05,
    line: { color: accent, transparency: 100 },
    fill: { color: accent },
  });
  slide.addText(title, {
    x: x + 0.18,
    y: y + 0.32,
    w: w - 0.36,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 10,
    bold: true,
    color: COLORS.muted,
    margin: 0,
  });
  slide.addText(ensureArray(bodyLines).join("\n"), {
    x: x + 0.18,
    y: y + 0.58,
    w: w - 0.36,
    h: h - 0.74,
    fontFace: "Avenir Next",
    fontSize: 10.3,
    color: COLORS.ink,
    margin: 0,
    valign: "top",
  });
}

function addSimpleTable(slide, x, y, w, headers, rows, accent, rowLimit = 5) {
  const data = [headers];
  const trimmed = ensureArray(rows).slice(0, rowLimit);
  trimmed.forEach((row) => data.push(row));
  while (data.length < rowLimit + 1) {
    data.push(headers.map(() => "—"));
  }
  slide.addTable(data, {
    x,
    y,
    w,
    border: { type: "solid", pt: 1, color: COLORS.border },
    fill: COLORS.white,
    color: COLORS.ink,
    fontFace: "Avenir Next",
    fontSize: 9.2,
    margin: 0.05,
    rowH: 0.42,
    autoFit: false,
    bold: false,
    valign: "mid",
    colW: headers.map(() => w / headers.length),
    cellProps: [
      {
        row: 0,
        fill: accent,
        color: COLORS.white,
        bold: true,
        fontSize: 9.5,
      },
    ],
  });
}

function slideTitle(slideDef, slidePayload) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "global-executive-summary":
      return `Global Q2 active ARR is ${normalizeText(slots.global_pipeline_arr_q2)} with ${normalizeText(slots.global_renewal_acv_q2)} open renewal ACV`;
    case "apac-region-summary":
    case "emea-region-summary":
    case "north-america-region-summary":
      return `${normalizeText(slots.region_name)} has ${normalizeText(slots.headline_pipeline_arr_q2)} ${regionQuarterLabel(slots)} active ARR and ${normalizeText(slots.renewal_open_acv)} open renewal ACV`;
    case "global-commercial-approval-overview": {
      const first = ensureArray(slots.largest_global_missing_candidates)[0];
      return first
        ? `Global approval backlog is concentrated in ${normalizeText(first.opportunity)}`
        : slideDef.title;
    }
    default:
      return slideDef.title;
  }
}

function slideSubtitle(slideDef, slidePayload) {
  const slots = (slidePayload && slidePayload.slots) || {};
  switch (slideDef.id) {
    case "global-executive-summary":
      return normalizeText(slots.global_top_risk, slideDef.subtitle);
    case "apac-region-summary":
    case "emea-region-summary":
    case "north-america-region-summary":
      if (slots.quarterly_pipeline_display_reason === "forward_quarter_fallback") {
        return `Showing ${regionQuarterTitle(slots)} because the current quarter is empty.`;
      }
      if (slots.quarterly_pipeline_display_reason === "empty_current_and_forward") {
        return "No current or forward-quarter pipeline is available; keep the empty-state note explicit.";
      }
      return normalizeText(slots.top_risk, slideDef.subtitle);
    case "global-commercial-approval-overview":
      return "Approved 2026 exposure and missing-approval backlog ranked by ARR.";
    default:
      return slideDef.subtitle || "";
  }
}

function addCover(pptx, snapshotDate, fillPayload) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  slide.addShape("rect", {
    x: 0,
    y: 0,
    w: 4.34,
    h: 7.2,
    line: { color: COLORS.tealDeep, transparency: 100 },
    fill: { color: COLORS.tealDeep },
  });
  slide.addText("SIMCORP", {
    x: 0.64,
    y: 0.72,
    w: 1.4,
    h: 0.2,
    fontFace: "Avenir Next",
    fontSize: 11,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText("Sales Global Summary", {
    x: 0.64,
    y: 1.16,
    w: 3.2,
    h: 0.42,
    fontFace: "Avenir Next",
    fontSize: 25,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText("Monthly CRO operating review", {
    x: 0.64,
    y: 1.92,
    w: 3.2,
    h: 0.28,
    fontFace: "Avenir Next",
    fontSize: 15,
    color: "D7EFF1",
    margin: 0,
  });
  const execSlide = payloadForSlide(fillPayload, "global-executive-summary");
  const slots = (execSlide && execSlide.slots) || {};
  slide.addText("Validated monthly leadership view", {
    x: 0.64,
    y: 2.48,
    w: 2.9,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 9.4,
    bold: true,
    color: "D7EFF1",
    margin: 0,
  });
  slide.addText("Three regional pages plus one approval control slide, all sourced from validated regional rollups.", {
    x: 0.64,
    y: 2.7,
    w: 3.08,
    h: 0.6,
    fontFace: "Avenir Next",
    fontSize: 10,
    color: "D7EFF1",
    margin: 0,
  });
  addKpiCard(slide, 4.62, 0.98, 1.82, 1.42, "Q2 active ARR", slots.global_pipeline_arr_q2, "Cross-region active pipeline.", COLORS.teal);
  addKpiCard(slide, 6.58, 0.98, 1.82, 1.42, "Open renewal ACV", slots.global_renewal_acv_q2, "Renewals stay ACV.", COLORS.gold);
  addKpiCard(slide, 8.54, 0.98, 1.82, 1.42, "Missing approvals", slots.global_missing_approval_count, "Global stage 3+ backlog count.", COLORS.rust);
  addTextPanel(slide, 10.5, 0.98, 1.72, 1.42, "Top action", [normalizeText(slots.global_top_action)], COLORS.plum, COLORS.plumSoft);
  addTextPanel(
    slide,
    4.62,
    2.68,
    7.6,
    2.4,
    "Operating read",
    [
      normalizeText(slots.global_top_action, "State the global operating posture in one sentence."),
      normalizeText(slots.global_top_risk, "Keep the primary cross-region risk visible on the cover."),
    ],
    COLORS.teal,
    COLORS.blueSoft
  );
  addTextPanel(slide, 4.62, 5.34, 3.54, 1.04, "Primary risk", [normalizeText(slots.global_top_risk, "Global risk statement stays derived from the validated regional rollups.")], COLORS.rust, COLORS.rustSoft);
  addTextPanel(slide, 8.34, 5.34, 3.88, 1.04, "Data discipline", [
    "Validated regional rollups only.",
    "ARR, ACV, and approval backlog stay separate.",
  ], COLORS.teal, COLORS.white);
  slide.addText(`Snapshot ${snapshotDate}`, {
    x: 0.64,
    y: 6.48,
    w: 2.2,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 10,
    color: COLORS.muted,
    margin: 0,
  });
  addFooter(slide, 1, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function addAgenda(pptx, contract, snapshotDate, fillPayload) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  const execSlide = payloadForSlide(fillPayload, "global-executive-summary");
  const slots = (execSlide && execSlide.slots) || {};
  addTopRule(slide, COLORS.teal);
  slide.addText("Review sequence", {
    x: 0.58,
    y: 0.88,
    w: 8.7,
    h: 0.34,
    fontFace: "Avenir Next",
    fontSize: 22,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText("Monthly sequence for CRO and regional review", {
    x: 0.58,
    y: 1.27,
    w: 6.2,
    h: 0.22,
    fontFace: "Avenir Next",
    fontSize: 10,
    color: COLORS.muted,
    margin: 0,
  });
  slide.addText("SIMCORP", {
    x: 11.0,
    y: 0.34,
    w: 1.1,
    h: 0.18,
    fontFace: "Avenir Next",
    fontSize: 10,
    bold: true,
    color: COLORS.teal,
    align: "right",
    margin: 0,
  });
  const titles = contract.slides.map((slideDef) => {
    switch (slideDef.id) {
      case "global-executive-summary":
        return "Global Executive Summary";
      case "apac-region-summary":
        return "APAC";
      case "emea-region-summary":
        return "EMEA";
      case "north-america-region-summary":
        return "North America";
      case "global-commercial-approval-overview":
        return "Commercial Approval Overview";
      case "global-appendix":
        return "Appendix and Guardrails";
      default:
        return slideDef.title;
    }
  });
  titles.forEach((title, index) => {
    const col = index % 2;
    const row = Math.floor(index / 2);
    const x = 0.72 + col * 6.0;
    const y = 1.76 + row * 0.86;
    addPanel(slide, x, y, 5.52, 0.68, index < 3 ? COLORS.blueSoft : COLORS.white);
    slide.addText(String(index + 1).padStart(2, "0"), {
      x: x + 0.18,
      y: y + 0.16,
      w: 0.32,
      h: 0.18,
      fontFace: "Avenir Next",
      fontSize: 9.5,
      bold: true,
      color: COLORS.teal,
      margin: 0,
    });
    slide.addText(title, {
      x: x + 0.62,
      y: y + 0.14,
      w: 4.7,
      h: 0.28,
      fontFace: "Avenir Next",
      fontSize: 11.2,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
  });
  addTextPanel(
    slide,
    0.72,
    4.58,
    5.52,
    1.18,
    "Current posture",
    [
      `Q2 active ARR: ${normalizeText(slots.global_pipeline_arr_q2, "€x.xM")}`,
      `Open renewal ACV: ${normalizeText(slots.global_renewal_acv_q2, "€x.xM")}`,
      "Approval backlog stays visible by region.",
    ],
    COLORS.teal,
    COLORS.white
  );
  addTextPanel(
    slide,
    6.72,
    4.58,
    5.52,
    1.18,
    "Read this deck",
    [
      "Regional slides are rollups, not substitutes for MD-1 reviews.",
      "Pipeline remains ARR, renewals remain ACV, and risk statements stay derived.",
    ],
    COLORS.gold,
    COLORS.goldSoft
  );
  addFooter(slide, 2, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function addExecutiveSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  const slots = (slidePayload && slidePayload.slots) || {};
  const header = addSlideHeader(
    slide,
    slideTitle(slideDef, slidePayload),
    slideSubtitle(slideDef, slidePayload),
    "What is the global position this month, and what is the single action leadership should drive first?",
    COLORS.teal,
    { showQuestionBox: !slidePayload }
  );
  const y = header.contentY;
  addKpiCard(slide, 0.72, y, 2.82, 1.52, "Q2 active pipeline", slots.global_pipeline_arr_q2, "Deterministic rollup from regional snapshots.", COLORS.teal);
  addKpiCard(slide, 3.72, y, 2.82, 1.52, "Open renewal ACV", slots.global_renewal_acv_q2, "Global open renewal exposure.", COLORS.gold);
  addKpiCard(slide, 6.72, y, 2.82, 1.52, "Missing approvals", slots.global_missing_approval_count, "Stage 3+ candidate count across regions.", COLORS.rust);
  addTextPanel(slide, 9.72, y, 2.52, 1.52, "Top action", [normalizeText(slots.global_top_action)], COLORS.plum, COLORS.plumSoft);
  addTextPanel(slide, 0.72, y + 1.82, 5.72, 1.36, "Cross-region risk read", [normalizeText(slots.global_top_risk)], COLORS.rust, COLORS.rustSoft);
  addTextPanel(
    slide,
    6.62,
    y + 1.82,
    5.62,
    1.36,
    "Decision use",
    [
      "Use the region slides to decide where to intervene this month.",
      "Use the approval slide to focus cross-region deal unblockers.",
    ],
    COLORS.teal,
    COLORS.blueSoft
  );
  addFooter(slide, pageNum, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function addRegionSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate, accent) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  const slots = (slidePayload && slidePayload.slots) || {};
  const header = addSlideHeader(
    slide,
    slideTitle(slideDef, slidePayload),
    slideSubtitle(slideDef, slidePayload),
    `What does ${normalizeText(slots.region_name)} need from leadership this month?`,
    accent,
    { showQuestionBox: !slidePayload }
  );
  const y = header.contentY;
  addKpiCard(slide, 0.72, y, 2.82, 1.52, `${regionQuarterLabel(slots)} active ARR`, slots.headline_pipeline_arr_q2, `${regionQuarterTitle(slots)} active pipeline.`, accent);
  addKpiCard(slide, 3.72, y, 2.82, 1.52, "Commit / Best case", `${normalizeText(slots.q2_commit_arr)} / ${normalizeText(slots.q2_best_case_arr)}`, "Forecast categories kept separate.", COLORS.gold);
  addKpiCard(slide, 6.72, y, 2.82, 1.52, "Omitted / Approval rate", `${normalizeText(slots.q2_omitted_arr)} / ${normalizeText(slots.approval_rate_stage3_plus)}`, "Keep omitted visible and outside active pipeline.", COLORS.rust);
  addKpiCard(slide, 9.72, y, 2.52, 1.52, "Open renewal ACV", slots.renewal_open_acv, "Renewals stay ACV.", COLORS.green);
  addTextPanel(slide, 0.72, y + 1.82, 5.72, 1.36, "Top risk", [normalizeText(slots.top_risk)], COLORS.rust, COLORS.rustSoft);
  addTextPanel(slide, 6.62, y + 1.82, 5.62, 1.36, "Required action", [normalizeText(slots.top_action)], accent, COLORS.blueSoft);
  if (slots.quarterly_pipeline_footnote) {
    slide.addText(normalizeText(slots.quarterly_pipeline_footnote), {
      x: 0.72,
      y: 5.92,
      w: 11.5,
      h: 0.28,
      fontFace: "Avenir Next",
      fontSize: 8.6,
      color: COLORS.muted,
      margin: 0,
    });
  }
  addFooter(slide, pageNum, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function addApprovalOverviewSlide(pptx, slideDef, slidePayload, pageNum, snapshotDate) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  const slots = (slidePayload && slidePayload.slots) || {};
  const approvedRows = ensureArray(slots.approved_2026_by_region).map((row) => [
    normalizeText(row.region_name),
    normalizeText(row.deal_count),
    normalizeText(row.arr_eur),
  ]);
  const missingRows = ensureArray(slots.missing_approval_by_region).map((row) => [
    normalizeText(row.region_name),
    normalizeText(row.candidate_count),
    normalizeText(row.arr_eur),
  ]);
  const largestRows = ensureArray(slots.largest_global_missing_candidates).map((row) => [
    normalizeText(row.region_name),
    normalizeText(row.opportunity),
    normalizeText(row.owner),
    normalizeText(row.arr_eur),
  ]);
  const header = addSlideHeader(
    slide,
    slideTitle(slideDef, slidePayload),
    slideSubtitle(slideDef, slidePayload),
    "Which approval candidates materially need cross-region leadership escalation now?",
    COLORS.rust,
    { showQuestionBox: !slidePayload }
  );
  const y = header.contentY;
  addSimpleTable(slide, 0.72, y, 3.6, ["Region", "Deals", "Approved ARR"], approvedRows, COLORS.teal, 3);
  addSimpleTable(slide, 4.5, y, 3.6, ["Region", "Backlog", "Missing ARR"], missingRows, COLORS.rust, 3);
  addTextPanel(
    slide,
    8.28,
    y,
    3.96,
    1.52,
    "Operating read",
    [
      "Use approved exposure to show what has already cleared.",
      "Use missing exposure and the watchlist below to drive escalation.",
    ],
    COLORS.gold,
    COLORS.goldSoft
  );
  addSimpleTable(slide, 0.72, y + 1.9, 11.52, ["Region", "Opportunity", "Owner", "ARR"], largestRows, COLORS.plum, 5);
  addFooter(slide, pageNum, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function addAppendixSlide(pptx, slideDef, slidePayload, pageNum, snapshotDate) {
  const slide = pptx.addSlide();
  slide.background = { color: COLORS.paper };
  const slots = (slidePayload && slidePayload.slots) || {};
  const header = addSlideHeader(
    slide,
    slideDef.title,
    slideDef.subtitle,
    "What caveats and metric rules must stay visible when this deck is reused or excerpted?",
    COLORS.gold,
    { showQuestionBox: !slidePayload }
  );
  const y = header.contentY;
  addTextPanel(slide, 0.72, y, 3.72, 2.86, "Metric rules", ensureArray(slots.metric_definition_notes), COLORS.teal, COLORS.blueSoft);
  addTextPanel(slide, 4.66, y, 3.72, 2.86, "Rollup notes", ensureArray(slots.region_rollup_notes), COLORS.plum, COLORS.plumSoft);
  addTextPanel(slide, 8.6, y, 3.64, 2.86, "Known gaps", ensureArray(slots.known_gaps), COLORS.rust, COLORS.rustSoft);
  addTextPanel(slide, 0.72, y + 3.12, 11.52, 0.92, "Meeting use", [
    "Use this page to settle metric disputes quickly and keep unsupported claims out of the global discussion.",
  ], COLORS.teal, COLORS.tealSoft);
  addFooter(slide, pageNum, snapshotDate);
  if (SHOULD_VALIDATE_LAYOUT) {
    warnIfSlideHasOverlaps(slide, pptx);
    warnIfSlideElementsOutOfBounds(slide, pptx);
  }
}

function renderSlideById(pptx, slideDef, slidePayload, pageNum, snapshotDate) {
  switch (slideDef.id) {
    case "global-executive-summary":
      addExecutiveSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    case "apac-region-summary":
      addRegionSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate, COLORS.teal);
      break;
    case "emea-region-summary":
      addRegionSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate, COLORS.plum);
      break;
    case "north-america-region-summary":
      addRegionSummarySlide(pptx, slideDef, slidePayload, pageNum, snapshotDate, COLORS.green);
      break;
    case "global-commercial-approval-overview":
      addApprovalOverviewSlide(pptx, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    case "global-appendix":
      addAppendixSlide(pptx, slideDef, slidePayload, pageNum, snapshotDate);
      break;
    default:
      throw new Error(`Unsupported global slide id: ${slideDef.id}`);
  }
}

async function buildDeck({ contractPath, outputPath, snapshotDate, fillPayloadPath }) {
  const contract = loadJson(contractPath);
  const fillPayload = fillPayloadPath && fs.existsSync(fillPayloadPath) ? loadJson(fillPayloadPath) : null;
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";
  pptx.author = "OpenAI Codex";
  pptx.company = "SimCorp";
  pptx.subject = "Sales Global Summary";
  pptx.title = "Sales Global Summary Validated Baseline";
  pptx.lang = "en-US";
  pptx.theme = {
    headFontFace: "Avenir Next",
    bodyFontFace: "Avenir Next",
    lang: "en-US",
  };
  pptx.background = { color: COLORS.paper };

  addCover(pptx, snapshotDate, fillPayload);
  addAgenda(pptx, contract, snapshotDate, fillPayload);
  contract.slides.forEach((slideDef, index) => {
    renderSlideById(pptx, slideDef, payloadForSlide(fillPayload, slideDef.id), index + 3, snapshotDate);
  });

  await pptx.writeFile({ fileName: outputPath });
  return {
    slideCount: contract.slides.length + 2,
    fillPayloadPath: fillPayloadPath || null,
    outputPath,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const contractPath = args.contract || DEFAULT_CONTRACT;
  const outputPath = args.output;
  const snapshotDate = args["snapshot-date"];
  if (!outputPath || !snapshotDate) {
    throw new Error("Usage: node build_sales_global_summary_shell_v1.js --snapshot-date YYYY-MM-DD --output FILE [--contract FILE] [--fill-payload FILE]");
  }
  const result = await buildDeck({
    contractPath,
    outputPath,
    snapshotDate,
    fillPayloadPath: args["fill-payload"] || null,
  });
  process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.stack || error.message}\n`);
  process.exit(1);
});
