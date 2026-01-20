import { chromium } from "playwright";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import crypto from "crypto";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const INVENTORY_URL =
  process.env.INVENTORY_URL ||
  "https://www.bankstreethyundai.com/ottawa-gloucester-hyundai-dealer/used-cars/used-vehicle-inventory?type=U";

const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const S3_BUCKET = process.env.S3_BUCKET; // REQUIRED

const DEALER_GROUP = process.env.DEALER_GROUP || "Dilawri";
const DEALERSHIP = process.env.DEALERSHIP || "Bank Street Hyundai";
const DEALER_ID = process.env.DEALER_ID || "dilawri_bank_street_hyundai";
const SCRAPER_ID = process.env.SCRAPER_ID || "bankstreethyundai_inventory_v1";
const SCHEMA_VERSION = 1;

// Batch run identifier (shared across multiple dealer scrapes in a single refresh)
// Passed in by the batch orchestrator. Optional but recommended for staging pipelines.
const BATCH_ID = process.env.BATCH_ID || null;

// CloudFront (recommended): you set Origin path = /images, so CloudFront URLs must NOT include "images/" prefix.
const AWS_PUBLIC_BASE_URL = (process.env.AWS_PUBLIC_BASE_URL || "").replace(/\/+$/, "");
const CLOUDFRONT_ORIGIN_PATH_PREFIX = (
  process.env.CLOUDFRONT_ORIGIN_PATH_PREFIX || "images/"
).replace(/^\/+/, "");

const MAX_IMAGES_PER_VEHICLE = Number(process.env.MAX_IMAGES_PER_VEHICLE || 15);
const IMAGE_FETCH_CONCURRENCY = Number(process.env.IMAGE_FETCH_CONCURRENCY || 3);
const IMAGE_DOWNLOAD_CONCURRENCY = Number(process.env.IMAGE_DOWNLOAD_CONCURRENCY || 6);
// For scrapers that must visit VDPs to extract full details (e.g., Applewood)
const VDP_CONCURRENCY = Number(process.env.VDP_CONCURRENCY || 3);
const DEBUG_IMAGES = String(process.env.DEBUG_IMAGES || "").trim() === "1";

const DESKTOP_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36";

if (!S3_BUCKET) {
  console.error(
    JSON.stringify({
      ok: false,
      error: "Missing required env var S3_BUCKET",
    })
  );
  process.exit(1);
}

const s3 = new S3Client({ region: AWS_REGION });

function normalizeToH600(url) {
  return String(url).replace(/INV_H\d+/i, "INV_H600");
}

function toAbs(url, base) {
  return new URL(url, base).toString();
}

function safeSlug(s) {
  return (
    String(s || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 80) || "unknown"
  );
}

function parseNumberFromText(s) {
  if (!s) return null;
  const digits = String(s).replace(/[^\d]/g, "");
  if (!digits) return null;
  const n = Number(digits);
  return Number.isFinite(n) ? n : null;
}

function stripRegisteredSymbol(s) {
  // Applewood uses the registered trademark symbol in titles (e.g., Ford®)
  return String(s || "").replace(/\u00AE/g, "").replace(/®/g, "").trim();
}

function cleanWhitespace(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function normalizeVin(vin) {
  if (!vin) return null;
  const v = String(vin).toUpperCase().replace(/\s+/g, "").trim();
  return v || null;
}

function guessExtFromContentType(ct) {
  const t = String(ct || "").toLowerCase();
  if (t.includes("image/jpeg") || t.includes("image/jpg")) return "jpg";
  if (t.includes("image/png")) return "png";
  if (t.includes("image/webp")) return "webp";
  if (t.includes("image/gif")) return "gif";
  return "jpg";
}

function s3KeyToAwsUrl(key) {
  if (!key) return null;

  // If CloudFront is configured with origin path "/images",
  // the CloudFront viewer URL should NOT contain the "images/" prefix.
  if (AWS_PUBLIC_BASE_URL) {
    const normalizedKey = String(key);
    const prefix = String(CLOUDFRONT_ORIGIN_PATH_PREFIX || "images/");
    const withoutPrefix = normalizedKey.startsWith(prefix)
      ? normalizedKey.slice(prefix.length)
      : normalizedKey;

    // Encode each path segment safely (spaces, etc.)
    const encoded = withoutPrefix
      .split("/")
      .map((seg) => encodeURIComponent(seg))
      .join("/");

    return `${AWS_PUBLIC_BASE_URL}/${encoded}`;
  }

  // Fallback: durable but not browser-friendly (still useful for debugging)
  return `s3://${S3_BUCKET}/${key}`;
}

async function putToS3({ key, body, contentType }) {
  await s3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: body,
      ContentType: contentType || "application/octet-stream",
    })
  );
  return key;
}

/**
 * Fetch vehicle image URLs from modal, in SAME context.
 * Returns relative URLs (as seen in DOM), normalized to H600.
 */
async function fetchVehicleImages(context, baseUrl, vid) {
  if (!vid) return [];

  const modalUrl = new URL(
    `/NewCars/Modal/Vehicle-Images.aspx?vid=${encodeURIComponent(vid)}`,
    baseUrl
  ).toString();

  const p = await context.newPage();

  try {
    const resp = await p.goto(modalUrl, { waitUntil: "domcontentloaded", timeout: 60000 });

    if (DEBUG_IMAGES) {
      console.error(
        JSON.stringify({
          debug: "modal_status",
          vid,
          modalUrl,
          status: resp?.status?.() ?? null,
          finalUrl: p.url(),
          title: await p.title().catch(() => null),
        })
      );
    }

    await p.waitForSelector("img, [style*='vimgs']", { timeout: 8000 }).catch(() => {});
    await p.waitForTimeout(700);

    let raw = await p.$$eval("img", (imgs) => {
      const out = [];
      for (const img of imgs) {
        const src = img.getAttribute("src");
        const dataSrc = img.getAttribute("data-src");
        if (src) out.push(src);
        if (dataSrc) out.push(dataSrc);
      }
      return out;
    });

    // Fallback: background-image URLs
    if (!raw.length) {
      const styleUrls = await p.$$eval("[style]", (els) => {
        const out = [];
        const re = /url\((['"]?)(.*?)\1\)/gi;
        for (const el of els) {
          const s = el.getAttribute("style") || "";
          let m;
          while ((m = re.exec(s))) out.push(m[2]);
        }
        return out;
      });
      raw = styleUrls;
    }

    const filtered = raw
      .filter(Boolean)
      .map((s) => String(s).trim())
      .filter((s) => s.includes("/vimgs/"));

    const normalized = filtered.map(normalizeToH600);
    const deduped = [...new Set(normalized)].slice(0, MAX_IMAGES_PER_VEHICLE);

    if (DEBUG_IMAGES) {
      console.error(
        JSON.stringify({
          debug: "images",
          vid,
          modalUrl,
          foundRaw: raw.length,
          foundFiltered: filtered.length,
          returned: deduped.length,
          sample: deduped.slice(0, 3),
        })
      );
    }

    return deduped;
  } catch (e) {
    if (DEBUG_IMAGES) {
      console.error(
        JSON.stringify({
          debug: "images_error",
          vid,
          modalUrl,
          error: String(e?.message || e),
        })
      );
    }
    return [];
  } finally {
    await p.close().catch(() => {});
  }
}

/**
 * Applewood (applewoodperformancecenter.com)
 * - SRP is infinite scroll (URL does not change)
 * - Must visit each VDP to extract details
 * - Images are present on VDP page (gallery)
 *
 * IMPORTANT FIX:
 * - Do NOT rely on '/inventory/Used-' prefix; capture all VDPs under /inventory/<something>
 * - Scroll the actual scroll container if present, not just window
 */
async function scrapeApplewoodVdpUrls(page, inventoryUrl) {
  await page.goto(inventoryUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(1500);

  // Extract VDPs:
  // - keep /inventory/<slug> links
  // - exclude /inventory (root)
  // - exclude /inventory?filters=... (SRP itself)
  // - prefer links inside VehicleCard containers (hashed classname contains 'VehicleCard')
  async function extract() {
    const urls = await page.evaluate(() => {
      const abs = (href) => new URL(href, location.href).toString();

      const isVdpHref = (href) => {
        if (!href) return false;
        if (!href.startsWith("/inventory/")) return false;
        if (href === "/inventory") return false;
        if (href.startsWith("/inventory?")) return false; // SRP
        if (href.includes("filters=")) return false;
        if (href.includes("#")) return true; // VDP + hash is still VDP
        // Must have path segment after /inventory/
        const rest = href.slice("/inventory/".length);
        return rest.length > 2;
      };

      const all = Array.from(document.querySelectorAll('a[href^="/inventory/"]'))
        .map((a) => a.getAttribute("href"))
        .filter(isVdpHref);

      // Prefer links inside vehicle cards if present
      const inCards = Array.from(document.querySelectorAll('[class*="VehicleCard"] a[href^="/inventory/"]'))
        .map((a) => a.getAttribute("href"))
        .filter(isVdpHref);

      const chosen = inCards.length ? inCards : all;
      const out = [];
      const seen = new Set();

      for (const href of chosen) {
        const u = abs(href);
        // Normalize away PhotoSwipe hash to keep VDP stable
        const clean = u.split("#")[0];
        if (seen.has(clean)) continue;
        seen.add(clean);
        out.push(clean);
      }

      return out;
    });

    return urls;
  }

  // Scroll:
  // - try scrolling the biggest scrollable container (overflow-y: auto/scroll)
  // - fallback to window scroll + PageDown
  async function scrollOnce() {
    await page.evaluate(() => {
      const candidates = Array.from(document.querySelectorAll("div, main, section"))
        .filter((el) => {
          const st = window.getComputedStyle(el);
          const oy = st.overflowY;
          const scrollable = (oy === "auto" || oy === "scroll") && el.scrollHeight > el.clientHeight + 50;
          return scrollable;
        })
        .sort((a, b) => b.scrollHeight - a.scrollHeight);

      if (candidates.length) {
        const el = candidates[0];
        el.scrollTop = el.scrollHeight;
      } else {
        window.scrollTo(0, document.body.scrollHeight);
      }
    });

    await page.keyboard.press("PageDown").catch(() => {});
  }

  const seen = new Set();
  let noNewStreak = 0;

  // Prime
  const initial = await extract();
  for (const u of initial) seen.add(u);

  // Loop until stable (5 consecutive no-growth loops)
  for (let i = 0; i < 220 && noNewStreak < 5; i++) {
    await scrollOnce();
    await page.waitForTimeout(1800);

    const now = await extract();
    const before = seen.size;
    for (const u of now) seen.add(u);
    const after = seen.size;

    if (after === before) noNewStreak += 1;
    else noNewStreak = 0;
  }

  const out = Array.from(seen);
  console.log("APPLEWOOD_VDPS_FOUND:", out.length);
  return out;
}

function parseYearMakeModelFromTitle(titleText) {
  // Expected: "2016 Ford Focus" (Ford may include ®)
  const cleaned = cleanWhitespace(stripRegisteredSymbol(titleText));
  const m = cleaned.match(/^(\d{4})\s+(.+)$/);
  if (!m) return { year: null, make: null, model: null, raw: cleaned };
  const year = Number(m[1]);
  const rest = m[2].trim();
  const parts = rest.split(" ").filter(Boolean);
  const make = parts[0] || null;
  const model = parts.slice(1).join(" ") || null;
  return {
    year: Number.isFinite(year) ? year : null,
    make,
    model,
    raw: cleaned,
  };
}

async function scrapeApplewoodVdp(context, vdpUrl) {
  const p = await context.newPage();
  try {
    await p.goto(vdpUrl, { waitUntil: "domcontentloaded", timeout: 60000 });

    // Wait for VDP hydration (these fields appear after initial DOMContentLoaded on many VDPs)
    await p
      .waitForFunction(() => {
        const t = document.body?.innerText || "";
        return (
          t.includes("Vehicle Specifications") ||
          t.includes("VIN") ||
          t.includes("Stock #") ||
          t.includes("Purchase Price") ||
          t.includes("Retail Price")
        );
      }, { timeout: 25000 })
      .catch(() => {});

    // Title (Year/Make/Model)
    const titleText = await p
      .locator("h1")
      .first()
      .innerText()
      .catch(() => "");
    const ymm = parseYearMakeModelFromTitle(titleText);

    // Trim: bodyLarge paragraph under title (module classnames are hashed, so match stable fragments)
    const trimText = await p
      .locator('p[class*="Text-module"][class*="bodyLarge"] span')
      .first()
      .innerText()
      .catch(async () => {
        // Fallback: first non-empty paragraph
        return await p
          .locator("p")
          .filter({ hasText: /\S/ })
          .first()
          .innerText()
          .catch(() => "");
      });

    // Prefer explicit selectors where possible, but fall back to label-based parsing.
        const extractVdpData = async () => {
      return await p.evaluate(() => {
        const raw = String(document.body?.innerText || "");
        const collapsed = raw.replace(/\s+/g, " ").trim();

        // Banner can be bullet-separated or just spaced, so be flexible:
        // "Kilometres: 78,385 • VIN: ... • Stock #: ..."
        const banner =
          collapsed.match(
            /Kilometres\s*[:#]?\s*([\d,]+)\s*(?:[•\-–]|·|\||\s)\s*VIN\s*[:#]?\s*([A-HJ-NPR-Z0-9]{11,17})\s*(?:[•\-–]|·|\||\s)\s*Stock\s*#\s*[:#]?\s*([A-Z0-9\-]+)/i
          ) || null;

        const mileage =
          (banner && banner[1]) ||
          (collapsed.match(/Kilometres\s*[:#]?\s*([\d,]+)/i)?.[1] ?? null) ||
          (collapsed.match(/\bMileage\s*[:#]?\s*([\d,]+)/i)?.[1] ?? null);

        const vin =
          (banner && banner[2]) ||
          (collapsed.match(/\bVIN\s*[:#]?\s*([A-HJ-NPR-Z0-9]{11,17})\b/i)?.[1] ?? null);

        const stock =
          (banner && banner[3]) ||
          (collapsed.match(/\bStock\s*#\s*[:#]?\s*([A-Z0-9\-]+)\b/i)?.[1] ?? null);

        // Price: prefer "Purchase Price" then "Retail Price"
        const purchasePrice = collapsed.match(/Purchase Price\s*[:#]?\s*\$?\s*([\d,]+)/i)?.[1] ?? null;
        const retailPrice = collapsed.match(/Retail Price\s*[:#]?\s*\$?\s*([\d,]+)/i)?.[1] ?? null;
        const price = purchasePrice || retailPrice || (collapsed.match(/\$\s*([\d,]{4,})/)?.[1] ?? null);

        // Line-based extraction (prevents capturing the whole rest of the page)
        const txLine = raw.match(/(?:^|\n)\s*Transmission\s+([^\n]+)/i)?.[1] ?? null;
        const exLine = raw.match(/(?:^|\n)\s*Exterior Color\s+([^\n]+)/i)?.[1] ?? null;

        const clean = (s) => String(s || "").replace(/\s+/g, " ").trim();

        return {
          mileage: mileage ? clean(mileage) : null,
          vin: vin ? clean(vin) : null,
          stock: stock ? clean(stock) : null,
          price: price ? clean(price) : null,
          transmission: txLine ? clean(txLine) : null,
          exteriorColor: exLine ? clean(exLine) : null,
        };
      });
    };

    let vdpData = await extractVdpData();

    // Optional (but recommended): if a VDP comes back “empty”, retry once
    const missingCritical = !vdpData.vin && !vdpData.stock && !vdpData.mileage && !vdpData.price;
    if (missingCritical) {
      console.warn("APPLEWOOD_VDP_EMPTY_RETRY:", { vdpUrl });
      await p.reload({ waitUntil: "domcontentloaded", timeout: 60000 }).catch(() => {});
      await p.waitForTimeout(1200);
      // Re-wait for hydration but don't hard-fail the VDP
      await p
        .waitForFunction(() => {
          const t = document.body?.innerText || "";
          return (
            t.includes("Vehicle Specifications") ||
            t.includes("VIN") ||
            t.includes("Stock #") ||
            t.includes("Purchase Price") ||
            t.includes("Retail Price")
          );
        }, { timeout: 25000 })
        .catch(() => {});
      vdpData = await extractVdpData();
    }

    // Best-effort trim (your provided selector is reliable, but keep fallback)
    const trim = cleanWhitespace(stripRegisteredSymbol(trimText)) || null;

    // Images (Applewood):
    // - The VDP shows the main image in-page (alt="image-0")
    // - Clicking opens a PhotoSwipe gallery with next/prev arrows and alt="image-N"
    // We collect from the page first, then (best-effort) page through the PhotoSwipe gallery
    // to capture the complete set.

    // First pass: in-page gallery images
    let imageSourceUrls = await p.$$eval('img[alt^="image-"]', (imgs) => {
      const out = [];
      for (const img of imgs) {
        const src = img.getAttribute("src") || img.getAttribute("data-src") || "";
        if (!src) continue;
        if (src.startsWith("data:")) continue;
        out.push(new URL(src, location.href).toString());
      }
      return out;
    });

    // Second pass: PhotoSwipe (click image-0, then click next until images stop changing)
    try {
      const mainImg = p.locator('img[alt="image-0"]').first();
      if ((await mainImg.count()) > 0) {
        await mainImg.click({ timeout: 5000 });
        // wait for the PhotoSwipe UI to appear
        await p
          .waitForSelector(
            '.pswp__content img[alt^="image-"], button.pswp__button--arrow--next',
            { timeout: 8000 }
          )
          .catch(() => {});
        await p.waitForTimeout(250);
      }

      const seen = new Set(imageSourceUrls);
      const popupImg = () => p.locator('.pswp__content img[alt^="image-"]').first();
      const nextBtn = () => p.locator("button.pswp__button--arrow--next");

      let lastSrc = null;
      for (let i = 0; i < Math.max(10, MAX_IMAGES_PER_VEHICLE * 3); i++) {
        const src = await popupImg().getAttribute("src").catch(() => null);
        if (src) {
          const abs = toAbs(src, vdpUrl);
          seen.add(abs);
        }

        // If src is unchanged after a next click, we are likely at the end
        const nbCount = await nextBtn().count().catch(() => 0);
        if (!nbCount) break;

        const before = src || lastSrc;
        await nextBtn().click().catch(() => {});
        await p.waitForTimeout(350);

        const after = await popupImg().getAttribute("src").catch(() => null);
        if (!after || (before && after === before)) {
          // one extra short wait for slow transitions
          await p.waitForTimeout(350);
          const after2 = await popupImg().getAttribute("src").catch(() => null);
          if (!after2 || (before && after2 === before)) break;
          lastSrc = after2;
        } else {
          lastSrc = after;
        }
      }

      // Close gallery
      await p.keyboard.press("Escape").catch(() => {});

      imageSourceUrls = [...seen];
    } catch (_) {
      // Ignore PhotoSwipe errors; we will fall back to in-page images
    }

    const dedupedImages = [...new Set(imageSourceUrls)].slice(0, MAX_IMAGES_PER_VEHICLE);

    return {
      url: vdpUrl,
      year: ymm.year,
      make: ymm.make,
      model: ymm.model,
      trim,
      vin: vdpData.vin,
      stockNumber: vdpData.stock,
      mileage: vdpData.mileage,
      price: vdpData.price,
      transmission: vdpData.transmission,
      exteriorColor: vdpData.exteriorColor,
      imageSourceUrls: dedupedImages,
    };
  } finally {
    await p.close().catch(() => {});
  }
}


async function withTimeout(promise, ms, label = "timeout") {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`${label} after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(t);
  }
}

/**
 * Concurrency runner
 */
async function mapWithConcurrency(items, concurrency, fn) {
  const results = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (true) {
      const i = idx++;
      if (i >= items.length) break;
      results[i] = await fn(items[i], i);
    }
  }

  const workers = Array.from({ length: Math.max(1, concurrency) }, () => worker());
  await Promise.all(workers);
  return results;
}

/**
 * Download one image URL and upload to S3 (buffered).
 * Returns { s3Key, contentType, bytes } or null if failed.
 */
async function downloadAndUploadImage({ imageUrlAbs, s3KeyBase }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 45000);

  try {
    const res = await fetch(imageUrlAbs, {
      signal: controller.signal,
      headers: {
        "User-Agent": DESKTOP_UA,
        Accept: "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
      },
    });

    if (!res.ok) {
      if (DEBUG_IMAGES) {
        console.error(
          JSON.stringify({
            debug: "img_fetch_bad_status",
            url: imageUrlAbs,
            status: res.status,
            statusText: res.statusText,
          })
        );
      }
      return null;
    }

    const contentType = res.headers.get("content-type") || "application/octet-stream";

    const ab = await res.arrayBuffer();
    const buf = Buffer.from(ab);

    if (!buf.length) {
      if (DEBUG_IMAGES) {
        console.error(JSON.stringify({ debug: "img_empty_body", url: imageUrlAbs }));
      }
      return null;
    }

    // Use correct extension based on Content-Type
    const ext = guessExtFromContentType(contentType);
    const key = s3KeyBase.replace(/\.jpg$/i, `.${ext}`);

    await putToS3({ key, body: buf, contentType });

    if (DEBUG_IMAGES) {
      console.error(
        JSON.stringify({
          debug: "img_uploaded",
          url: imageUrlAbs,
          s3Key: key,
          bytes: buf.length,
          contentType,
        })
      );
    }

    return { s3Key: key, contentType, bytes: buf.length };
  } catch (e) {
    if (DEBUG_IMAGES) {
      console.error(
        JSON.stringify({
          debug: "img_upload_error",
          url: imageUrlAbs,
          s3KeyBase,
          error: String(e?.message || e),
        })
      );
    }
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

(async () => {
  const startedAt = new Date().toISOString();
  const runId = crypto.randomUUID();

  const IS_APPLEWOOD =
    /applewoodperformancecenter\.com/i.test(INVENTORY_URL) ||
    /applewoodperformancecenter/i.test(SCRAPER_ID) ||
    /applewood/i.test(DEALER_ID);

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ userAgent: DESKTOP_UA });
  const page = await context.newPage();

  // S3 run file key
  const now = new Date();
  const yyyy = String(now.getUTCFullYear());
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const ts = now.toISOString().replace(/[:.]/g, "-");
  const runS3Key = `runs/${DEALER_ID}/${yyyy}/${mm}/${dd}/${ts}_${runId}.json`;

  try {
    let vehicles = [];

    if (IS_APPLEWOOD) {
      // Applewood: SRP -> VDP URLs -> scrape each VDP for details + images
      const vdpUrls = await scrapeApplewoodVdpUrls(page, INVENTORY_URL);
      const VDP_TIMEOUT_MS = Number(process.env.VDP_TIMEOUT_MS || 45000);

      let done = 0;
      vehicles = await mapWithConcurrency(vdpUrls, VDP_CONCURRENCY, async (url, i) => {
        try {
          const v = await withTimeout(
            scrapeApplewoodVdp(context, url),
            VDP_TIMEOUT_MS,
            `VDP scrape ${url}`
          );
          return v;
        } catch (e) {
          // Return a minimal record so the run can complete and we can debug which VDP failed
          return {
            url,
            year: null,
            make: null,
            model: null,
            trim: null,
            vin: null,
            stockNumber: null,
            mileage: null,
            price: null,
            transmission: null,
            exteriorColor: null,
            imageSourceUrls: [],
            _warnings: [`vdp_failed:${String(e?.message || e)}`],
          };
        } finally {
          done += 1;
          if (done % 10 === 0) {
            console.log("APPLEWOOD_VDP_PROGRESS:", { done, total: vdpUrls.length });
          }
        }
      });
} else {
      // Bank Street Hyundai (legacy scraper)
      await page.goto(INVENTORY_URL, { waitUntil: "domcontentloaded", timeout: 60000 });

      // Wait for vehicle anchors
      for (let i = 0; i < 30; i++) {
        const found = await page.locator("div.bold.fsize16.vnamelink a.styleColor").count();
        if (found > 0) break;
        await sleep(1000);
      }

      // Infinite scroll until stable
      let lastCount = 0;
      let stableRounds = 0;
      for (let i = 0; i < 120; i++) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await sleep(1200);

        const count = await page.locator("div.bold.fsize16.vnamelink a.styleColor").count();
        if (count === lastCount) stableRounds++;
        else stableRounds = 0;
        lastCount = count;

        if (stableRounds >= 8 && count > 0) break;
      }

      // Extract cards
      vehicles = await page.$$eval("div.inv-type.used-vehicle", (cards) => {
        const vehicles = [];
        const seen = new Set();

        const clean = (s) => String(s || "").replace(/\s+/g, " ").trim();

        for (const card of cards) {
          try {
            // VDP URL
            const a = card.querySelector("div.bold.fsize16.vnamelink a.styleColor");
            const href = a?.getAttribute("href") || "";
            if (!href) continue;
            const vdpUrl = new URL(href, location.href).toString();

            // Heading like: "2020 Hyundai Elantra Preferred"
            const heading = clean(a.textContent);
            const parts = heading.split(" ").filter(Boolean);

            const year = parts[0] || null;
            const make = parts[1] || null;
            const model = parts[2] || null;
            const trim = parts.slice(3).join(" ") || null;

            // Map label->value from overview list
            const labelValueMap = {};
            const items = card.querySelectorAll(".result-vehicle-overview-list li");
            for (const li of items) {
              const t = clean(li.textContent);
              const kv = t.split(":");
              if (kv.length >= 2) {
                const k = clean(kv[0]).toLowerCase();
                const v = clean(kv.slice(1).join(":"));
                labelValueMap[k] = v;
              }
            }

            const vin =
              labelValueMap["vin"] ||
              labelValueMap["vin#"] ||
              labelValueMap["vin #"] ||
              "unlisted";
            const stockNumber =
              labelValueMap["stock #"] ||
              labelValueMap["stock"] ||
              "unlisted";

            const dedupeKey = vin !== "unlisted" ? `vin:${vin}` : `stock:${stockNumber}`;
            if (seen.has(dedupeKey)) continue;
            seen.add(dedupeKey);

            const mileage = labelValueMap["mileage"] || "unlisted";
            const exteriorColor =
              labelValueMap["exterior"] || labelValueMap["exterior color"] || "unlisted";
            const transmission =
              labelValueMap["tran"] || labelValueMap["transmission"] || "unlisted";

            let price = "unlisted";
            const priceMatch = (card.innerText || "").match(/\$\d[\d,]+/);
            if (priceMatch) price = priceMatch[0];

            let vid = null;
            const imagesLink = card.querySelector('div.result-blink a[onclick*="openVehImages("]');
            const onclick = imagesLink?.getAttribute("onclick") || "";
            const m = onclick.match(/openVehImages\((\d+)\s*,/);
            if (m) vid = m[1];

            vehicles.push({
              vid,
              url: vdpUrl,
              year,
              make,
              model,
              trim,
              vin,
              stockNumber,
              mileage,
              exteriorColor,
              transmission,
              price,
              rawHeading: heading,
              overviewLabels: labelValueMap,
            });
          } catch {
            // ignore per-card errors
          }
        }
        return vehicles;
      });

      // ✅ ALWAYS fetch image source URLs for all vehicles
      const imageUrlResults = await mapWithConcurrency(
        vehicles,
        IMAGE_FETCH_CONCURRENCY,
        async (v) => {
          const rel = await fetchVehicleImages(context, INVENTORY_URL, v.vid);
          return rel.map((src) => toAbs(src, INVENTORY_URL));
        }
      );

      for (let i = 0; i < vehicles.length; i++) {
        vehicles[i].imageSourceUrls = imageUrlResults[i] || [];
      }
    }

    // ✅ Normalize vehicles into final schema objects
    const scrapedAt = startedAt;
    const normalizedVehicles = vehicles.map((v) => {
      const upstreamWarnings = Array.isArray(v._warnings) ? v._warnings : [];
      const warnings = [...upstreamWarnings];

      const vinNorm = normalizeVin(v.vin);
      if (!vinNorm || vinNorm.length !== 17) warnings.push("vin_missing_or_invalid");

      const priceNum = parseNumberFromText(v.price);
      if (priceNum == null) warnings.push("price_unparseable");

      const mileageKm = parseNumberFromText(v.mileage);
      if (mileageKm == null) warnings.push("mileage_unparseable");

      const yearNum = parseNumberFromText(v.year);
      if (yearNum == null) warnings.push("year_unparseable");

      // Stable external key for Airtable upsert later
      const externalKey =
        vinNorm && vinNorm.length === 17
          ? `${DEALER_ID}::vin::${vinNorm}`
          : v.stockNumber && v.stockNumber !== "unlisted"
          ? `${DEALER_ID}::stock::${String(v.stockNumber).trim()}`
          : v.vid
          ? `${DEALER_ID}::vid::${String(v.vid).trim()}`
          : `${DEALER_ID}::url::${v.url}`;

      return {
        schemaVersion: SCHEMA_VERSION,
        dealerGroup: DEALER_GROUP,
        dealership: DEALERSHIP,
        dealerId: DEALER_ID,
        scraperId: SCRAPER_ID,
        scrapedAt,
        inventoryUrl: INVENTORY_URL,
        externalKey,

        // core fields
        vid: v.vid || null,
        vdpUrl: v.url,
        vin: vinNorm,
        stockNumber: v.stockNumber || null,
        year: yearNum,
        make: v.make || null,
        model: v.model || null,
        trim: v.trim || null,
        price: priceNum,
        mileageKm,
        exteriorColor: v.exteriorColor || null,
        transmission: v.transmission || null,

        // images: keep dealer URLs separately; we'll replace `imageUrls` with AWS URLs later.
        imageSourceUrls: (v.imageSourceUrls || []).slice(0, MAX_IMAGES_PER_VEHICLE),

        // filled after uploads
        imageS3Keys: [],
        awsImageUrls: [],
        imageUrls: [],

        // diagnostics
        _warnings: warnings,
      };
    });

    // Build all image download tasks
    const downloadTasks = [];
    for (const v of normalizedVehicles) {
      const vinOrStock = safeSlug(v.vin || v.stockNumber || v.vid || "unknown");
      const prefix = `images/${DEALER_ID}/${vinOrStock}/${runId}/`;

      (v.imageSourceUrls || []).forEach((urlAbs, idx) => {
        // Base key uses .jpg; we will correct extension after fetch using Content-Type.
        const keyBase = `${prefix}${String(idx + 1).padStart(2, "0")}.jpg`;
        downloadTasks.push({ vehicle: v, urlAbs, keyBase });
      });
    }

    // Download + upload images with global concurrency
    const uploadResults = await mapWithConcurrency(
      downloadTasks,
      IMAGE_DOWNLOAD_CONCURRENCY,
      async (t) => {
        const uploaded = await downloadAndUploadImage({
          imageUrlAbs: t.urlAbs,
          s3KeyBase: t.keyBase,
        });
        return { vehicle: t.vehicle, urlAbs: t.urlAbs, uploaded };
      }
    );

    // Attach uploaded keys back to vehicles
    for (const r of uploadResults) {
      if (r?.uploaded?.s3Key) r.vehicle.imageS3Keys.push(r.uploaded.s3Key);
    }

    // ✅ Build CloudFront URLs (or S3 URIs fallback) and REPLACE imageUrls with durable AWS links
    for (const v of normalizedVehicles) {
      v.awsImageUrls = (v.imageS3Keys || []).map(s3KeyToAwsUrl).filter(Boolean);
      v.imageUrls = v.awsImageUrls; // downstream-friendly: "imageUrls" now means durable links
    }

    // Write run JSON to S3
    const runPayload = {
      ok: true,
      schemaVersion: SCHEMA_VERSION,
      runId,
      scrapedAt: startedAt,
      batchId: BATCH_ID,
      dealerGroup: DEALER_GROUP,
      dealership: DEALERSHIP,
      dealerId: DEALER_ID,
      scraperId: SCRAPER_ID,
      inventoryUrl: INVENTORY_URL,
      count: normalizedVehicles.length,
      vehicles: normalizedVehicles,
    };

    await putToS3({
      key: runS3Key,
      body: JSON.stringify(runPayload),
      contentType: "application/json",
    });

    // CloudWatch summary only
    const totalImagesFound = normalizedVehicles.reduce(
      (acc, v) => acc + (v.imageSourceUrls?.length || 0),
      0
    );
    const totalUploaded = normalizedVehicles.reduce(
      (acc, v) => acc + (v.imageS3Keys?.length || 0),
      0
    );

    console.log(
      JSON.stringify({
        ok: true,
        runId,
        batchId: BATCH_ID,
        dealerId: DEALER_ID,
        count: normalizedVehicles.length,
        totalImagesFound,
        totalImagesUploaded: totalUploaded,
        awsPublicBaseUrl: AWS_PUBLIC_BASE_URL || null,
        cloudfrontOriginPathPrefix: CLOUDFRONT_ORIGIN_PATH_PREFIX || null,
        runS3Key,
        bucket: S3_BUCKET,
      })
    );
  } catch (e) {
    console.log(
      JSON.stringify({
        ok: false,
        runId,
        dealerId: DEALER_ID,
        inventoryUrl: INVENTORY_URL,
        error: String(e?.message || e),
      })
    );
    process.exitCode = 1;
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
})();

