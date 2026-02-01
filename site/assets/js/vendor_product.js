(() => {
  const esc = (value) => {
    const text = String(value ?? "");
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  };

  const readJson = (id) => {
    const el = document.getElementById(id);
    if (!el) return null;
    try {
      return JSON.parse(el.textContent || "null");
    } catch (err) {
      return null;
    }
  };

  const normalizeList = (items) => {
    if (!Array.isArray(items)) return [];
    return items.filter((item) => item && item.slug);
  };

  const extractSummary = (value) => {
    if (!value) return "";
    let text = String(value).trim();
    if (!text) return "";
    if (text.startsWith("```")) {
      text = text.replace(/^```[a-zA-Z]*\s*/i, "").replace(/```$/, "").trim();
    }
    if (text.startsWith("{") || text.startsWith("[")) {
      try {
        const parsed = JSON.parse(text);
        if (parsed && typeof parsed === "object") {
          if (typeof parsed.summary === "string") return parsed.summary.trim();
          if (Array.isArray(parsed.bullets) && parsed.bullets.length) {
            return parsed.bullets.join(" ").trim();
          }
          if (Array.isArray(parsed.key_points) && parsed.key_points.length) {
            return parsed.key_points.join(" ").trim();
          }
        }
      } catch (err) {
        return text;
      }
    }
    return text;
  };

  const slugFromPath = (prefixes) => {
    const list = Array.isArray(prefixes) ? prefixes : [prefixes];
    const parts = window.location.pathname.split("/").filter(Boolean);
    for (const prefix of list) {
      const idx = parts.indexOf(prefix);
      if (idx >= 0 && parts[idx + 1]) {
        return parts[idx + 1];
      }
    }
    return "";
  };

  const renderArticle = (item) => {
    const vendors = Array.isArray(item.vendors) ? item.vendors : [];
    const products = Array.isArray(item.product_items) ? item.product_items : [];
    const summary = extractSummary(item.summary);
    const chips = [
      ...vendors.map(
        (vendor) =>
          `<a class="sv-chip sv-chip--vendor" href="/vendor/${esc(vendor.slug)}/">${esc(
            vendor.display_name || vendor.name || ""
          )}</a>`
      ),
      ...products.map(
        (product) =>
          `<a class="sv-chip sv-chip--product" href="/product/${esc(product.slug)}/">${esc(
            product.display_name || product.name || ""
          )}</a>`
      ),
    ].join("");
    return `
      <article class="sv-card">
        <div class="sv-card-body">
          <div class="sv-card-header">
            <a class="sv-title" href="${esc(item.url || "")}" target="_blank" rel="noopener">${esc(
      item.title || ""
    )}</a>
            <div class="sv-meta">
              <span class="sv-source">${esc(item.source || "")}</span>
              ${item.published_at_human ? `<span class="sv-sep">•</span><span class="sv-date">${esc(item.published_at_human)}</span>` : ""}
            </div>
          </div>
          ${chips ? `<div class="sv-chips">${chips}</div>` : ""}
          ${summary ? `<p class="sv-summary sv-summary--clamp">${esc(summary)}</p>` : ""}
        </div>
      </article>
    `;
  };

  const renderCve = (item) => {
    const vendors = Array.isArray(item.vendors) ? item.vendors : [];
    const products = Array.isArray(item.product_items) ? item.product_items : [];
    const versions = Array.isArray(item.versions) ? item.versions : [];
    const summary = extractSummary(item.summary);
    const chips = [
      ...vendors.map(
        (vendor) =>
          `<a class="sv-chip sv-chip--vendor" href="/vendor/${esc(vendor.slug)}/">${esc(
            vendor.display_name || vendor.name || ""
          )}</a>`
      ),
      ...products.map(
        (product) =>
          `<a class="sv-chip sv-chip--product" href="/product/${esc(product.slug)}/">${esc(
            product.display_name || product.name || ""
          )}</a>`
      ),
      ...versions.map((version) => `<span class="sv-chip sv-chip--tag">${esc(version)}</span>`),
    ].join("");
    return `
      <article class="sv-card sv-card--cve">
        <div class="sv-card-body">
          <div class="sv-card-header">
            <a class="sv-title" href="${esc(item.url || `https://nvd.nist.gov/vuln/detail/${item.cve_id || ""}`)}" target="_blank" rel="noopener">
              ${esc(item.cve_id || "CVE")}
            </a>
            <div class="sv-meta">
              ${item.severity ? `<span class="sv-chip sv-chip--tag">${esc(item.severity)}</span>` : ""}
              ${item.published_at_human ? `<span class="sv-sep">•</span><span class="sv-date">${esc(item.published_at_human)}</span>` : ""}
            </div>
          </div>
          ${chips ? `<div class="sv-chips">${chips}</div>` : ""}
          ${summary ? `<p class="sv-summary sv-summary--clamp">${esc(summary)}</p>` : ""}
        </div>
      </article>
    `;
  };

  const init = () => {
    const vendors = normalizeList(readJson("sv-vendors-data"));
    const products = normalizeList(readJson("sv-products-data"));
    const vendorMap = readJson("sv-vendor-map") || {};
    const productMap = readJson("sv-product-map") || {};
    const cloud = document.querySelector("[data-vp-cloud]");
    const results = document.querySelector("[data-vp-results]");
    const detail = document.querySelector("[data-vp-detail]");
    const title = document.querySelector("[data-vp-title]");
    const meta = document.querySelector("[data-vp-meta]");
    const articles = document.querySelector("[data-vp-articles]");
    const cves = document.querySelector("[data-vp-cves]");
    const search = document.querySelector(".sv-vp-search");
    let defaultSlug = document.querySelector("[data-vp-default-slug]")?.getAttribute("data-vp-default-slug") || "";
    const pageType = document.querySelector("[data-vp-page]")?.getAttribute("data-vp-page") || "";
    const inventory = pageType.startsWith("product") || pageType === "products" ? products : vendors;
    const indexMap = pageType.startsWith("product") || pageType === "products" ? productMap : vendorMap;
    if (!defaultSlug) {
      if (pageType.startsWith("product")) defaultSlug = slugFromPath(["product", "products"]);
      if (pageType.startsWith("vendor")) defaultSlug = slugFromPath(["vendor", "vendors"]);
    }

    const showResults = (items) => {
      if (!results) return;
      if (!items.length) {
        results.innerHTML = `<div class="sv-vp-empty">No matches found.</div>`;
        results.hidden = false;
        return;
      }
      results.innerHTML = items
        .map(
          (item) => `
            <button class="sv-vp-result" type="button" data-slug="${esc(item.slug)}">
              <span class="sv-vp-result-name">${esc(item.display_name || item.name || "")}</span>
              <span class="sv-vp-result-count">${esc(item.total_count ?? item.count_total ?? 0)}</span>
            </button>
          `
        )
        .join("");
      results.hidden = false;
    };

    const showDetail = (slug) => {
      if (!slug || !detail) return;
      const entry = indexMap[slug];
      if (!entry) {
        detail.hidden = false;
        if (title) title.textContent = "No data available";
        if (meta) meta.textContent = "";
        if (articles) articles.innerHTML = "";
        if (cves) cves.innerHTML = "";
        return;
      }
      detail.hidden = false;
      if (title) title.textContent = entry.display_name || entry.name || "";
      if (meta) {
        const articleCount = entry.article_count ?? entry.count_articles ?? 0;
        const cveCount = entry.cve_count ?? entry.count_cves ?? 0;
        meta.textContent = `${articleCount} articles · ${cveCount} CVEs`;
      }
      if (articles) {
        const items = Array.isArray(entry.article_items) ? entry.article_items : [];
        articles.innerHTML = items.length ? items.map(renderArticle).join("") : `<div class="sv-vp-empty">No linked articles yet.</div>`;
      }
      if (cves) {
        const items = Array.isArray(entry.cve_items) ? entry.cve_items : [];
        cves.innerHTML = items.length ? items.map(renderCve).join("") : `<div class="sv-vp-empty">No linked CVEs yet.</div>`;
      }
    };

    if (cloud) {
      cloud.addEventListener("click", (event) => {
        const button = event.target.closest("[data-slug]");
        if (!button) return;
        showDetail(button.getAttribute("data-slug"));
        if (results) results.hidden = true;
      });
    }

    if (results) {
      results.addEventListener("click", (event) => {
        const button = event.target.closest("[data-slug]");
        if (!button) return;
        showDetail(button.getAttribute("data-slug"));
      });
    }

    if (search) {
      search.addEventListener("input", (event) => {
        const value = event.target.value.trim().toLowerCase();
        if (!value) {
          if (results) results.hidden = true;
          if (cloud) cloud.hidden = false;
          return;
        }
        const matches = inventory.filter((item) => {
          const name = String(item.display_name || item.name || "").toLowerCase();
          return name.includes(value);
        });
        if (cloud) cloud.hidden = true;
        showResults(matches);
      });
    }

    if (defaultSlug) {
      showDetail(defaultSlug);
      if (cloud) cloud.hidden = true;
      if (results) results.hidden = true;
    }
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
