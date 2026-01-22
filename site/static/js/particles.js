(() => {
  // Respect Reduce Motion
  if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;

  const DOC = globalThis.document;
  const root = DOC.documentElement;

  function start() {
    // Don't double-create
    let canvas = DOC.getElementById("bg-canvas");
    if (!canvas) {
      canvas = DOC.createElement("canvas");
      canvas.id = "bg-canvas";

      canvas.style.position = "fixed";
      canvas.style.inset = "0";
      canvas.style.width = "100vw";
      canvas.style.height = "100vh";
      canvas.style.pointerEvents = "none";
      canvas.style.zIndex = "1"; // above background, below content
      canvas.style.opacity = "1";
      canvas.style.display = "block";

      DOC.body.prepend(canvas);
    }

    const ctx = canvas.getContext("2d", { alpha: true });
    if (!ctx) return;

    // ---- Theme detection ----
    const prefersDarkMQ = window.matchMedia?.("(prefers-color-scheme: dark)");

    function getTheme() {
      // Blowfish commonly uses data-theme="light|dark" on <html>
      const dt = root.getAttribute("data-theme");
      if (dt === "dark" || dt === "light") return dt;

      // Some configs use data-default-appearance
      const da = root.getAttribute("data-default-appearance");
      if (da === "dark" || da === "light") return da;

      // Or a class toggle
      if (root.classList.contains("dark")) return "dark";
      if (root.classList.contains("light")) return "light";

      // fallback: OS preference
      return prefersDarkMQ?.matches ? "dark" : "light";
    }

    // Helper: read CSS variable (fallback if missing)
    const cssVar = (name, fallback) => {
      const v = getComputedStyle(root).getPropertyValue(name).trim();
      return v || fallback;
    };

    // Parse "rgb(...)" / "#rrggbb" into {r,g,b}
    const parseColor = (s) => {
      s = (s || "").trim();
      if (!s) return null;

      const m = s.match(/^rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(?:\s*,\s*[\d.]+)?\s*\)$/i);
      if (m) return { r: +m[1], g: +m[2], b: +m[3] };

      const h = s.replace("#", "");
      if (h.length === 3) {
        return {
          r: parseInt(h[0] + h[0], 16),
          g: parseInt(h[1] + h[1], 16),
          b: parseInt(h[2] + h[2], 16),
        };
      }
      if (h.length === 6) {
        return {
          r: parseInt(h.slice(0, 2), 16),
          g: parseInt(h.slice(2, 4), 16),
          b: parseInt(h.slice(4, 6), 16),
        };
      }
      return null;
    };

    function resolveParticleRGB(theme) {
      // Optional explicit overrides from CSS:
      // :root { --particles-color-light: rgba(0,0,0,0.9); }
      // :root[data-theme="dark"] { --particles-color-dark: rgba(255,255,255,0.9); }
      const darkVar  = cssVar("--particles-color-dark", "");
      const lightVar = cssVar("--particles-color-light", "");
      if (theme === "dark" && darkVar) return parseColor(darkVar);
      if (theme === "light" && lightVar) return parseColor(lightVar);

      // Otherwise choose contrasting color vs background
      const bg =
        parseColor(cssVar("--color-bg", "")) ||
        parseColor(getComputedStyle(DOC.body).backgroundColor);

      if (!bg) return theme === "dark" ? { r: 255, g: 255, b: 255 } : { r: 0, g: 0, b: 0 };

      const lum = 0.2126 * (bg.r / 255) + 0.7152 * (bg.g / 255) + 0.0722 * (bg.b / 255);
      return lum < 0.5 ? { r: 255, g: 255, b: 255 } : { r: 0, g: 0, b: 0 };
    }

    function applyThemeLayering(theme) {
      // Screen blend helps dark mode; normal is best for light
      canvas.style.mixBlendMode = (theme === "dark") ? "screen" : "normal";
    }

    let w = 0, h = 0;
    function resize() {
      const dpr = Math.max(1, window.devicePixelRatio || 1);
      w = Math.floor(window.innerWidth);
      h = Math.floor(window.innerHeight);
      canvas.width = Math.floor(w * dpr);
      canvas.height = Math.floor(h * dpr);
      canvas.style.width = w + "px";
      canvas.style.height = h + "px";
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
    window.addEventListener("resize", resize, { passive: true });
    resize();

    // ---- Particle system ----
    const particleCount = 80;
    const particles = Array.from({ length: particleCount }, () => ({
      x: Math.random() * w,
      y: Math.random() * h,
      r: Math.random() * 1.6 + 0.8,
      vx: (Math.random() - 0.5) * 0.25,
      vy: (Math.random() - 0.5) * 0.25,
      o: Math.random() * 0.35 + 0.12
    }));

    let theme = getTheme();
    let rgb = resolveParticleRGB(theme);
    applyThemeLayering(theme);

    // IMPORTANT: recompute color *after* theme toggle has applied styles
    function refreshTheme() {
      const next = getTheme();
      theme = next;

      // let CSS settle (theme toggle updates DOM + CSS vars)
      requestAnimationFrame(() => {
        rgb = resolveParticleRGB(theme);
        applyThemeLayering(theme);
      });
    }

    // Watch ALL likely toggle mechanisms
    const mo = new MutationObserver(refreshTheme);
    mo.observe(root, {
      attributes: true,
      attributeFilter: ["data-theme", "data-default-appearance", "class"]
    });

    // Also catch OS theme changes if user has auto-switch enabled
    prefersDarkMQ?.addEventListener?.("change", refreshTheme);

    function rgba(a) {
      return `rgba(${rgb.r},${rgb.g},${rgb.b},${a})`;
    }

    function step() {
      ctx.clearRect(0, 0, w, h);

      for (const p of particles) {
        p.x += p.vx; p.y += p.vy;

        if (p.x < -10) p.x = w + 10;
        if (p.x > w + 10) p.x = -10;
        if (p.y < -10) p.y = h + 10;
        if (p.y > h + 10) p.y = -10;

        ctx.beginPath();
        ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
        ctx.fillStyle = rgba(p.o);
        ctx.fill();
      }

      requestAnimationFrame(step);
    }

    step();

    // one initial “settle” pass
    refreshTheme();
  }

  if (DOC.readyState === "loading") {
    DOC.addEventListener("DOMContentLoaded", start, { once: true });
  } else {
    start();
  }
})();