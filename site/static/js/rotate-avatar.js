(() => {
  const DOC = document;
  if (!DOC) return;

  const IMAGES = [
    "04A09E85-2A92-4637-AF63-C1C230B63AF4.jpg",
    "5DF3D3A7-34CC-4E0B-8E04-16906E9C210F.jpg",
    "5E8B480F-D8F8-4500-8321-13A1D66375EF.jpg",
    "7A3C0B39-D8C8-49F1-B784-2626A8E1E9CA.jpg",
    "7C5C7631-667D-4C84-A999-080DF2C95C3F.jpg",
    "17A51CCF-C0E5-457F-B5EA-7FD867BA2041.jpg",
    "32BFB80A-AEAA-4F31-A866-BE2693EA521C.jpg",
    "970E5B18-308D-47C6-AFE7-681EF32E80D1.jpg",
    "13688D92-AB4A-4834-9C44-FDEA6020E8E6.jpg",
    "34464BDB-AEF6-4ADD-AA9A-02ED9AAC4568.jpg",
    "99772EB2-FB9C-4107-9CE1-E552797FC69B.jpg",
    "ACECE76C-47C2-4B83-B9DC-849852188072.jpg",
    "avatar.jpg",
    "B0950E43-1B74-494D-85EE-80B758DF4B75.jpg",
    "CDAB458D-A6CA-4D51-9F5F-C9E316D31BF9.jpg",
    "EA3309BC-B152-4167-A204-8B19F7655AAE.jpg",
    "EE277BA4-9F6F-4F8D-AE8A-AF1DF9F7143E.jpg",
    "F80F337F-93AA-49D8-9CA6-B6276423B3AF.jpg",
    "selfie.jpg",
  ];

  const BASE = "/img/";
  const INTERVAL = 3000;
  let timer = null;

  function findAvatar() {
    return (
      DOC.querySelector("header img") ||
      DOC.querySelector(".profile img") ||
      DOC.querySelector('img[src*="/img/avatar"]') ||
      DOC.querySelector('img[src*="/img/"]')
    );
  }

  // Keep the zoom target in sync with the rotated image
  function syncZoomTarget(img, nextAbs) {
    // If image is wrapped in a link, zoom typically uses the link target
    const a = img.closest("a");
    if (a) {
      a.href = nextAbs;
      a.setAttribute("data-zoom-src", nextAbs);
      a.setAttribute("data-src", nextAbs);
    }

    // Some zoom libs read from img data-attributes instead
    img.setAttribute("data-zoom-src", nextAbs);
    img.setAttribute("data-src", nextAbs);
    img.setAttribute("data-original", nextAbs);
  }

  // Random pick, but don’t repeat the current one
  function pickNext(currentAbs) {
    const currentFile = (currentAbs || "").split("/").pop()?.split("?")[0] || "";
    const pool = IMAGES.filter(f => f && f !== currentFile);
    return pool[Math.floor(Math.random() * pool.length)] || IMAGES[0];
  }

  function rotate(img) {
    const currentAbs = img.currentSrc || img.src || "";
    const nextFile = pickNext(currentAbs);
    const nextAbs = BASE + nextFile;

    img.style.transition = "opacity 250ms ease";
    img.style.opacity = "0";

    const preload = new Image();
    preload.onload = () => {
      // Set src (cache-bust for dev)
      img.src = nextAbs + "?v=" + Date.now();

      // IMPORTANT: update what the zoom/click handler uses
      syncZoomTarget(img, nextAbs);

      img.style.opacity = "1";
    };
    preload.src = nextAbs;
  }

  function start() {
    const img = findAvatar();
    if (!img) return;

    img.style.borderRadius = "9999px";

    // Ensure initial zoom target matches whatever is currently showing
    const initialAbs = (img.currentSrc || img.src || "");
    if (initialAbs.includes("/img/")) {
      syncZoomTarget(img, initialAbs.split("?")[0]);
    }

    if (timer) clearInterval(timer);
    timer = setInterval(() => rotate(img), INTERVAL);
  }

  // Blowfish instant navigation – DOM can change
  const observer = new MutationObserver(start);

  function boot() {
    start();
    observer.observe(DOC.body, { childList: true, subtree: true });
  }

  if (DOC.readyState === "loading") {
    DOC.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();