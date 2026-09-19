"use strict";
(() => {
  const body = document.body;
  const packet = body.dataset.packet;
  const workflow = body.dataset.workflow;
  const key = "sempervigil-event-review:" + packet + ":" + body.dataset.review;
  const passages = [...document.querySelectorAll("[data-passage]")];
  const ids = new Set(passages.map(p => p.dataset.passage));
  const note = document.getElementById("review-note");
  const status = document.getElementById("storage-status");
  let reading = false;
  function valid(value) {
    return value && typeof value === "object" && !Array.isArray(value) &&
      Object.keys(value).sort().join(",") === "decisions,note,packet_version,workflow" &&
      value.packet_version === packet && value.workflow === workflow &&
      typeof value.note === "string" && value.note.length <= 1000 &&
      value.decisions && typeof value.decisions === "object" && !Array.isArray(value.decisions) &&
      Object.entries(value.decisions).every(([id, decision]) => ids.has(id) && ["include", "exclude", "hold"].includes(decision));
  }
  function collect() {
    return {workflow, packet_version: packet, note: note.value,
      decisions: Object.fromEntries(passages.map(p => [p.dataset.passage, p.querySelector("select").value]))};
  }
  function update() {
    let included = 0, excluded = 0;
    for (const p of passages) {
      const choice = p.querySelector("select").value;
      p.dataset.state = choice;
      included += choice === "include" ? 1 : 0;
      excluded += choice === "exclude" ? 1 : 0;
      p.hidden = reading && choice !== "include";
    }
    for (const source of document.querySelectorAll(".source")) {
      source.hidden = reading && ![...source.querySelectorAll("[data-passage]")].some(p => !p.hidden);
    }
    document.getElementById("progress").textContent = `${included} included / ${excluded} excluded / ${passages.length - included - excluded} pending`;
    document.getElementById("empty-reading").hidden = !reading || included > 0;
    body.classList.toggle("reading", reading);
  }
  function save() {
    try {
      localStorage.setItem(key, JSON.stringify(collect()));
      status.textContent = "Decisions saved in this browser. Download for a durable copy.";
    } catch (_) {
      status.textContent = "Browser storage unavailable. Download decisions before closing this page.";
    }
    update();
  }
  try {
    const saved = localStorage.getItem(key);
    if (saved) {
      const value = JSON.parse(saved);
      if (!valid(value)) throw new Error("Invalid saved decisions");
      note.value = value.note;
      for (const p of passages) p.querySelector("select").value = value.decisions[p.dataset.passage] || "hold";
      status.textContent = "Restored browser decisions for this exact snapshot.";
    } else status.textContent = "No saved browser decisions for this snapshot.";
  } catch (_) {
    status.textContent = "Saved decisions unavailable or invalid. Use Download decisions to keep your review.";
  }
  for (const p of passages) p.querySelector("select").addEventListener("change", save);
  note.addEventListener("input", save);
  document.getElementById("mode").addEventListener("click", event => {
    reading = !reading;
    event.currentTarget.setAttribute("aria-pressed", String(reading));
    event.currentTarget.textContent = reading ? "Return to review" : "Reading view";
    update();
  });
  document.getElementById("export").addEventListener("click", () => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(collect(), null, 2)], {type: "application/json"}));
    const link = document.createElement("a");
    link.href = url;
    link.download = "event-review-" + packet + ".json";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
  update();
})();
