async function apiFetch(url, options = {}) {
  const headers = Object.assign(
    { "Content-Type": "application/json" },
    options.headers || {}
  );
  const response = await fetch(
    url,
    Object.assign({}, options, { headers, credentials: "same-origin" })
  );
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || response.statusText);
  }
  return response.json();
}

function wireEnqueueButtons() {
  document.querySelectorAll("[data-enqueue]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const jobType = btn.getAttribute("data-enqueue");
      try {
        await apiFetch("/jobs/enqueue", {
          method: "POST",
          body: JSON.stringify({ job_type: jobType }),
        });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireSources() {
  const form = document.getElementById("source-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("source-id");
  const nameField = document.getElementById("source-name");
  const kindField = document.getElementById("source-kind");
  const urlField = document.getElementById("source-url");
  const intervalField = document.getElementById("source-interval");
  const tagsField = document.getElementById("source-tags");
  const enabledField = document.getElementById("source-enabled");
  const resetBtn = document.getElementById("source-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    kindField.value = "rss";
    urlField.value = "";
    intervalField.value = "60";
    tagsField.value = "";
    enabledField.checked = true;
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      kind: kindField.value,
      url: urlField.value.trim(),
      interval_minutes: parseInt(intervalField.value, 10),
      tags: tagsField.value.trim(),
      enabled: enabledField.checked,
    };
    const target = idField.value ? `/sources/${idField.value}` : "/sources";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, {
        method,
        body: JSON.stringify(payload),
      });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-source").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.sourceId;
      nameField.value = row.querySelector(".source-name").textContent.trim();
      kindField.value = row.querySelector(".source-kind").textContent.trim() || "rss";
      urlField.value = row.querySelector(".source-url").textContent.trim();
      intervalField.value = row.querySelector(".source-interval").textContent.trim() || "60";
      enabledField.checked = row.querySelector(".toggle-enabled").checked;
    });
  });

  document.querySelectorAll(".toggle-enabled").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const sourceId = row.dataset.sourceId;
      try {
        await apiFetch(`/sources/${sourceId}`, {
          method: "PATCH",
          body: JSON.stringify({ enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });

  document.querySelectorAll(".delete-source").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const sourceId = row.dataset.sourceId;
      if (!confirm(`Delete source ${sourceId}?`)) {
        return;
      }
      try {
        await apiFetch(`/sources/${sourceId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });

  document.querySelectorAll(".test-source").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const sourceId = row.dataset.sourceId;
      const outputRow = document.querySelector(`tr.test-result[data-source-id="${sourceId}"]`);
      try {
        const result = await apiFetch(`/sources/${sourceId}/test`, { method: "POST" });
        const items = (result.items || [])
          .map((item) => `- ${item.title || ""} ${item.url || ""}`)
          .join("\n");
        outputRow.querySelector("td").textContent =
          `status=${result.status} http=${result.http_status || ""} found=${result.found_count} accepted=${result.accepted_count}\n${items}`;
        outputRow.style.display = "table-row";
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireJobs() {
  const refresh = document.getElementById("jobs-refresh");
  if (!refresh) {
    return;
  }
  refresh.addEventListener("click", () => {
    window.location.reload();
  });
}

function wireLogin() {
  const form = document.getElementById("login-form");
  if (!form) {
    return;
  }
  const tokenInput = document.getElementById("login-token");
  const error = document.getElementById("login-error");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await apiFetch("/ui/login", {
        method: "POST",
        body: JSON.stringify({ token: tokenInput.value.trim() }),
      });
      window.location.href = "/ui";
    } catch (err) {
      error.style.display = "block";
      error.textContent = "Invalid token";
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  wireEnqueueButtons();
  wireSources();
  wireJobs();
  wireLogin();
});
