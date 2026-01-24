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

function parseJsonField(value, fallback) {
  if (!value) {
    return fallback;
  }
  try {
    return JSON.parse(value);
  } catch (err) {
    alert("Invalid JSON: " + err);
    throw err;
  }
}

function wireAiProviders() {
  const form = document.getElementById("provider-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("provider-id");
  const nameField = document.getElementById("provider-name");
  const typeField = document.getElementById("provider-type");
  const baseField = document.getElementById("provider-base-url");
  const timeoutField = document.getElementById("provider-timeout");
  const retriesField = document.getElementById("provider-retries");
  const enabledField = document.getElementById("provider-enabled");
  const resetBtn = document.getElementById("provider-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    typeField.value = "openai_compatible";
    baseField.value = "";
    timeoutField.value = "30";
    retriesField.value = "2";
    enabledField.checked = true;
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      type: typeField.value,
      base_url: baseField.value.trim() || undefined,
      timeout_s: parseInt(timeoutField.value, 10),
      retries: parseInt(retriesField.value, 10),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/providers/${idField.value}` : "/admin/ai/providers";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-provider").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.providerId;
      nameField.value = row.querySelector(".provider-name").textContent.trim();
      typeField.value = row.querySelector(".provider-type").textContent.trim();
      baseField.value = row.querySelector(".provider-base-url").textContent.trim();
    });
  });

  document.querySelectorAll(".toggle-provider").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const providerId = row.dataset.providerId;
      try {
        await apiFetch(`/admin/ai/providers/${providerId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });

  document.querySelectorAll(".set-provider-key").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      const apiKey = prompt("Enter API key (will be stored encrypted)");
      if (!apiKey) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/secret`, {
          method: "POST",
          body: JSON.stringify({ api_key: apiKey }),
        });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });

  document.querySelectorAll(".clear-provider-key").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      if (!confirm("Clear API key?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/secret`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });

  document.querySelectorAll(".test-provider").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const providerId = row.dataset.providerId;
      try {
        await apiFetch(`/admin/ai/providers/${providerId}/test`, { method: "POST" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireAiModels() {
  const form = document.getElementById("model-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("model-id");
  const providerField = document.getElementById("model-provider");
  const nameField = document.getElementById("model-name");
  const contextField = document.getElementById("model-context");
  const tagsField = document.getElementById("model-tags");
  const paramsField = document.getElementById("model-params");
  const enabledField = document.getElementById("model-enabled");
  const resetBtn = document.getElementById("model-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    contextField.value = "";
    tagsField.value = "";
    paramsField.value = "";
    enabledField.checked = true;
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      provider_id: providerField.value,
      model_name: nameField.value.trim(),
      max_context: contextField.value ? parseInt(contextField.value, 10) : undefined,
      tags: tagsField.value.trim(),
      default_params: parseJsonField(paramsField.value.trim(), {}),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/models/${idField.value}` : "/admin/ai/models";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-model").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.modelId;
      providerField.value = row.dataset.providerId;
      nameField.value = row.querySelector(".model-name").textContent.trim();
      tagsField.value = row.querySelector(".model-tags").textContent.trim();
    });
  });

  document.querySelectorAll(".toggle-model").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const modelId = row.dataset.modelId;
      try {
        await apiFetch(`/admin/ai/models/${modelId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });

  document.querySelectorAll(".delete-model").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const modelId = row.dataset.modelId;
      if (!confirm("Delete model?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/models/${modelId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireAiPrompts() {
  const form = document.getElementById("prompt-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("prompt-id");
  const nameField = document.getElementById("prompt-name");
  const versionField = document.getElementById("prompt-version");
  const systemField = document.getElementById("prompt-system");
  const userField = document.getElementById("prompt-user");
  const notesField = document.getElementById("prompt-notes");
  const resetBtn = document.getElementById("prompt-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    versionField.value = "v1";
    systemField.value = "";
    userField.value = "";
    notesField.value = "";
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      version: versionField.value.trim(),
      system_template: systemField.value,
      user_template: userField.value,
      notes: notesField.value.trim(),
    };
    const target = idField.value ? `/admin/ai/prompts/${idField.value}` : "/admin/ai/prompts";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-prompt").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.promptId;
      nameField.value = row.querySelector(".prompt-name").textContent.trim();
      versionField.value = row.querySelector(".prompt-version").textContent.trim();
    });
  });

  document.querySelectorAll(".delete-prompt").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const promptId = row.dataset.promptId;
      if (!confirm("Delete prompt?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/prompts/${promptId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireAiSchemas() {
  const form = document.getElementById("schema-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("schema-id");
  const nameField = document.getElementById("schema-name");
  const versionField = document.getElementById("schema-version");
  const jsonField = document.getElementById("schema-json");
  const resetBtn = document.getElementById("schema-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    versionField.value = "v1";
    jsonField.value = "";
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      version: versionField.value.trim(),
      json_schema: parseJsonField(jsonField.value.trim(), {}),
    };
    const target = idField.value ? `/admin/ai/schemas/${idField.value}` : "/admin/ai/schemas";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-schema").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.schemaId;
      nameField.value = row.querySelector(".schema-name").textContent.trim();
      versionField.value = row.querySelector(".schema-version").textContent.trim();
    });
  });

  document.querySelectorAll(".delete-schema").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const schemaId = row.dataset.schemaId;
      if (!confirm("Delete schema?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/schemas/${schemaId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireAiProfiles() {
  const form = document.getElementById("profile-form");
  if (!form) {
    return;
  }
  const idField = document.getElementById("profile-id");
  const nameField = document.getElementById("profile-name");
  const providerField = document.getElementById("profile-provider");
  const modelField = document.getElementById("profile-model");
  const promptField = document.getElementById("profile-prompt");
  const schemaField = document.getElementById("profile-schema");
  const paramsField = document.getElementById("profile-params");
  const fallbackField = document.getElementById("profile-fallback");
  const enabledField = document.getElementById("profile-enabled");
  const resetBtn = document.getElementById("profile-reset");

  function resetForm() {
    idField.value = "";
    nameField.value = "";
    paramsField.value = "";
    fallbackField.value = "";
    enabledField.checked = true;
  }

  resetBtn.addEventListener("click", resetForm);

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const payload = {
      id: idField.value.trim() || undefined,
      name: nameField.value.trim(),
      primary_provider_id: providerField.value,
      primary_model_id: modelField.value,
      prompt_id: promptField.value,
      schema_id: schemaField.value || null,
      params: parseJsonField(paramsField.value.trim(), {}),
      fallback: parseJsonField(fallbackField.value.trim(), []),
      is_enabled: enabledField.checked,
    };
    const target = idField.value ? `/admin/ai/profiles/${idField.value}` : "/admin/ai/profiles";
    const method = idField.value ? "PATCH" : "POST";
    try {
      await apiFetch(target, { method, body: JSON.stringify(payload) });
      window.location.reload();
    } catch (err) {
      alert(err);
    }
  });

  document.querySelectorAll(".edit-profile").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = btn.closest("tr");
      idField.value = row.dataset.profileId;
      nameField.value = row.querySelector(".profile-name").textContent.trim();
    });
  });

  document.querySelectorAll(".toggle-profile").forEach((checkbox) => {
    checkbox.addEventListener("change", async () => {
      const row = checkbox.closest("tr");
      const profileId = row.dataset.profileId;
      try {
        await apiFetch(`/admin/ai/profiles/${profileId}`, {
          method: "PATCH",
          body: JSON.stringify({ is_enabled: checkbox.checked }),
        });
      } catch (err) {
        alert(err);
        checkbox.checked = !checkbox.checked;
      }
    });
  });

  document.querySelectorAll(".delete-profile").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const profileId = row.dataset.profileId;
      if (!confirm("Delete profile?")) {
        return;
      }
      try {
        await apiFetch(`/admin/ai/profiles/${profileId}`, { method: "DELETE" });
        window.location.reload();
      } catch (err) {
        alert(err);
      }
    });
  });

  document.querySelectorAll(".test-profile").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const profileId = row.dataset.profileId;
      const text = prompt("Enter test input");
      if (!text) {
        return;
      }
      try {
        const result = await apiFetch(`/admin/ai/profiles/${profileId}/test`, {
          method: "POST",
          body: JSON.stringify({ text }),
        });
        alert(
          result.schema_valid
            ? "Schema valid"
            : `Schema invalid: ${result.schema_error || ""}`
        );
      } catch (err) {
        alert(err);
      }
    });
  });
}

function wireAiRouting() {
  document.querySelectorAll(".save-route").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const row = btn.closest("tr");
      const stage = row.dataset.stageName;
      const select = row.querySelector(".route-profile");
      const profileId = select.value;
      if (!profileId) {
        alert("Select a profile first");
        return;
      }
      try {
        await apiFetch("/admin/ai/pipeline-routing", {
          method: "POST",
          body: JSON.stringify({ stage_name: stage, profile_id: profileId }),
        });
        alert("Saved");
      } catch (err) {
        alert(err);
      }
    });
  });
}

document.addEventListener("DOMContentLoaded", () => {
  wireEnqueueButtons();
  wireSources();
  wireJobs();
  wireLogin();
  wireAiProviders();
  wireAiModels();
  wireAiPrompts();
  wireAiSchemas();
  wireAiProfiles();
  wireAiRouting();
});
