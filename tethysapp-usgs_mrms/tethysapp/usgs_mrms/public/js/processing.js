const BASE_URL = "/apps/usgs-mrms/";

const PROCESS_URLS = {
    'basin_download': BASE_URL + "do_download_basin/",
    'zarr_download': BASE_URL + "do_download_zarr/",
    'flood_alert': BASE_URL + "do_run_flood_alert/"
}

const DOWNLOAD_PROCESS_BASE_URL = BASE_URL + "basin/";
const FLOOD_ALERT_PROCESS_BASE_URL = BASE_URL + "flood-alert/";

const DOWNLOAD_PROCESS_PREVIOUS_BASE_URL = BASE_URL + "/basin/";

let csrfToken;
let state;
let gageId;
let runId;
let workers;
let processType;

const poll_interval = 3000;

function submitToRunFloodAlert() {
    const form = document.createElement("form");
    form.method = "POST";
    form.action = BASE_URL + "flood-alert/run/";

    const fields = {
        csrfmiddlewaretoken: csrfToken,
        state: state,
        run_id: runId,
        workers: workers,
    };

    for (const [name, value] of Object.entries(fields)) {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = name;
        input.value = value;
        form.appendChild(input);
    }

    document.body.appendChild(form);
    form.submit();
}

async function pollFloodAlertStatus() {
    const url = BASE_URL + "flood-alert/status/" + state.toUpperCase() + "/" + runId + "/";

    while (true) {
        let data;
        try {
            const res = await fetch(url, { method: "GET", headers: { "X-CSRFToken": csrfToken } });
            data = await res.json();
        } catch (err) {
            showError("Lost connection while checking flood alert status.");
            return;
        }

        if (data.status === "done") {
            // Submit a form to redirect to the flood alert results page
            submitToRunFloodAlert();
            return;
        }

        if (data.status === "not_found") {
            showError("Flood alert run could not be found.");
            return;
        }

        await new Promise((resolve) => setTimeout(resolve, poll_interval));
    }
}

async function pollZarrStatus() {
    const url = BASE_URL + "zarr_status/" + gageId + "/";

    while (true) {
        let data;
        try {
            const res = await fetch(url, { method: "GET", headers: { "X-CSRFToken": csrfToken } });
            data = await res.json();
        } catch (err) {
            showError("Lost connection while checking download status.");
            return;
        }

        if (data.status === "success") {
            window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/" + gageId + "/";
            return;
        }

        if (data.status === "error") {
            showError(data.message || "Download failed.");
            return;
        }

        if (data.status === "idle") {
            showError("There was an issue downloading the necessary data. Please try again.");
            return;
        }

        await new Promise((resolve) => setTimeout(resolve, poll_interval));
    }
}

async function runProcess() {
    let url;
    if (processType === "basin_download") {
        url = PROCESS_URLS.basin_download + state + "/";
    } else if (processType === "zarr_download") {
        url = PROCESS_URLS.zarr_download + state + "/" + gageId + "/";
     } else if (processType === "flood_alert") {
        url = PROCESS_URLS.flood_alert;
    }

    try {
        let res;
        let data;

        if (processType === "flood_alert") {
            const body = new FormData();
            body.append("state", state);
            body.append("run_id", runId);
            body.append("workers", workers);
            res = await fetch(url, { method: "POST", headers: { "X-CSRFToken": csrfToken }, body });
            data = await res.json();
        } else {
            res = await fetch(url, { method: "POST", headers: { "X-CSRFToken": csrfToken } });
            data = await res.json();
        }
        

        if (processType === "flood_alert") {
            // The background job has been started (or was already running).
            // Poll the status endpoint and redirect to results when it finishes,
            if (data.status === "success" || data.status === "running") {
                await pollFloodAlertStatus();
            } else {
                showError(data.message || "Flood alert generation failed.");
            }
            return;
        } else if (processType === "zarr_download") {
            if (data.status === "success") {
                // cached, no need to run download, go straight to page
                window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/" + gageId + "/";
            } else if (data.status === "running") {
                await pollZarrStatus();
            } else {
                showError('No data could be found for the specified gage ID. Try again later.');
            }
        }

        if (data.status === "success") {
            if (processType === "basin_download") {
                window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/";
            } else if (processType === "zarr_download") {
                window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/" + gageId + "/";
            }
        } else {
            if (res.status === 404) {
                if (processType === "basin_download") {
                    showError('No basin data could be found for the specified state.');
                } else if (processType === "zarr_download") {
                    showError('No data could be found for the specified gage ID. Try again later, as this data may not yet be available in the system.');
                }
            }
        }
    } catch (err) {
        if (processType === "basin_download" || processType === "zarr_download") {
            showError("Download failed");
        } else {
            showError("Flood alert generation failed.");
        }
    }
}

function loadProcessData() {
    const processData = JSON.parse(
        document.getElementById("processing-data").textContent
    );
    csrfToken = processData.csrfToken;
    state = processData.state;
    gageId = processData.gageId;
    runId = processData.runId;
    workers = processData.workers;
    processType = processData.processType;
}

function showError(message) {
    document.querySelector(".process-container").style.display = "none";
    document.querySelector(".error-message-container").style.display = "block";
    document.querySelector(".error-message").textContent = message;
}

function returnToPreviousPage() {
    if (processType === "basin_download") {
        window.location.href = DOWNLOAD_PROCESS_BASE_URL;
    } else if (processType === "zarr_download") {
        window.location.href = DOWNLOAD_PROCESS_BASE_URL + state + "/";
    } else if (processType === "flood_alert") {
        window.location.href = FLOOD_ALERT_PROCESS_BASE_URL;
    }
}
document.addEventListener("DOMContentLoaded", () => {
    loadProcessData();
    runProcess();
});