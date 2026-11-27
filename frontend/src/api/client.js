
/**
 * API client service for Zomato Scraper Django backend.
 */

const API_BASE = '';

/**
 * Retrieves a cookie value by name.
 * Used to retrieve Django's CSRF token.
 */
function getCookie(name) {
  const cookies = document.cookie.split(';');

  for (const cookie of cookies) {
    const [key, ...value] = cookie.trim().split('=');

    if (key === name) {
      return decodeURIComponent(value.join('='));
    }
  }

  return null;
}

async function fetchJson(url, options = {}) {
  const method = (options.method || 'GET').toUpperCase();

  const headers = {
    'Content-Type': 'application/json',
    ...options.headers,
  };

  // Attach CSRF token to unsafe HTTP methods
  if (!['GET', 'HEAD', 'OPTIONS', 'TRACE'].includes(method)) {
    const csrfToken = getCookie('csrftoken');

    if (csrfToken) {
      headers['X-CSRFToken'] = csrfToken;
    }
  }

  const resp = await fetch(url, {
    ...options,
    credentials: 'include',
    headers,
  });

  const contentType = resp.headers.get('content-type') || '';

  let data = null;

  if (contentType.includes('application/json')) {
    data = await resp.json();
  }

  return {
    ok: resp.ok,
    status: resp.status,
    data,
  };
}

export const api = {
  /**
   * Discovers locations for a city with preloaded HTML fallback.
   */
  async searchLocations(query) {
    return fetchJson(`${API_BASE}/api/search-locations/?query=${encodeURIComponent(query)}`);
  },

  /**
   * Retrieves collection counts and totals directly from PostgreSQL.
   */
  async getCollectionStats() {
    return fetchJson(`${API_BASE}/api/collection-stats/`);
  },

  /**
   * Returns schema fields (default and customizable) for extraction.
   */
  async getSchemaFields() {
    return fetchJson(`${API_BASE}/api/get-schema-fields/`);
  },

  /**
   * Starts a new 100% LIVE extraction pipeline.
   */
  async startScrape({ city, location, selectedFields, requestId }) {
    return fetchJson(`${API_BASE}/api/start-scrape/`, {
      method: 'POST',
      body: JSON.stringify({
        city,
        location,
        selected_fields: selectedFields,
        request_id: requestId,
      }),
    });
  },

  /**
   * Polls live extraction progress, stage counts, and active batch status.
   */
  async getScrapeStatus(requestId) {
    return fetchJson(`${API_BASE}/api/scrape-status/${encodeURIComponent(requestId)}/`);
  },

  /**
   * Fetches preview extracted records for a specific batch.
   */
  async getScrapeData(requestId, fields = [], limit = 200) {
    const fieldsParam = Array.isArray(fields) && fields.length > 0 ? `&fields=${encodeURIComponent(fields.join(','))}` : '';
    return fetchJson(`${API_BASE}/api/scrape-data/${encodeURIComponent(requestId)}/?limit=${limit}${fieldsParam}`);
  },

  /**
   * Cancels and deletes a queued or active scrape request from Redis and PostgreSQL.
   */
  async deleteBatch(requestId) {
    return fetchJson(`${API_BASE}/api/delete-batch/${encodeURIComponent(requestId)}/`, {
      method: 'POST',
    });
  },

  /**
   * Terminates active scraping process, empties Redis queue, and advances to next queued request.
   */
  async terminateBatch(requestId) {
    return fetchJson(`${API_BASE}/api/terminate-batch/${encodeURIComponent(requestId)}/`, {
      method: 'POST',
    });
  },

  /**
   * Explicitly triggers execution of a queued batch in Redis.
   */
  async startQueuedBatch(requestId) {
    return fetchJson(`${API_BASE}/api/start-queued-batch/${encodeURIComponent(requestId)}/`, {
      method: 'POST',
    });
  },

  /**
   * Fetches list of recently completed/terminated batches with records counts.
   */
  async getCompletedBatches(limit = 25) {
    return fetchJson(`${API_BASE}/api/completed-batches/?limit=${limit}`);
  },

  /**
   * Triggers a live test alert to Slack incoming webhook.
   */
  async testSlackAlert() {
    return fetchJson(`${API_BASE}/api/test-slack-alert/`, {
      method: 'POST',
    });
  },

  /**
   * Generates CSV download URL.
   */
  getExportCsvUrl(requestId, fields = []) {
    const fieldsParam = Array.isArray(fields) && fields.length > 0 ? `?fields=${encodeURIComponent(fields.join(','))}` : '';
    return `${API_BASE}/api/export-csv/${encodeURIComponent(requestId)}/${fieldsParam}`;
  },

  /**
   * Robust browser download of CSV data via Blob API.
   * Handles errors, custom filenames, and prevents page navigation or popup blocking.
   */
  async downloadCsvBlob(requestId, fields = [], fallbackFilename = null) {
    const url = this.getExportCsvUrl(requestId, fields);
    const resp = await fetch(url, {
      credentials: 'include',
    });

    if (!resp.ok) {
      throw new Error(`Server returned HTTP ${resp.status} during CSV export`);
    }

    // Extract filename from Content-Disposition if present
    let filename = fallbackFilename;
    const disposition = resp.headers.get('content-disposition');
    if (disposition && disposition.includes('filename=')) {
      const match = disposition.match(/filename=["']?([^"';]+)["']?/i);
      if (match && match[1]) {
        filename = match[1].trim();
      }
    }

    if (!filename) {
      filename = `zomato_export_${String(requestId).substring(0, 8)}.csv`;
    }

    const blob = await resp.blob();
    const blobUrl = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = blobUrl;
    link.download = filename;
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();

    setTimeout(() => {
      try {
        document.body.removeChild(link);
        window.URL.revokeObjectURL(blobUrl);
      } catch (e) {
        // ignore
      }
    }, 1500);

    return filename;
  },

  /**
   * Retrieves Google OAuth Client configuration (client_id).
   */
  async getGoogleAuthConfig() {
    return fetchJson(`${API_BASE}/api/auth/google/config/`);
  },

  /**
   * Submits Google ID Token credential to backend for validation and session creation.
   */
  async sendGoogleCredential(credential) {
    return fetchJson(`${API_BASE}/api/auth/google/`, {
      method: 'POST',
      body: JSON.stringify({ credential }),
    });
  },

  /**
   * Fetches currently authenticated user details.
   */
  async getCurrentUser() {
    return fetchJson(`${API_BASE}/api/auth/me/`);
  },
  async initializeCsrf() {
    return fetchJson(`${API_BASE}/api/csrf/`);
  },

  /**
   * Terminates active session.
   */
  async logout() {
    return fetchJson(`${API_BASE}/api/auth/logout/`, {
      method: 'POST',
    });
  },
};
