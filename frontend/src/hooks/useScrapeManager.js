import { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api/client';

const mergeCompletedBatches = (serverBatches, activeBatch) => {
  let list = Array.isArray(serverBatches) ? [...serverBatches] : [];
  if (activeBatch && (activeBatch.task_status === 'completed' || activeBatch.status === 'completed' || activeBatch.progress === 100)) {
    const activeId = activeBatch.batch_id || activeBatch.request_id;
    if (activeId) {
      const idx = list.findIndex((b) => b.batch_id === activeId || b.request_id === activeId);
      const activeEntry = {
        batch_id: activeId,
        request_id: activeBatch.request_id || activeId,
        city: activeBatch.city,
        location: activeBatch.location,
        status: 'completed',
        total_restaurants: activeBatch.total_restaurants || 0,
        completed_jobs: activeBatch.completed_jobs || activeBatch.total_restaurants || 0,
        records_count:
          activeBatch.records_count ||
          activeBatch.scraped_details ||
          activeBatch.completed_jobs ||
          activeBatch.total_restaurants ||
          0,
        completed_at: activeBatch.completed_at || new Date().toISOString(),
        csv_url: `/api/export-csv/${activeId}/`,
      };
      if (idx >= 0) {
        list[idx] = { ...list[idx], ...activeEntry };
      } else {
        list.unshift(activeEntry);
      }
    }
  }
  return list;
};

export function useScrapeManager({ user, onStatsRefresh } = {}) {
  const [currentRequestId, setCurrentRequestId] = useState(null);
  const [taskStatus, setTaskStatus] = useState('ready');
  const [progressPct, setProgressPct] = useState(0);
  const [progressTitle, setProgressTitle] = useState('Select Location to Scrape');
  const [progressStepDesc, setProgressStepDesc] = useState('Ready to start extraction pipeline...');
  const [counts, setCounts] = useState({ urls: 0, details: 0, delivery: 0 });
  const [queuedRequests, setQueuedRequests] = useState([]);
  const [completedBatches, setCompletedBatches] = useState([]);
  const [isLoadingCompleted, setIsLoadingCompleted] = useState(false);
  const [scrapeData, setScrapeData] = useState([]);
  const [tableFields, setTableFields] = useState([]);
  const [toast, setToast] = useState(null);
  const [modalConfig, setModalConfig] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isTerminating, setIsTerminating] = useState(false);

  const getStorageKey = useCallback((u) => {
    return u?.id ? `zomato_active_request_id_u${u.id}` : 'zomato_active_request_id';
  }, []);

  const completedNotifiedBatches = useRef(new Set());
  const pollTimerRef = useRef(null);
  const isInspectingHistoryRef = useRef(false);
  const currentRequestIdRef = useRef(currentRequestId);
  currentRequestIdRef.current = currentRequestId;

  const showToast = useCallback((message, type = 'info', durationMs = 5000) => {
    setToast({ message, type, id: Date.now() });
    setTimeout(() => {
      setToast((prev) => (prev?.message === message ? null : prev));
    }, durationMs);
  }, []);

  const clearToast = useCallback(() => {
    setToast(null);
  }, []);

  const closeModal = useCallback(() => {
    setModalConfig(null);
  }, []);

  const fetchCompletedBatches = useCallback(async (activeBatch = null) => {
    setIsLoadingCompleted(true);
    try {
      const res = await api.getCompletedBatches(25);
      if (res.ok && res.data && res.data.status === 'success') {
        setCompletedBatches(mergeCompletedBatches(res.data.batches || [], activeBatch));
      }
    } catch (err) {
      console.warn('Error fetching completed batches:', err);
    } finally {
      setIsLoadingCompleted(false);
    }
  }, []);

  const downloadBatchCsv = useCallback(async (batchId, fields = [], customName = null) => {
    if (!batchId) return;
    showToast('⏳ Exporting and downloading CSV sheet...', 'info', 2500);
    try {
      const filename = await api.downloadCsvBlob(batchId, fields, customName);
      showToast(`📥 Downloaded ${filename}`, 'success', 4000);
    } catch (err) {
      console.error('CSV download error:', err);
      showToast(`Failed to download CSV: ${err.message}`, 'error', 4000);
    }
  }, [showToast]);

  const triggerAutoCsvDownload = useCallback((batchId, fields = []) => {
    if (!batchId) return;
    downloadBatchCsv(batchId, fields);
  }, [downloadBatchCsv]);

  const testSlackNotification = useCallback(async () => {
    showToast('🔔 Dispatching test notification to Slack...', 'info', 3000);
    try {
      const res = await api.testSlackAlert();
      if (res.ok && res.data && res.data.status === 'success') {
        showToast('✓ Test alert delivered to Slack channel!', 'success', 5000);
      } else {
        showToast(res.data?.message || 'Slack test failed. Check webhook URL in .env', 'error', 5000);
      }
    } catch (err) {
      showToast(`Slack alert error: ${err.message}`, 'error', 5000);
    }
  }, [showToast]);

  const performReset = useCallback(() => {
    setModalConfig(null);
    isInspectingHistoryRef.current = false;
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
    setCurrentRequestId(null);
    currentRequestIdRef.current = null;
    setTaskStatus('ready');
    setProgressPct(0);
    setProgressTitle('Select Location to Scrape');
    setProgressStepDesc('Ready to start extraction pipeline...');
    setCounts({ urls: 0, details: 0, delivery: 0 });
    setScrapeData([]);
    setTableFields([]);
    setQueuedRequests([]);

    localStorage.removeItem(getStorageKey(user));
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('request_id')) {
      window.history.replaceState(null, '', window.location.pathname);
    }
    fetchCompletedBatches();
  }, [fetchCompletedBatches, getStorageKey, user]);

  // Account switch detection: if active user changes, immediately clear state & reload for new user
  const prevUserIdRef = useRef(user?.id);
  useEffect(() => {
    if (prevUserIdRef.current !== undefined && prevUserIdRef.current !== user?.id) {
      prevUserIdRef.current = user?.id;
      performReset();
    }
  }, [user?.id, performReset]);

  const resetSession = useCallback(() => {
    const isRunning = ['extracting_urls', 'processing_restaurants', 'fetching_details'].includes(taskStatus);

    if (isRunning) {
      setModalConfig({
        isOpen: true,
        title: 'Reset Active Session?',
        message: 'An extraction pipeline is actively running. Resetting session will detach live tracking from this view.',
        details: [
          'Background workers will continue running until completed.',
          'You can inspect or download the CSV from Completed History anytime.',
        ],
        type: 'confirm',
        confirmText: 'Reset View',
        cancelText: 'Stay on Live View',
        onConfirm: () => {
          performReset();
          showToast('Session reset. Ready for new search.', 'info', 3000);
        },
        onCancel: () => setModalConfig(null),
      });
    } else {
      performReset();
      showToast('Session reset. Ready for new search.', 'info', 3000);
    }
  }, [performReset, showToast, taskStatus]);

  const switchBatch = useCallback((batchId, title = '', shouldTriggerBackend = false) => {
    if (!batchId) return;
    isInspectingHistoryRef.current = false;
    setCurrentRequestId(batchId);
    currentRequestIdRef.current = batchId;
    // CRITICAL: Instantly clear previous queue's data to eliminate visual glitch
    setScrapeData([]);
    setProgressPct(0);
    if (title) setProgressTitle(title);

    localStorage.setItem(getStorageKey(user), batchId);
    window.history.replaceState(null, '', `?request_id=${encodeURIComponent(batchId)}`);

    // Remove from queue
    setQueuedRequests((prev) => prev.filter((q) => q.batch_id !== batchId && q.request_id !== batchId));

    // Only start if explicitly asked (e.g. promoting a waiting queued batch)
    // NEVER when switching to a newly started batch that startScrape already dispatched!
    if (shouldTriggerBackend) {
      api.startQueuedBatch(batchId).catch(() => {});
    }
  }, [getStorageKey, user]);

  const inspectCompletedBatch = useCallback(async (batch) => {
    if (!batch) return;
    const batchId = batch.batch_id || batch.request_id;
    isInspectingHistoryRef.current = true;
    setCurrentRequestId(batchId);
    currentRequestIdRef.current = batchId;
    setTaskStatus(batch.status || 'completed');
    setProgressPct(100);
    setProgressTitle(`${batch.city} (${batch.location})`);
    const savedCount = batch.records_count ?? batch.completed_jobs ?? 0;
    setProgressStepDesc(`Completed extraction: ${savedCount} records saved`);
    setCounts({
      urls: batch.total_restaurants || 0,
      details: batch.completed_jobs || 0,
      delivery: savedCount,
    });
    setScrapeData([]);
    localStorage.setItem(getStorageKey(user), batchId);
    window.history.replaceState(null, '', `?request_id=${encodeURIComponent(batchId)}`);

    try {
      const res = await api.getScrapeData(batchId);
      if (res.status === 403 || res.data?.code === 'permission_denied') {
        showToast('Access Denied: You do not have permission to view that extraction batch.', 'error', 5000);
        performReset();
        return;
      }
      if (res.ok && res.data && res.data.status === 'success') {
        setScrapeData(res.data.data || []);
        if (res.data.fields && res.data.fields.length > 0) {
          setTableFields(res.data.fields);
        }
      }
    } catch (e) {
      console.warn('Error inspecting batch:', e);
    }
    showToast(`Loaded ${savedCount} records for ${batch.city} (${batch.location})`, 'info', 3000);
  }, [getStorageKey, performReset, showToast, user]);

  const fetchScrapedData = useCallback(async (reqId, fields = []) => {
    if (!reqId) return;
    try {
      const res = await api.getScrapeData(reqId, fields);
      if (res.status === 403 || res.data?.code === 'permission_denied') {
        showToast('Access Denied: You do not have permission to view this extraction data.', 'error', 5000);
        performReset();
        return;
      }
      // Guard against stale async responses overwriting current batch
      if (reqId !== currentRequestIdRef.current) return;
      if (res.ok && res.data && res.data.status === 'success') {
        setScrapeData(res.data.data || []);
        if (res.data.fields && res.data.fields.length > 0) {
          setTableFields(res.data.fields);
        }
      }
    } catch (err) {
      console.warn('Error fetching preview data:', err);
    }
  }, [performReset, showToast]);

  const syncQueuedBatches = useCallback((backendList) => {
    if (!Array.isArray(backendList)) return;
    const currentId = currentRequestIdRef.current;
    if (backendList.length === 0) {
      setQueuedRequests([]);
      return;
    }
    const filtered = backendList.filter((b) => {
      const bId = b.batch_id || b.request_id;
      return bId !== currentId;
    });
    setQueuedRequests(filtered);
  }, []);

  const tickAutoRefresh = useCallback(async () => {
    const reqId = currentRequestIdRef.current;
    if (!reqId) return;

    try {
      const res = await api.getScrapeStatus(reqId);

      // Handle 403 Forbidden (cross-user or unassigned batch access attempt)
      if (res.status === 403 || res.data?.code === 'permission_denied') {
        console.warn(`[Pipeline Sync] Access denied for batch ${reqId} (403). Purging.`);
        localStorage.removeItem(getStorageKey(user));
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get('request_id') === reqId) {
          window.history.replaceState(null, '', window.location.pathname);
        }
        showToast('Access Denied: You do not have permission to view that extraction request.', 'error', 5000);
        performReset();
        return;
      }

      // Handle 404 (deleted/missing batch)
      if (res.status === 404) {
        console.warn(`[Pipeline Sync] Batch ${reqId} not found (404). Recovering.`);
        const deadId = reqId;
        setCurrentRequestId(null);
        currentRequestIdRef.current = null;
        localStorage.removeItem(getStorageKey(user));

        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.get('request_id') === deadId) {
          window.history.replaceState(null, '', window.location.pathname);
        }

        setQueuedRequests((prev) => prev.filter((q) => q.batch_id !== deadId && q.request_id !== deadId));

        if (res.data?.active_running_batch) {
          const active = res.data.active_running_batch;
          showToast(`Active batch: ${active.city} (${active.location})`, 'info', 4000);
          switchBatch(active.batch_id, `${active.city} (${active.location})`);
          return;
        }

        let activeResp = null;
        try {
          activeResp = await api.getScrapeStatus('active');
        } catch (e) {
          // ignore
        }

        if (activeResp?.ok && activeResp?.data?.active_running_batch) {
          const active = activeResp.data.active_running_batch;
          if (active.batch_id !== deadId) {
            showToast(`Active batch: ${active.city} (${active.location})`, 'info', 4000);
            switchBatch(active.batch_id, `${active.city} (${active.location})`);
            return;
          }
        }

        // Check if other queued batch exists
        setQueuedRequests((prev) => {
          if (prev.length > 0) {
            const next = prev[0];
            showToast(`Starting next in queue: ${next.city} (${next.location})...`, 'info', 4000);
            switchBatch(next.batch_id || next.request_id, `${next.city} (${next.location})`, true);
            return prev.slice(1);
          }
          resetSession();
          return [];
        });
        return;
      }

      if (res.ok && res.data && res.data.status === 'success') {
        const data = res.data;

        // Auto-switch if current tracked batch is terminated/completed/queued and another is actively executing
        // (Do not auto-switch if user is deliberately inspecting a historical completed batch)
        if (
          !isInspectingHistoryRef.current &&
          (data.task_status === 'terminated' || data.task_status === 'completed' || data.task_status === 'queued') &&
          data.active_running_batch &&
          data.active_running_batch.batch_id !== reqId
        ) {
          const active = data.active_running_batch;
          switchBatch(active.batch_id, `${active.city} (${active.location})`);
          return;
        }

        setTaskStatus(data.task_status);
        const pct = Math.min(100, Math.max(0, parseInt(data.progress, 10) || 0));
        setProgressPct(pct);

        const done = data.completed_jobs || data.scraped_details || 0;
        const total = data.total_restaurants || 0;
        setProgressStepDesc(`Progress: ${pct}% | Restaurants: ${done} / ${total} completed`);

        setCounts({
          urls: data.total_restaurants || 0,
          details: data.scraped_details || 0,
          delivery: data.parsed_delivery || 0,
        });

        if (Array.isArray(data.queued_batches)) {
          syncQueuedBatches(data.queued_batches);
        }

        const isNowCompleted = data.task_status === 'completed' || data.progress === 100;

        // Live update completed batches list from status response
        if (Array.isArray(data.completed_batches) && data.completed_batches.length > 0) {
          setCompletedBatches(mergeCompletedBatches(data.completed_batches, isNowCompleted ? data : null));
        }

        // Fetch data table preview strictly for this batch
        await fetchScrapedData(reqId, data.fields_selected);
        if (onStatsRefresh) onStatsRefresh();

        // Check completion
        if (isNowCompleted) {
          setProgressPct(100);

          // Immediately ensure this completed batch is displayed in completed history
          setCompletedBatches((prev) => mergeCompletedBatches(prev, data));

          fetchCompletedBatches(data);

          if (!completedNotifiedBatches.current.has(reqId)) {
            completedNotifiedBatches.current.add(reqId);
            showToast(`✓ Extraction completed for ${data.city} (${data.location})! Click "Export CSV" to download records.`, 'success', 6000);
          }

          // Advance to next request after settle
          if (data.active_running_batch && data.active_running_batch.batch_id !== reqId) {
            const active = data.active_running_batch;
            showToast(`Next active request: ${active.city} (${active.location})...`, 'info', 4000);
            setTimeout(() => {
              switchBatch(active.batch_id, `${active.city} (${active.location})`);
            }, 2000);
          } else {
            const nextBatch = data.queued_batches?.[0];
            if (nextBatch) {
              setQueuedRequests((prev) => prev.filter((q) => q.batch_id !== nextBatch.batch_id && q.request_id !== nextBatch.request_id));
              showToast(`Auto-starting next request: ${nextBatch.city} (${nextBatch.location})...`, 'success', 4000);
              setTimeout(() => {
                switchBatch(nextBatch.batch_id || nextBatch.request_id, `${nextBatch.city} (${nextBatch.location})`, true);
              }, 2000);
            } else {
              // No more queued batches: empty the queue completely!
              setQueuedRequests([]);
            }
          }
        } else if (data.task_status === 'terminated') {
          setProgressStepDesc(data.error_message || 'Extraction process terminated by user.');
          fetchCompletedBatches();
          if (data.active_running_batch && data.active_running_batch.batch_id !== reqId) {
            const active = data.active_running_batch;
            showToast(`Process stopped. Moving to active request: ${active.city} (${active.location})...`, 'info', 3000);
            setTimeout(() => {
              switchBatch(active.batch_id, `${active.city} (${active.location})`);
            }, 1200);
          } else if (data.queued_batches?.[0]) {
            const next = data.queued_batches[0];
            setQueuedRequests((prev) => prev.filter((q) => q.batch_id !== next.batch_id && q.request_id !== next.request_id));
            showToast(`Process stopped. Moving to next in queue: ${next.city} (${next.location})...`, 'info', 3000);
            setTimeout(() => {
              switchBatch(next.batch_id || next.request_id, `${next.city} (${next.location})`, true);
            }, 1200);
          } else {
            setQueuedRequests([]);
          }
        } else if (data.task_status === 'failed') {
          setProgressStepDesc(data.error_message || 'Extraction task failed.');
          fetchCompletedBatches();
        }
      }
    } catch (err) {
      console.warn('Real-time sync error:', err);
    }
  }, [fetchCompletedBatches, fetchScrapedData, onStatsRefresh, resetSession, showToast, switchBatch, syncQueuedBatches, triggerAutoCsvDownload]);

  // Fast reactive polling: 1.2s when running, 3.5s when idle
  useEffect(() => {
    if (!currentRequestId) {
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
      return;
    }

    const isRunning = ['extracting_urls', 'processing_restaurants', 'fetching_details', 'queued', 'pending'].includes(taskStatus);
    const intervalMs = isRunning ? 1200 : 3500;

    tickAutoRefresh();
    pollTimerRef.current = setInterval(tickAutoRefresh, intervalMs);

    return () => {
      if (pollTimerRef.current) {
        clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };
  }, [currentRequestId, taskStatus, tickAutoRefresh]);

  // Continuous background polling: keeps completed history & queue always fresh and up-to-date
  useEffect(() => {
    const pollCompleted = async () => {
      try {
        const res = await api.getCompletedBatches(25);
        if (res.ok && res.data?.status === 'success' && Array.isArray(res.data.batches)) {
          setCompletedBatches(res.data.batches);
        }
        if (onStatsRefresh) onStatsRefresh();
      } catch (e) {
        // ignore
      }
    };

    const idlePoll = setInterval(pollCompleted, 3500);
    return () => clearInterval(idlePoll);
  }, [onStatsRefresh]);

  // Initial mount load
  useEffect(() => {
    async function initMount() {
      fetchCompletedBatches();

      const urlParams = new URLSearchParams(window.location.search);
      const paramId = urlParams.get('request_id');
      const savedId = localStorage.getItem(getStorageKey(user));

      let targetId = paramId || savedId;
      let targetTitle = null;

      try {
        const queryUrl = paramId ? paramId : 'active';
        const res = await api.getScrapeStatus(queryUrl);

        if (res.status === 403 || res.data?.code === 'permission_denied') {
          console.warn('[Initial Check] Forbidden batch in URL or session (403). Purging.');
          localStorage.removeItem(getStorageKey(user));
          if (paramId) {
            window.history.replaceState(null, '', window.location.pathname);
          }
          targetId = null;
          showToast('Access Denied: You do not have permission to view that extraction request.', 'error', 5000);
        } else if (res.ok && res.data && res.data.status === 'success') {
          const d = res.data;
          if (Array.isArray(d.queued_batches)) {
            syncQueuedBatches(d.queued_batches);
          }
          if (d.active_running_batch) {
            targetId = d.active_running_batch.batch_id;
            targetTitle = `${d.active_running_batch.city} (${d.active_running_batch.location})`;
          } else if (d.batch_id) {
            targetId = d.batch_id;
            targetTitle = `${d.city} (${d.location})`;
          }
        }
      } catch (err) {
        console.warn('Initial status check error:', err);
      }

      if (targetId) {
        setCurrentRequestId(targetId);
        currentRequestIdRef.current = targetId;
        if (targetTitle) setProgressTitle(targetTitle);
        localStorage.setItem(getStorageKey(user), targetId);
        window.history.replaceState(null, '', `?request_id=${encodeURIComponent(targetId)}`);
      }
    }

    initMount();
  }, [fetchCompletedBatches, syncQueuedBatches, getStorageKey, user, showToast]);

  const startPipeline = useCallback(
    async ({ city, location, selectedFields, requestId }) => {
      if (!city?.trim()) {
        setModalConfig({
          isOpen: true,
          title: 'Target City Required',
          message: 'Please enter a target city name before initiating the extraction pipeline.',
          type: 'alert',
          confirmText: 'Acknowledge',
          onConfirm: () => setModalConfig(null),
          onCancel: () => setModalConfig(null),
        });
        return;
      }

      setIsSubmitting(true);
      isInspectingHistoryRef.current = false;
      try {
        const res = await api.startScrape({
          city: city.trim(),
          location,
          selectedFields,
          requestId: requestId || (typeof location === 'object' ? location?.request_id : null),
        });

        if (res.ok && res.data && (res.data.status === 'cached' || res.data.status === 'queued')) {
          const newReqId = res.data.request_id || res.data.batch_id;
          const newCity = res.data.city || city;
          const locTitle = typeof location === 'string' ? location : location?.entity_title || 'Location';
          const newLocation = res.data.location || locTitle;

          const isCurrentActive =
            currentRequestId &&
            (taskStatus === 'extracting_urls' ||
              taskStatus === 'processing_restaurants' ||
              taskStatus === 'fetching_details' ||
              (taskStatus !== 'completed' && taskStatus !== 'failed' && progressPct < 100));

          if (isCurrentActive && newReqId !== currentRequestId) {
            const newQueuedItem = {
              request_id: newReqId,
              batch_id: res.data.batch_id || newReqId,
              city: newCity,
              location: newLocation,
              status: 'queued',
              created_at: new Date().toISOString(),
              time_str: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            };

            setQueuedRequests((prev) => {
              if (prev.some((q) => q.batch_id === newQueuedItem.batch_id || q.request_id === newQueuedItem.request_id)) {
                return prev;
              }
              return [...prev, newQueuedItem];
            });

            showToast(
              `Added ${newCity} (${newLocation}) to queue.`,
              'info',
              5000
            );
          } else {
            // New batch is directly active; do NOT fire startQueuedBatch
            switchBatch(newReqId, `${newCity} (${newLocation})`, false);
          }
        } else {
          setModalConfig({
            isOpen: true,
            title: 'Pipeline Initialization Notice',
            message: res.data?.message || 'Failed to start scraping pipeline.',
            type: 'alert',
            confirmText: 'Close',
            onConfirm: () => setModalConfig(null),
            onCancel: () => setModalConfig(null),
          });
        }
      } catch (err) {
        console.error('Error starting scrape:', err);
        setModalConfig({
          isOpen: true,
          title: 'Connection Error',
          message: 'Failed to communicate with the scraping engine. Check server logs.',
          details: [err.message || 'Network communication error'],
          type: 'alert',
          confirmText: 'Close',
          onConfirm: () => setModalConfig(null),
          onCancel: () => setModalConfig(null),
        });
      } finally {
        setIsSubmitting(false);
      }
    },
    [currentRequestId, progressPct, showToast, switchBatch, taskStatus]
  );

  const terminateProcess = useCallback(async () => {
    if (!currentRequestId) return;

    setModalConfig({
      isOpen: true,
      title: 'Abort Extraction Pipeline?',
      message: 'Are you sure you want to stop the currently running extraction pipeline?',
      details: [
        `Batch ID: ${currentRequestId}`,
        'Active workers will cleanly finish their current request and halt.',
        'Remaining queued jobs for this batch will be cancelled.',
        'Extracted records are preserved in PostgreSQL and ready for CSV download.',
      ],
      type: 'confirm',
      confirmText: 'Yes, Abort Pipeline',
      cancelText: 'Keep Running',
      onCancel: () => setModalConfig(null),
      onConfirm: async () => {
        setModalConfig(null);
        setIsTerminating(true);
        showToast('Stopping process...', 'warning', 2500);

        try {
          const res = await api.terminateBatch(currentRequestId);
          if (res.ok && res.data && res.data.status === 'success') {
            setTaskStatus('terminated');
            setProgressStepDesc('Process stopped by user. Scraped records preserved.');
            showToast('Extraction stopped.', 'info', 3500);

            await fetchScrapedData(currentRequestId);
            if (onStatsRefresh) onStatsRefresh();
            fetchCompletedBatches();

            if (res.data.next_batch) {
              const next = res.data.next_batch;
              showToast(`Starting next in queue: ${next.city} (${next.location})...`, 'info', 3000);
              setTimeout(() => {
                switchBatch(next.batch_id || next.request_id, `${next.city} (${next.location})`, true);
              }, 1000);
            } else if (queuedRequests.length > 0) {
              const next = queuedRequests[0];
              setQueuedRequests((prev) => prev.slice(1));
              showToast(`Starting next in queue: ${next.city} (${next.location})...`, 'info', 3000);
              setTimeout(() => {
                switchBatch(next.batch_id || next.request_id, `${next.city} (${next.location})`, true);
              }, 1000);
            } else {
              setQueuedRequests([]);
            }
          } else {
            showToast(`Failed to stop process.`, 'error', 3500);
          }
        } catch (err) {
          showToast(`Error: ${err.message}`, 'error', 3500);
        } finally {
          setIsTerminating(false);
        }
      },
    });
  }, [currentRequestId, fetchCompletedBatches, fetchScrapedData, onStatsRefresh, queuedRequests, showToast, switchBatch]);

  const cancelQueuedBatch = useCallback(
    async (batchId, city, location) => {
      if (!batchId) return;

      setModalConfig({
        isOpen: true,
        title: 'Cancel Queued Extraction?',
        message: `Remove "${city || 'Selected'}" (${location || 'All'}) from the execution queue?`,
        details: [
          `Batch ID: ${batchId}`,
          'Pending extraction tasks will be purged from Redis and PostgreSQL.',
        ],
        type: 'confirm',
        confirmText: 'Remove Task',
        cancelText: 'Keep in Queue',
        onCancel: () => setModalConfig(null),
        onConfirm: async () => {
          setModalConfig(null);
          try {
            const res = await api.deleteBatch(batchId);
            if (res.ok && res.data && res.data.status === 'success') {
              setQueuedRequests((prev) => prev.filter((q) => q.batch_id !== batchId && q.request_id !== batchId));
              showToast(`Removed ${city || 'Task'} from queue.`, 'success', 3500);
              if (onStatsRefresh) onStatsRefresh();

              if (currentRequestId === batchId) {
                resetSession();
              }
            } else {
              showToast(`Failed to remove: ${res.data?.message || 'Error'}`, 'error', 3500);
            }
          } catch (err) {
            showToast(`Cancel error: ${err.message}`, 'error', 3500);
          }
        },
      });
    },
    [currentRequestId, onStatsRefresh, resetSession, showToast]
  );

  return {
    currentRequestId,
    taskStatus,
    progressPct,
    progressTitle,
    progressStepDesc,
    counts,
    queuedRequests,
    completedBatches,
    isLoadingCompleted,
    scrapeData,
    tableFields,
    toast,
    modalConfig,
    setModalConfig,
    closeModal,
    isSubmitting,
    isTerminating,
    showToast,
    clearToast,
    startPipeline,
    terminateProcess,
    cancelQueuedBatch,
    resetSession,
    switchBatch,
    inspectCompletedBatch,
    downloadBatchCsv,
    testSlackNotification,
    fetchCompletedBatches,
    triggerAutoCsvDownload,
  };
}
