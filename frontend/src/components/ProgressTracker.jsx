import React, { useState, useEffect } from 'react';
import { Square, Copy, Check, Compass, Store, Database, Download, Clock, CheckCircle2 } from 'lucide-react';
import { ToastBanner } from './ToastBanner';
import { QueuedRequestsList } from './QueuedRequestsList';
import { CompletedBatchesList } from './CompletedBatchesList';

export function ProgressTracker({
  currentRequestId,
  taskStatus,
  progressPct,
  progressTitle,
  progressStepDesc,
  counts,
  queuedRequests,
  completedBatches,
  onInspectBatch,
  onDownloadCsv,
  onRefreshCompleted,
  isLoadingCompleted,
  toast,
  onDismissToast,
  onTerminate,
  isTerminating,
  onCancelQueuedBatch,
}) {
  const [copied, setCopied] = React.useState(false);
  const [activeTab, setActiveTab] = useState('queue');

  const isCompleted = taskStatus === 'completed' || progressPct === 100;

  // Automatically switch tab: if queued items exist, show queue; when completed, show completed history
  useEffect(() => {
    if (queuedRequests && queuedRequests.length > 0) {
      setActiveTab('queue');
    } else if (isCompleted) {
      setActiveTab('completed');
    }
  }, [queuedRequests?.length, isCompleted]);

  const statusMap = {
    ready: {
      text: 'SYSTEM READY',
      class: 'bg-zinc-900 text-zinc-300 border-2 border-zinc-700',
      dot: 'bg-zinc-400',
    },
    pending: {
      text: 'QUEUED',
      class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
      dot: 'bg-emerald-400 animate-pulse',
    },
    queued: {
      text: 'QUEUED',
      class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
      dot: 'bg-emerald-400 animate-pulse',
    },
    extracting_urls: {
      text: 'DISCOVERING URLS',
      class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
      dot: 'bg-emerald-400 animate-pulse',
    },
    processing_restaurants: {
      text: 'PROCESSING RECORDS',
      class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
      dot: 'bg-emerald-400 animate-pulse',
    },
    fetching_details: {
      text: 'FETCHING DETAILS',
      class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
      dot: 'bg-emerald-400 animate-pulse',
    },
    terminated: {
      text: 'TERMINATED',
      class: 'bg-zinc-900 text-zinc-300 border-2 border-zinc-700',
      dot: 'bg-zinc-500',
    },
    completed: {
      text: 'COMPLETED',
      class: 'bg-emerald-950/90 text-emerald-300 border-2 border-emerald-500',
      dot: 'bg-emerald-400',
    },
    failed: {
      text: 'FAILED',
      class: 'bg-zinc-900 text-zinc-400 border-2 border-zinc-700',
      dot: 'bg-zinc-500',
    },
  };

  const meta = statusMap[taskStatus] || {
    text: 'ACTIVE',
    class: 'bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50',
    dot: 'bg-emerald-400 animate-pulse',
  };

  const isRunning = ['extracting_urls', 'processing_restaurants', 'fetching_details'].includes(
    taskStatus
  );

  const handleCopyId = () => {
    if (!currentRequestId) return;
    navigator.clipboard.writeText(currentRequestId);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="bg-[#0e1422] rounded-xl border-2 border-zinc-700 shadow-xl p-5 space-y-4">
      {/* Precision Header with Status, Request ID, Terminate & Quick Download */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between border-b-2 border-zinc-700 pb-3.5 gap-2.5">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <span
              className={`px-2 py-0.5 font-mono text-[10px] font-semibold rounded-md flex items-center gap-1.5 ${meta.class}`}
            >
              <span className={`w-1.5 h-1.5 rounded-full ${meta.dot}`}></span>
              <span>{meta.text}</span>
            </span>
            <h3 className="text-xs font-bold text-white truncate max-w-xs">{progressTitle}</h3>
          </div>
          <div className="flex items-center gap-1.5 text-[11px] font-mono text-zinc-400">
            <span>BATCH //</span>
            <span className="text-zinc-200 font-semibold bg-zinc-900 border border-zinc-700 px-1.5 py-0.5 rounded">
              {currentRequestId ? `${currentRequestId.substring(0, 18)}...` : 'NONE'}
            </span>
            {currentRequestId && (
              <button
                type="button"
                onClick={handleCopyId}
                title="Copy Batch ID"
                className="text-zinc-400 hover:text-emerald-400 transition cursor-pointer p-0.5"
              >
                {copied ? <Check className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
              </button>
            )}
          </div>
        </div>

        {/* Action Controls: Stop Process OR Download CSV */}
        <div className="flex items-center gap-2">
          {isRunning && (
            <button
              onClick={onTerminate}
              disabled={isTerminating}
              type="button"
              className="px-3 py-1.5 font-mono text-xs font-semibold rounded-lg border-2 border-zinc-600 bg-zinc-900 hover:bg-zinc-800 hover:border-zinc-400 text-zinc-200 transition flex items-center gap-1.5 cursor-pointer active:scale-95 disabled:opacity-50"
            >
              <Square className="w-3 h-3 fill-current text-zinc-400" />
              <span>{isTerminating ? 'STOPPING...' : 'ABORT PIPELINE'}</span>
            </button>
          )}

          {isCompleted && currentRequestId && onDownloadCsv && (
            <button
              type="button"
              onClick={() => onDownloadCsv(currentRequestId)}
              className="px-3.5 py-1.5 font-mono text-xs font-bold rounded-lg border-2 border-emerald-400 bg-emerald-600 hover:bg-emerald-500 text-black transition flex items-center gap-1.5 shadow-[0_0_15px_rgba(16,185,129,0.35)] active:scale-95 cursor-pointer"
            >
              <Download className="w-3.5 h-3.5" />
              <span>DOWNLOAD CSV</span>
            </button>
          )}
        </div>
      </div>

      {/* High-Visibility Progress Bar Box with Distinct Borders */}
      <div className="p-3 rounded-xl border-2 border-zinc-700 bg-[#0a0e17] space-y-2 shadow-inner">
        <div className="flex justify-between items-center text-xs">
          <span className="font-mono text-[11px] text-zinc-300 font-medium truncate pr-2">
            {progressStepDesc}
          </span>
          <span className="font-mono font-bold text-emerald-400 text-xs shrink-0 px-2 py-0.5 rounded bg-zinc-900 border border-zinc-700">
            {progressPct}%
          </span>
        </div>
        <div className="w-full bg-[#070a10] rounded-full h-3.5 overflow-hidden border-2 border-zinc-600 p-[2px]">
          <div
            style={{ width: `${progressPct}%` }}
            className="bg-gradient-to-r from-emerald-500 to-emerald-400 h-full rounded-full transition-all duration-300 shadow-[0_0_12px_rgba(16,185,129,0.8)] border border-emerald-300/40"
          ></div>
        </div>
      </div>

      {/* 3-Step Pipeline Telemetry Stages with Visible Borders */}
      <div className="grid grid-cols-3 gap-2.5 pt-1">
        <div className="p-3 rounded-xl border-2 border-zinc-700 bg-[#0a0e17] hover:border-emerald-500/50 transition flex items-center gap-2.5 shadow-md">
          <div className="w-8 h-8 rounded-lg bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center shrink-0">
            <Compass className="w-4 h-4" />
          </div>
          <div className="min-w-0">
            <p className="text-[10px] font-mono text-zinc-400 font-semibold uppercase truncate">
              STAGE 01: URLs
            </p>
            <p className="font-mono font-bold text-white text-sm">{counts?.urls || 0}</p>
          </div>
        </div>

        <div className="p-3 rounded-xl border-2 border-zinc-700 bg-[#0a0e17] hover:border-emerald-500/50 transition flex items-center gap-2.5 shadow-md">
          <div className="w-8 h-8 rounded-lg bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center shrink-0">
            <Store className="w-4 h-4" />
          </div>
          <div className="min-w-0">
            <p className="text-[10px] font-mono text-zinc-400 font-semibold uppercase truncate">
              STAGE 02: Specs
            </p>
            <p className="font-mono font-bold text-white text-sm">{counts?.details || 0}</p>
          </div>
        </div>

        <div className="p-3 rounded-xl border-2 border-zinc-700 bg-[#0a0e17] hover:border-emerald-500/50 transition flex items-center gap-2.5 shadow-md">
          <div className="w-8 h-8 rounded-lg bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center shrink-0">
            <Database className="w-4 h-4" />
          </div>
          <div className="min-w-0">
            <p className="text-[10px] font-mono text-zinc-400 font-semibold uppercase truncate">
              STAGE 03: Saved DB
            </p>
            <p className="font-mono font-bold text-white text-sm">{counts?.delivery || 0}</p>
          </div>
        </div>
      </div>

      {/* Notification Toast */}
      <ToastBanner toast={toast} onDismiss={onDismissToast} />

      {/* Queue vs Completed History Tabs with Crisp High-Contrast Borders */}
      <div className="pt-2">
        <div className="flex items-center space-x-2 border-b-2 border-zinc-700 pb-2.5 mb-2">
          <button
            type="button"
            onClick={() => setActiveTab('queue')}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold font-mono transition flex items-center gap-1.5 cursor-pointer ${
              activeTab === 'queue'
                ? 'bg-emerald-950/70 text-emerald-300 border-2 border-emerald-500 shadow-xs'
                : 'text-zinc-300 hover:text-white hover:bg-zinc-900 border-2 border-zinc-700'
            }`}
          >
            <Clock className="w-3.5 h-3.5" />
            <span>Execution Queue</span>
            <span className="text-[10px] font-bold px-1.5 py-0.2 rounded-full bg-zinc-800 border border-zinc-600 text-zinc-200">
              {queuedRequests?.length || 0}
            </span>
          </button>

          <button
            type="button"
            onClick={() => setActiveTab('completed')}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold font-mono transition flex items-center gap-1.5 cursor-pointer ${
              activeTab === 'completed'
                ? 'bg-emerald-950/70 text-emerald-300 border-2 border-emerald-500 shadow-xs'
                : 'text-zinc-300 hover:text-white hover:bg-zinc-900 border-2 border-zinc-700'
            }`}
          >
            <CheckCircle2 className="w-3.5 h-3.5" />
            <span>Completed History</span>
            <span className="text-[10px] font-bold px-1.5 py-0.2 rounded-full bg-zinc-800 border border-zinc-600 text-zinc-200">
              {completedBatches?.length || 0}
            </span>
          </button>
        </div>

        {/* Tab 1: Execution Queue */}
        {activeTab === 'queue' && (
          <div>
            {queuedRequests && queuedRequests.length > 0 ? (
              <QueuedRequestsList
                queuedRequests={queuedRequests}
                onCancelQueuedBatch={onCancelQueuedBatch}
              />
            ) : (
              <div className="py-4 px-3 bg-[#0a0e17] border-2 border-dashed border-zinc-700 rounded-xl text-center space-y-1">
                <p className="text-xs font-semibold text-zinc-200">
                  Pipeline queue is empty
                </p>
                <p className="text-[11px] text-zinc-400 font-mono">
                  All submitted extractions have completed. Submit new localities or inspect previous runs below.
                </p>
                {completedBatches && completedBatches.length > 0 && (
                  <button
                    type="button"
                    onClick={() => setActiveTab('completed')}
                    className="mt-2 text-xs font-semibold text-emerald-400 hover:text-emerald-300 hover:underline inline-flex items-center gap-1 cursor-pointer"
                  >
                    <span>View {completedBatches.length} Completed Extractions</span>
                  </button>
                )}
              </div>
            )}
          </div>
        )}

        {/* Tab 2: Completed Requests with Direct CSV Downloads */}
        {activeTab === 'completed' && (
          <CompletedBatchesList
            completedBatches={completedBatches}
            onDownloadCsv={onDownloadCsv}
            onInspectBatch={onInspectBatch}
            onRefresh={onRefreshCompleted}
            isLoading={isLoadingCompleted}
            currentRequestId={currentRequestId}
          />
        )}
      </div>
    </div>
  );
}
