import React, { useRef } from 'react';
import { CheckCircle2, Download, ChevronLeft, ChevronRight, Clock, RefreshCw, Eye } from 'lucide-react';

export function CompletedBatchesList({
  completedBatches,
  onDownloadCsv,
  onInspectBatch,
  onRefresh,
  isLoading,
  currentRequestId,
}) {
  const scrollRef = useRef(null);

  if (!completedBatches || completedBatches.length === 0) {
    return (
      <div className="pt-3 border-t-2 border-zinc-700 text-center py-4">
        <p className="text-xs text-zinc-400 font-mono">No previous completed requests in history yet.</p>
      </div>
    );
  }

  const scroll = (direction) => {
    if (scrollRef.current) {
      const scrollAmount = direction === 'left' ? -300 : 300;
      scrollRef.current.scrollBy({ left: scrollAmount, behavior: 'smooth' });
    }
  };

  return (
    <div className="pt-3 border-t-2 border-zinc-700 space-y-2.5">
      {/* Header bar */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-5 h-5 rounded-md bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center">
            <CheckCircle2 className="w-3.5 h-3.5" />
          </div>
          <span className="text-xs font-bold text-white">
            Completed Extractions History
          </span>
          <span className="font-mono text-[10px] font-bold bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/40 px-2 py-0.5 rounded-md">
            {completedBatches.length} COMPLETED
          </span>
        </div>

        {/* Scroll Controls & Refresh with High-Contrast Borders */}
        <div className="flex items-center gap-1.5">
          {onRefresh && (
            <button
              type="button"
              onClick={onRefresh}
              disabled={isLoading}
              title="Refresh completed history"
              className="p-1 rounded-md text-zinc-300 hover:text-white hover:border-emerald-500 bg-zinc-900 border-2 border-zinc-700 transition cursor-pointer"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${isLoading ? 'animate-spin text-emerald-400' : ''}`} />
            </button>
          )}

          {completedBatches.length > 2 && (
            <>
              <button
                type="button"
                onClick={() => scroll('left')}
                className="p-1 rounded-md text-zinc-300 hover:text-white hover:border-emerald-500 bg-zinc-900 border-2 border-zinc-700 transition cursor-pointer"
                title="Scroll left"
              >
                <ChevronLeft className="w-3.5 h-3.5" />
              </button>
              <button
                type="button"
                onClick={() => scroll('right')}
                className="p-1 rounded-md text-zinc-300 hover:text-white hover:border-emerald-500 bg-zinc-900 border-2 border-zinc-700 transition cursor-pointer"
                title="Scroll right"
              >
                <ChevronRight className="w-3.5 h-3.5" />
              </button>
            </>
          )}
        </div>
      </div>

      {/* Dynamic Scroll Track of Completed Batches with Visible Borders */}
      <div
        ref={scrollRef}
        className="flex gap-3 overflow-x-auto pb-2 scroll-smooth scrollbar-thin scrollbar-thumb-zinc-600 scrollbar-track-transparent"
      >
        {completedBatches.map((item, idx) => {
          const batchIdVal = item.batch_id || item.request_id || '';
          const batchShort = batchIdVal.substring(0, 8);
          const city = item.city || 'City';
          const location = item.location || 'All Locations';
          const recCount = item.records_count ?? item.completed_jobs ?? 0;
          const isInspecting = currentRequestId === batchIdVal;

          const isTerminated = item.status === 'terminated';
          const isFailed = item.status === 'failed';
          const statusText = isTerminated ? 'TERMINATED' : isFailed ? 'FAILED' : 'COMPLETED';
          const statusBadgeClass = isTerminated
            ? 'bg-amber-950/80 text-amber-300 border-amber-500/50'
            : isFailed
            ? 'bg-rose-950/80 text-rose-300 border-rose-500/50'
            : 'bg-emerald-950/80 text-emerald-300 border-emerald-500/50';
          const statusDotClass = isTerminated
            ? 'bg-amber-400'
            : isFailed
            ? 'bg-rose-400'
            : 'bg-emerald-400';

          const dateStr = item.completed_at
            ? new Date(item.completed_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            : 'Done';

          return (
            <div
              key={batchIdVal || idx}
              className={`shrink-0 w-80 p-3.5 rounded-xl border-2 flex flex-col justify-between transition-all duration-200 shadow-md ${
                isInspecting
                  ? 'bg-emerald-950/25 border-emerald-500 ring-1 ring-emerald-500 shadow-[0_0_15px_rgba(16,185,129,0.25)]'
                  : 'bg-[#0a0e17] border-zinc-700 hover:border-emerald-500/70'
              }`}
            >
              <div
                onClick={() => onInspectBatch && onInspectBatch(item)}
                className="cursor-pointer group"
                title="Click to view and inspect extracted records in table"
              >
                {/* Status chip & batch code */}
                <div className="flex items-center justify-between gap-2 mb-1.5">
                  <span className={`px-2 py-0.5 font-mono text-[9px] font-bold uppercase rounded tracking-wider flex items-center gap-1 border ${statusBadgeClass}`}>
                    <span className={`w-1.5 h-1.5 rounded-full ${statusDotClass}`}></span>
                    <span>{statusText}</span>
                  </span>

                  <span className="font-mono text-[10px] text-zinc-300 font-semibold bg-zinc-900 border border-zinc-700 px-1.5 py-0.2 rounded">
                    #{batchShort}
                  </span>
                </div>

                {/* Location & City */}
                <p className="font-bold text-white text-xs truncate group-hover:text-emerald-400 transition-colors">
                  {city}{' '}
                  <span className="font-normal text-zinc-400 text-[11px]">
                    ({location})
                  </span>
                </p>

                {/* Extracted Record Count and Time */}
                <div className="flex items-center justify-between text-[11px] text-zinc-400 font-mono mt-2 pt-2 border-t border-zinc-700">
                  <span className="font-bold text-emerald-400 flex items-center gap-1">
                    <span>{recCount} records saved</span>
                  </span>
                  <span className="text-zinc-400 flex items-center gap-1">
                    <Clock className="w-3 h-3 text-zinc-500" />
                    <span>{dateStr}</span>
                  </span>
                </div>
              </div>

              {/* Action Buttons: View Records & Download CSV */}
              <div className="mt-3 pt-2 border-t border-zinc-700 flex items-center gap-2">
                {onInspectBatch && (
                  <button
                    type="button"
                    onClick={() => onInspectBatch(item)}
                    title={isInspecting ? "Currently viewing this batch's records" : "View and load this batch's extracted records in table"}
                    className={`flex-1 py-1.5 px-2.5 rounded-lg text-xs font-bold font-mono border-2 transition-all flex items-center justify-center gap-1.5 cursor-pointer ${
                      isInspecting
                        ? 'bg-emerald-500 text-black border-emerald-400 shadow-[0_0_10px_rgba(16,185,129,0.4)]'
                        : 'bg-zinc-900 hover:bg-zinc-800 text-emerald-400 border-zinc-700 hover:border-emerald-500'
                    }`}
                  >
                    <Eye className="w-3.5 h-3.5" />
                    <span>{isInspecting ? 'Viewing' : 'View Records'}</span>
                  </button>
                )}

                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    onDownloadCsv(batchIdVal, [], `zomato_${city}_${location}_${batchShort}.csv`);
                  }}
                  title="Download CSV for this batch"
                  className="py-1.5 px-3 bg-zinc-900 hover:bg-zinc-800 text-zinc-200 hover:text-white rounded-lg text-xs font-bold font-mono border-2 border-zinc-700 hover:border-emerald-500 transition-all flex items-center justify-center gap-1.5 cursor-pointer"
                >
                  <Download className="w-3.5 h-3.5 text-emerald-400" />
                  <span>CSV</span>
                </button>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
