import React, { useRef } from 'react';
import { Clock, Trash2, ChevronLeft, ChevronRight } from 'lucide-react';

export function QueuedRequestsList({ queuedRequests, onCancelQueuedBatch }) {
  const scrollContainerRef = useRef(null);

  if (!queuedRequests || queuedRequests.length === 0) {
    return null;
  }

  const scroll = (direction) => {
    if (scrollContainerRef.current) {
      const scrollAmount = direction === 'left' ? -280 : 280;
      scrollContainerRef.current.scrollBy({ left: scrollAmount, behavior: 'smooth' });
    }
  };

  return (
    <div className="pt-3 border-t-2 border-zinc-700 space-y-2.5">
      {/* Header with queue count & scroll buttons */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="w-5 h-5 rounded-md bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center">
            <Clock className="w-3.5 h-3.5" />
          </div>
          <span className="text-xs font-bold text-white">
            Pipeline Execution Queue
          </span>
          <span className="font-mono text-[10px] font-bold bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/40 px-2 py-0.5 rounded-md">
            {queuedRequests.length} PENDING
          </span>
        </div>

        {/* Scroll navigation controls */}
        {queuedRequests.length > 2 && (
          <div className="flex items-center gap-1">
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
          </div>
        )}
      </div>

      {/* Dynamic Scroll Track with Clear Borders */}
      <div
        ref={scrollContainerRef}
        className="flex gap-2.5 overflow-x-auto pb-1.5 scroll-smooth scrollbar-thin scrollbar-thumb-zinc-600 scrollbar-track-transparent"
      >
        {queuedRequests.map((item, idx) => {
          const batchIdVal = item.batch_id || item.request_id || '';
          const batchShort = batchIdVal.substring(0, 8);
          const city = item.city || 'City';
          const location = item.location || 'All Locations';
          const isNext = idx === 0;

          return (
            <div
              key={batchIdVal || idx}
              className={`shrink-0 w-64 p-3 rounded-xl transition-all duration-200 shadow-md ${
                isNext
                  ? 'bg-emerald-950/30 border-2 border-emerald-500 shadow-[0_0_15px_rgba(16,185,129,0.2)]'
                  : 'bg-[#0a0e17] border-2 border-zinc-700 hover:border-zinc-500'
              }`}
            >
              <div className="flex items-center justify-between gap-2 mb-1.5">
                <span
                  className={`px-1.5 py-0.5 font-mono text-[9px] font-bold uppercase rounded tracking-wider flex items-center gap-1 ${
                    isNext
                      ? 'bg-emerald-500/25 text-emerald-300 border border-emerald-500/50'
                      : 'bg-zinc-900 text-zinc-300 border border-zinc-700'
                  }`}
                >
                  <span
                    className={`w-1.5 h-1.5 rounded-full ${
                      isNext ? 'bg-emerald-400 animate-pulse' : 'bg-zinc-500'
                    }`}
                  ></span>
                  {isNext ? 'NEXT UP' : `QUEUE #${idx + 1}`}
                </span>

                <button
                  type="button"
                  onClick={() => onCancelQueuedBatch(batchIdVal, city, location)}
                  title="Remove from queue"
                  className="text-zinc-400 hover:text-emerald-400 p-1 rounded-md hover:bg-zinc-800 border border-transparent hover:border-zinc-700 transition cursor-pointer"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              </div>

              <p className="font-bold text-white text-xs truncate">
                {city}{' '}
                <span className="font-normal text-zinc-400 text-[11px]">
                  ({location})
                </span>
              </p>

              <div className="flex items-center justify-between text-[10px] text-zinc-400 mt-2 pt-2 border-t border-zinc-700 font-mono">
                <span className="text-zinc-200 font-semibold bg-zinc-900 border border-zinc-700 px-1 py-0.2 rounded">{batchShort}</span>
                <span>{item.time_str || 'QUEUED'}</span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
