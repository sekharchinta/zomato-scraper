import React, { useState, useMemo } from 'react';
import { Search, Download, Star, ExternalLink, ChevronLeft, ChevronRight, FileSpreadsheet, Loader2 } from 'lucide-react';
import { api } from '../api/client';

export function DataTable({
  currentRequestId,
  scrapeData,
  tableFields,
  onDownloadCsv,
}) {
  const [searchTerm, setSearchTerm] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [isDownloading, setIsDownloading] = useState(false);

  // Filter records based on instant search
  const filteredData = useMemo(() => {
    if (!searchTerm.trim()) return scrapeData;
    const q = searchTerm.toLowerCase();
    return scrapeData.filter((row) =>
      Object.values(row).some((val) => String(val || '').toLowerCase().includes(q))
    );
  }, [scrapeData, searchTerm]);

  // Pagination calculation
  const totalRecords = filteredData.length;
  const totalPages = Math.max(1, Math.ceil(totalRecords / pageSize));
  const activePage = Math.min(Math.max(1, currentPage), totalPages);

  const startIndex = (activePage - 1) * pageSize;
  const endIndex = Math.min(startIndex + pageSize, totalRecords);
  const pageData = filteredData.slice(startIndex, endIndex);

  // Handle CSV Download via robust blob mechanism
  const handleDownloadCsv = async () => {
    if (!currentRequestId || isDownloading) return;
    setIsDownloading(true);
    try {
      if (onDownloadCsv) {
        await onDownloadCsv(currentRequestId, tableFields);
      } else {
        await api.downloadCsvBlob(currentRequestId, tableFields);
      }
    } catch (err) {
      console.error('Error downloading CSV:', err);
    } finally {
      setIsDownloading(false);
    }
  };

  if (!scrapeData || scrapeData.length === 0) {
    return null;
  }

  return (
    <div className="bg-[#0e1422] rounded-xl border-2 border-zinc-700 shadow-xl overflow-hidden">
      {/* Precision Header Bar with High-Contrast Border */}
      <div className="px-5 py-3.5 border-b-2 border-zinc-700 flex flex-col sm:flex-row items-center justify-between gap-3 bg-[#0a0e17]">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-lg bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center">
            <FileSpreadsheet className="w-3.5 h-3.5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-xs font-bold text-white">
                Extracted Records Telemetry
              </h3>
              <span className="font-mono text-[10px] font-semibold bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/40 px-2 py-0.5 rounded-md">
                {totalRecords} RECORDS
              </span>
            </div>
            <p className="text-[10px] font-mono text-zinc-400">
              STRUCTURED DATA STREAM // PERSISTED
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2.5 w-full sm:w-auto">
          {/* Search Filter Box with Visible Border */}
          <div className="relative w-full sm:w-60">
            <span className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-zinc-400">
              <Search className="w-3.5 h-3.5" />
            </span>
            <input
              type="text"
              placeholder="Search telemetry..."
              value={searchTerm}
              onChange={(e) => {
                setSearchTerm(e.target.value);
                setCurrentPage(1);
              }}
              className="pl-8 pr-3 py-1.5 text-xs bg-[#070a10] border-2 border-zinc-700 rounded-lg focus:border-emerald-400 focus:ring-1 focus:ring-emerald-400/40 text-white placeholder-zinc-500 w-full outline-hidden font-medium"
            />
          </div>

          {/* Export CSV Action with Border & Glow */}
          <button
            type="button"
            onClick={handleDownloadCsv}
            disabled={isDownloading}
            className="px-3.5 py-1.5 bg-emerald-600 hover:bg-emerald-500 active:scale-95 text-black rounded-lg text-xs font-bold border-2 border-emerald-400 shadow-[0_0_12px_rgba(16,185,129,0.3)] transition-all flex items-center gap-1.5 whitespace-nowrap cursor-pointer font-mono group relative overflow-hidden"
          >
            {isDownloading ? (
              <>
                <Loader2 className="w-3.5 h-3.5 animate-spin text-black" />
                <span>EXPORTING...</span>
              </>
            ) : (
              <>
                <Download className="w-3.5 h-3.5 group-hover:-translate-y-0.5 transition-transform text-black" />
                <span>EXPORT CSV</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Responsive Table with High-Contrast Row and Column Dividers */}
      <div className="overflow-x-auto max-h-[440px] scrollbar-thin scrollbar-thumb-zinc-600">
        <table className="min-w-full divide-y-2 divide-zinc-700 text-xs">
          <thead className="bg-[#070a10] sticky top-0 z-10 backdrop-blur-xs border-b-2 border-zinc-700">
            <tr>
              {tableFields.map((f) => (
                <th
                  key={f}
                  className="px-4 py-2.5 text-left font-mono font-semibold text-zinc-300 uppercase tracking-wider text-[10px] whitespace-nowrap border-r border-zinc-800 last:border-r-0"
                >
                  {f.replace(/_/g, ' ')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-[#0e1422] divide-y border-b border-zinc-700 divide-zinc-700/80 font-sans">
            {pageData.length === 0 ? (
              <tr>
                <td
                  colSpan={tableFields.length}
                  className="text-center py-8 text-zinc-400 italic text-xs font-mono"
                >
                  No matching telemetry records found.
                </td>
              </tr>
            ) : (
              pageData.map((row, rowIdx) => (
                <tr
                  key={rowIdx}
                  className="hover:bg-emerald-950/20 transition-colors"
                >
                  {tableFields.map((f) => {
                    const val = row[f];
                    let rendered = null;

                    if (val === null || val === undefined) {
                      rendered = <span className="text-zinc-600 font-mono">-</span>;
                    } else if (f === 'restaurant_id') {
                      rendered = (
                        <span className="font-mono text-zinc-300 text-[11px] font-semibold bg-zinc-900 border border-zinc-700 px-1.5 py-0.5 rounded">
                          #{val}
                        </span>
                      );
                    } else if (f === 'total_reviews') {
                      rendered = (
                        <span className="font-mono text-zinc-200 bg-zinc-900 border border-zinc-700 px-2 py-0.5 rounded text-[11px]">
                          {Number(val).toLocaleString()}
                        </span>
                      );
                    } else if (
                      f === 'rating' ||
                      f === 'aggregate_rating' ||
                      f === 'delivery_rating'
                    ) {
                      rendered = (
                        <span className="font-mono font-semibold text-emerald-300 bg-emerald-950/60 border border-emerald-500/60 px-2 py-0.5 rounded text-xs inline-flex items-center gap-1">
                          <Star className="w-2.5 h-2.5 text-emerald-400 fill-current" />
                          <span>{val}</span>
                        </span>
                      );
                    } else if (f === 'reviews' && Array.isArray(val)) {
                      rendered = (
                        <span className="font-mono text-[11px] text-zinc-300">
                          {val.length} revs
                        </span>
                      );
                    } else if ((f === 'canonical_url' || f === 'page_url') && val) {
                      rendered = (
                        <a
                          href={val}
                          target="_blank"
                          rel="noreferrer"
                          className="text-emerald-400 hover:text-emerald-300 hover:underline inline-flex items-center gap-1 font-mono text-[11px] font-medium"
                        >
                          <ExternalLink className="w-2.5 h-2.5" />
                          <span>LINK</span>
                        </a>
                      );
                    } else {
                      const str = String(val);
                      rendered = str.length > 40 ? `${str.substring(0, 40)}...` : str;
                    }

                    return (
                      <td key={f} className="px-4 py-2.5 whitespace-nowrap text-zinc-200 text-xs border-r border-zinc-800/60 last:border-r-0">
                        {rendered}
                      </td>
                    );
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination Bar with Visible Border */}
      <div className="px-5 py-3 bg-[#0a0e17] border-t-2 border-zinc-700 flex flex-col sm:flex-row items-center justify-between gap-3 text-xs">
        <div className="flex items-center space-x-2 text-zinc-300 text-[11px] font-mono">
          <span>
            SHOWING {totalRecords === 0 ? 0 : startIndex + 1}-{endIndex} OF {totalRecords}
          </span>
          <span className="text-zinc-600">&bull;</span>
          <label htmlFor="select-page-size" className="text-zinc-400">
            PER PAGE:
          </label>
          <select
            id="select-page-size"
            value={pageSize}
            onChange={(e) => {
              setPageSize(Number(e.target.value));
              setCurrentPage(1);
            }}
            className="bg-[#070a10] border-2 border-zinc-700 text-zinc-200 rounded px-2 py-0.5 text-[11px] cursor-pointer outline-hidden font-mono focus:border-emerald-400"
          >
            <option value="10">10</option>
            <option value="25">25</option>
            <option value="50">50</option>
            <option value="100">100</option>
          </select>
        </div>

        <div className="flex items-center space-x-2 font-mono">
          <button
            type="button"
            disabled={activePage <= 1}
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            className="px-2.5 py-1 bg-zinc-900 border-2 border-zinc-700 rounded text-zinc-200 hover:text-white hover:border-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed text-xs font-medium transition cursor-pointer flex items-center gap-1 active:scale-95"
          >
            <ChevronLeft className="w-3.5 h-3.5" />
            <span>PREV</span>
          </button>
          <span className="text-zinc-200 font-semibold text-xs px-1">
            {activePage} / {totalPages}
          </span>
          <button
            type="button"
            disabled={activePage >= totalPages}
            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
            className="px-2.5 py-1 bg-zinc-900 border-2 border-zinc-700 rounded text-zinc-200 hover:text-white hover:border-emerald-500 disabled:opacity-40 disabled:cursor-not-allowed text-xs font-medium transition cursor-pointer flex items-center gap-1 active:scale-95"
          >
            <span>NEXT</span>
            <ChevronRight className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
    </div>
  );
}
