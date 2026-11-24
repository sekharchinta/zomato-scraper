import React from 'react';
import { Layers, Compass, FileCode2, Database } from 'lucide-react';

export function MetricsCards({ stats }) {
  const items = [
    {
      code: 'PAR-01',
      title: 'Total Batches',
      value: Number(stats?.total_batches || 0).toLocaleString(),
      icon: Layers,
      sublabel: 'Executed Pipelines',
    },
    {
      code: 'PAR-02',
      title: 'Target URLs',
      value: Number(stats?.restaurants_urls || 0).toLocaleString(),
      icon: Compass,
      sublabel: 'Discovered Endpoints',
    },
    {
      code: 'PAR-03',
      title: 'Raw Payloads',
      value: Number(stats?.restaurants_data || 0).toLocaleString(),
      icon: FileCode2,
      sublabel: 'Parsed JSON Trees',
    },
    {
      code: 'PAR-04',
      title: 'Extracted Records',
      value: Number(stats?.delivery_data || 0).toLocaleString(),
      icon: Database,
      sublabel: 'Structured Database',
    },
  ];

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3.5">
      {items.map((item, idx) => {
        const Icon = item.icon;
        return (
          <div
            key={idx}
            className="bg-[#0e1422] p-4 rounded-xl border-2 border-zinc-700 hover:border-emerald-500 transition-all duration-200 flex flex-col justify-between relative group overflow-hidden shadow-lg hover:shadow-[0_0_15px_rgba(16,185,129,0.25)]"
          >
            {/* Top Row: Monospace Parameter Tag & Geometric Icon */}
            <div className="flex items-center justify-between mb-3">
              <span className="font-mono text-[10px] tracking-widest uppercase font-semibold text-zinc-300 bg-zinc-900/90 border border-zinc-600 px-2 py-0.5 rounded">
                {item.code}
              </span>
              <div className="w-8 h-8 rounded-lg flex items-center justify-center bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 group-hover:scale-105 transition-transform">
                <Icon className="w-4 h-4" />
              </div>
            </div>

            {/* Metric Value & Label */}
            <div>
              <h4 className="text-2xl font-bold font-mono tracking-tight text-white">
                {item.value}
              </h4>
              <p className="text-xs font-semibold text-zinc-200 mt-0.5">
                {item.title}
              </p>
              <p className="text-[10px] font-mono text-zinc-400 mt-0.5">
                {item.sublabel}
              </p>
            </div>

            {/* Subtle green indicator line on hover */}
            <div
              className="absolute bottom-0 left-0 right-0 h-[3px] bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)] opacity-0 group-hover:opacity-100 transition-opacity duration-200"
            />
          </div>
        );
      })}
    </div>
  );
}
