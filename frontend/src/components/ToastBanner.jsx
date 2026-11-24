import React from 'react';
import { Info, CheckCircle2, AlertTriangle, AlertCircle, X } from 'lucide-react';

export function ToastBanner({ toast, onDismiss }) {
  if (!toast || !toast.message) return null;

  const typeConfig = {
    info: {
      bg: 'bg-emerald-950/60 border-2 border-emerald-500/50 text-emerald-300',
      icon: <Info className="w-3.5 h-3.5 text-emerald-400 shrink-0" />,
      closeHover: 'hover:text-emerald-100 text-emerald-400',
    },
    success: {
      bg: 'bg-emerald-950/80 border-2 border-emerald-500 text-emerald-200',
      icon: <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400 shrink-0" />,
      closeHover: 'hover:text-white text-emerald-300',
    },
    warning: {
      bg: 'bg-zinc-900 border-2 border-zinc-600 text-zinc-200',
      icon: <AlertTriangle className="w-3.5 h-3.5 text-zinc-300 shrink-0" />,
      closeHover: 'hover:text-white text-zinc-400',
    },
    error: {
      bg: 'bg-zinc-900 border-2 border-zinc-600 text-zinc-200',
      icon: <AlertCircle className="w-3.5 h-3.5 text-zinc-300 shrink-0" />,
      closeHover: 'hover:text-white text-zinc-400',
    },
  };

  const cfg = typeConfig[toast.type] || typeConfig.info;

  return (
    <div
      className={`p-2.5 rounded-lg text-xs font-medium flex items-center justify-between gap-2 transition shadow-lg ${cfg.bg}`}
    >
      <div className="flex items-center gap-2 min-w-0 truncate">
        {cfg.icon}
        <span className="truncate font-mono text-[11px]">{toast.message}</span>
      </div>
      <button
        type="button"
        onClick={onDismiss}
        className={`text-xs font-bold shrink-0 cursor-pointer p-0.5 ${cfg.closeHover}`}
      >
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  );
}
