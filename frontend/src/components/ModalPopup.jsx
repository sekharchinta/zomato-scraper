import React, { useEffect } from 'react';
import { AlertTriangle, Info, CheckCircle2, X } from 'lucide-react';

export function ModalPopup({
  isOpen,
  title,
  message,
  details = [],
  type = 'confirm', // 'confirm' | 'alert' | 'warning'
  confirmText = 'Confirm',
  cancelText = 'Cancel',
  onConfirm,
  onCancel,
}) {
  // Close on Escape key press
  useEffect(() => {
    if (!isOpen) return;
    const handleKeyDown = (e) => {
      if (e.key === 'Escape' && onCancel) {
        onCancel();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [isOpen, onCancel]);

  if (!isOpen) return null;

  const isAlertOnly = type === 'alert';

  return (
    <div className="fixed inset-0 z-50 bg-black/85 backdrop-blur-sm flex items-center justify-center p-4 transition-all">
      {/* Click outside backdrop */}
      <div className="absolute inset-0" onClick={onCancel} />

      {/* High-Contrast Dialog Box (Black, White, Green) */}
      <div
        className="relative bg-[#0e1422] border-2 border-zinc-700 rounded-2xl shadow-[0_0_50px_rgba(0,0,0,0.9)] max-w-md w-full overflow-hidden p-6 space-y-4 z-10 transition-transform scale-100 animate-in fade-in zoom-in-95 duration-150"
        role="dialog"
        aria-modal="true"
      >
        {/* Top Header Row with Icon & Close Button */}
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-emerald-500/15 border-2 border-emerald-500/50 text-emerald-400 flex items-center justify-center shadow-[0_0_15px_rgba(16,185,129,0.3)] shrink-0">
              {type === 'warning' || type === 'confirm' ? (
                <AlertTriangle className="w-5 h-5 text-emerald-400" />
              ) : (
                <Info className="w-5 h-5 text-emerald-400" />
              )}
            </div>
            <div>
              <h3 className="text-sm font-bold text-white font-sans tracking-tight">
                {title || 'Confirmation Required'}
              </h3>
              <span className="font-mono text-[10px] text-zinc-400 uppercase tracking-wider">
                {type.toUpperCase()} // ACTION REQUIRED
              </span>
            </div>
          </div>

          {onCancel && (
            <button
              type="button"
              onClick={onCancel}
              className="text-zinc-400 hover:text-white p-1 rounded-lg border border-transparent hover:border-zinc-700 transition cursor-pointer"
            >
              <X className="w-4 h-4" />
            </button>
          )}
        </div>

        {/* Message Content */}
        <div className="space-y-2">
          <p className="text-xs text-zinc-300 font-mono leading-relaxed">
            {message}
          </p>

          {/* Optional Bulleted Details Box */}
          {details && details.length > 0 && (
            <div className="p-3 rounded-xl bg-[#070a10] border-2 border-zinc-700 text-xs text-zinc-300 font-mono space-y-1.5 mt-2">
              {details.map((item, idx) => (
                <div key={idx} className="flex items-start gap-2 text-[11px]">
                  <span className="text-emerald-400 shrink-0 font-bold">&bull;</span>
                  <span className="break-all">{item}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Action Buttons with High-Visibility Borders */}
        <div className="flex items-center justify-end gap-2.5 pt-2 border-t-2 border-zinc-700">
          {!isAlertOnly && onCancel && (
            <button
              type="button"
              onClick={onCancel}
              className="px-4 py-2 rounded-lg border-2 border-zinc-700 bg-zinc-900 hover:bg-zinc-800 text-zinc-300 hover:text-white font-mono text-xs font-semibold cursor-pointer active:scale-95 transition"
            >
              {cancelText}
            </button>
          )}

          <button
            type="button"
            onClick={onConfirm}
            className="px-5 py-2 rounded-lg border-2 border-emerald-400 bg-emerald-600 hover:bg-emerald-500 text-black font-bold font-mono text-xs shadow-[0_0_15px_rgba(16,185,129,0.35)] cursor-pointer active:scale-95 transition"
          >
            {confirmText}
          </button>
        </div>
      </div>
    </div>
  );
}
