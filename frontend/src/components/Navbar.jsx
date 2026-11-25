import React, { useState } from 'react';
import { RotateCcw, Bell, Loader2, LogOut, User as UserIcon } from 'lucide-react';

export function Navbar({ onResetSession, onTestSlack, isTestingSlack, auth }) {
  const [testing, setTesting] = useState(false);

  const handleTestSlack = async () => {
    if (testing || isTestingSlack) return;
    setTesting(true);
    try {
      if (onTestSlack) await onTestSlack();
    } finally {
      setTimeout(() => setTesting(false), 2000);
    }
  };

  return (
    <header className="bg-[#0a0e17]/95 backdrop-blur-md border-b-2 border-zinc-700 sticky top-0 z-50 transition-all shadow-md">
      <div className="max-w-[1540px] w-full mx-auto px-4 sm:px-6">
        <div className="flex justify-between h-16 items-center">
          {/* Brand Logo & Title (Strict Black, White, Green) */}
          <div className="flex items-center space-x-3">
            {/* Custom Minimalist Zomato Restaurant Extraction Emblem */}
            <div className="w-9 h-9 rounded-xl bg-emerald-500/15 border-2 border-emerald-500/50 text-emerald-400 flex items-center justify-center shadow-[0_0_15px_rgba(16,185,129,0.35)] shrink-0">
              <svg
                className="w-5 h-5 text-emerald-400"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2.2"
                strokeLinecap="round"
                strokeLinejoin="round"
              >
                {/* Cloche Dome & Handle */}
                <path d="M12 2v2" />
                <circle cx="12" cy="4.5" r="1.5" fill="currentColor" />
                <path d="M4 16a8 8 0 0 1 16 0" />
                <path d="M2 17h20" />
                {/* Internal Data Scanning Radar Waves */}
                <path d="M8 13a4 4 0 0 1 8 0" strokeDasharray="2 2" stroke="currentColor" />
                <path d="M9 20h6" />
              </svg>
            </div>

            {/* Brand Typography */}
            <div className="flex items-baseline space-x-2">
              <span className="text-base font-bold tracking-tight text-white font-sans">
                Zomato<span className="text-emerald-400">.Extract</span>
              </span>
              <span className="hidden sm:inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-mono font-semibold bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/40">
                DATA PIPELINE
              </span>
            </div>
          </div>

          {/* Right Header Navigation Controls with High-Contrast Visible Borders */}
          <div className="flex items-center space-x-2.5 sm:space-x-3">
            {/* Live Engine Indicator (Emerald Green Glow) */}
            <div className="hidden sm:flex items-center space-x-2 px-2.5 py-1 rounded-full bg-[#0e1422] border-2 border-zinc-700 text-[11px] font-mono text-zinc-300">
              <span className="relative flex h-2 w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
              </span>
              <span className="text-[10px] tracking-wider uppercase font-bold text-zinc-200">
                Engine Active
              </span>
            </div>

            {/* Test Slack Notification Action Button */}
            <button
              onClick={handleTestSlack}
              disabled={testing || isTestingSlack}
              type="button"
              title="Verify Slack Webhook Alerts Integration"
              className="text-xs font-semibold text-zinc-200 hover:text-white bg-[#0e1422] hover:bg-[#131b2e] border-2 border-zinc-700 hover:border-emerald-500 hover:shadow-[0_0_12px_rgba(16,185,129,0.25)] px-3 py-1.5 rounded-lg transition-all flex items-center gap-1.5 cursor-pointer active:scale-95"
            >
              {testing || isTestingSlack ? (
                <>
                  <Loader2 className="w-3.5 h-3.5 text-emerald-400 animate-spin" />
                  <span className="font-mono text-[11px]">Testing...</span>
                </>
              ) : (
                <>
                  <Bell className="w-3.5 h-3.5 text-zinc-400" />
                  <span className="font-mono text-[11px]">Slack Alerts</span>
                  <span className="w-1.5 h-1.5 rounded-full bg-emerald-500"></span>
                </>
              )}
            </button>

            {/* Reset / New Search Action */}
            <button
              onClick={onResetSession}
              type="button"
              className="text-xs font-semibold text-zinc-200 hover:text-white bg-[#0e1422] hover:bg-[#131b2e] border-2 border-zinc-700 hover:border-emerald-500 hover:shadow-[0_0_12px_rgba(16,185,129,0.25)] px-3.5 py-1.5 rounded-lg transition-all flex items-center gap-1.5 cursor-pointer active:scale-95"
            >
              <RotateCcw className="w-3.5 h-3.5 text-zinc-400" />
              <span>New Search</span>
            </button>

            {/* Google Authentication Control */}
            {auth && (
              <div className="flex items-center pl-2 border-l border-zinc-700">
                {auth.loading ? (
                  <div className="h-8 w-24 rounded-full bg-zinc-800/80 animate-pulse border border-zinc-700" />
                ) : auth.isAuthenticated && auth.user ? (
                  <div className="flex items-center space-x-2 bg-[#0e1422] border-2 border-zinc-700 rounded-full pl-1.5 pr-2.5 py-1">
                    {auth.user.avatar ? (
                      <img
                        src={auth.user.avatar}
                        alt={auth.user.name || 'User'}
                        className="w-6 h-6 rounded-full border border-emerald-500/50 object-cover"
                        referrerPolicy="no-referrer"
                      />
                    ) : (
                      <div className="w-6 h-6 rounded-full bg-emerald-900 border border-emerald-500/50 flex items-center justify-center text-[11px] font-bold text-emerald-300">
                        {(auth.user.name || auth.user.email || 'U')[0].toUpperCase()}
                      </div>
                    )}
                    <span className="text-xs font-medium text-zinc-200 max-w-[110px] truncate" title={auth.user.email}>
                      {auth.user.name || auth.user.email}
                    </span>
                    <button
                      onClick={auth.logout}
                      type="button"
                      title="Sign Out"
                      className="text-zinc-400 hover:text-rose-400 transition-colors p-0.5 rounded hover:bg-zinc-800"
                    >
                      <LogOut className="w-3.5 h-3.5" />
                    </button>
                  </div>
                ) : (
                  <div
                    ref={(el) => auth.renderGoogleButton(el)}
                    className="h-8 flex items-center min-w-[130px]"
                  />
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  );
}
