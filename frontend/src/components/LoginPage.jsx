import React, { useEffect, useRef } from 'react';
import { Shield, Sparkles, Database, Lock, AlertCircle } from 'lucide-react';

export function LoginPage({ auth }) {
  const googleBtnRef = useRef(null);

  useEffect(() => {
    if (googleBtnRef.current && auth?.renderGoogleButton) {
      auth.renderGoogleButton(googleBtnRef.current);
    }
  }, [auth]);

  return (
    <div className="min-h-screen bg-[#070a10] bg-engineering-grid flex flex-col justify-center items-center px-4 sm:px-6 lg:px-8 relative selection:bg-emerald-500 selection:text-black">
      {/* Background ambient lighting effects */}
      <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[550px] h-[350px] bg-emerald-500/10 blur-[130px] rounded-full pointer-events-none" />
      <div className="absolute bottom-1/4 left-1/2 -translate-x-1/2 translate-y-1/2 w-[400px] h-[300px] bg-blue-500/5 blur-[120px] rounded-full pointer-events-none" />

      {/* Main Login Card */}
      <div className="w-full max-w-md relative z-10 space-y-8">
        {/* Brand Header */}
        <div className="text-center space-y-3">
          <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-emerald-500/15 border-2 border-emerald-500/50 text-emerald-400 shadow-[0_0_25px_rgba(16,185,129,0.35)]">
            <svg
              className="w-8 h-8 text-emerald-400"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M12 2v2" />
              <circle cx="12" cy="4.5" r="1.5" fill="currentColor" />
              <path d="M4 16a8 8 0 0 1 16 0" />
              <path d="M2 17h20" />
              <path d="M8 13a4 4 0 0 1 8 0" strokeDasharray="2 2" stroke="currentColor" />
              <path d="M9 20h6" />
            </svg>
          </div>

          <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight text-white font-sans">
            Zomato<span className="text-emerald-400">.Extract</span>
          </h1>
          <p className="text-xs sm:text-sm text-zinc-400 max-w-sm mx-auto">
            High-Performance Restaurant Intelligence & Catalog Extraction Platform
          </p>
        </div>

        {/* Authentication Card */}
        <div className="bg-[#0b101c]/90 border-2 border-zinc-700/80 rounded-2xl p-6 sm:p-8 shadow-2xl backdrop-blur-xl space-y-6">
          <div className="space-y-1 text-center">
            <h2 className="text-lg font-bold text-zinc-100 flex items-center justify-center gap-2">
              <Lock className="w-4 h-4 text-emerald-400" />
              Authorized Access Only
            </h2>
            <p className="text-xs text-zinc-400">
              Sign in with your verified Google account to access live data pipelines and operations.
            </p>
          </div>

          {/* Auth Error Banner */}
          {auth?.authError && (
            <div className="bg-rose-950/60 border border-rose-500/60 rounded-xl p-3 flex items-start gap-2.5 text-rose-200 text-xs">
              <AlertCircle className="w-4 h-4 text-rose-400 shrink-0 mt-0.5" />
              <span>{auth.authError}</span>
            </div>
          )}

          {/* Google Sign In Button Container */}
          <div className="flex flex-col items-center justify-center pt-2 pb-1 min-h-[46px]">
            {auth?.loading ? (
              <div className="flex items-center gap-2.5 text-zinc-400 text-xs font-mono">
                <div className="w-4 h-4 border-2 border-emerald-400 border-t-transparent rounded-full animate-spin" />
                <span>Verifying credentials...</span>
              </div>
            ) : (
              <div
                ref={googleBtnRef}
                className="flex justify-center w-full min-h-[42px]"
              />
            )}
          </div>

          {/* Feature Highlights Grid */}
          <div className="pt-4 border-t border-zinc-800 grid grid-cols-2 gap-3 text-[11px] text-zinc-400 font-mono">
            <div className="flex items-center gap-1.5 bg-[#0e1424] px-2.5 py-2 rounded-lg border border-zinc-800">
              <Database className="w-3.5 h-3.5 text-emerald-400" />
              <span>PostgreSQL 16</span>
            </div>
            <div className="flex items-center gap-1.5 bg-[#0e1424] px-2.5 py-2 rounded-lg border border-zinc-800">
              <Sparkles className="w-3.5 h-3.5 text-emerald-400" />
              <span>Single RQ Queue</span>
            </div>
            <div className="flex items-center gap-1.5 bg-[#0e1424] px-2.5 py-2 rounded-lg border border-zinc-800">
              <Shield className="w-3.5 h-3.5 text-emerald-400" />
              <span>OAuth 2.0 Auth</span>
            </div>
            <div className="flex items-center gap-1.5 bg-[#0e1424] px-2.5 py-2 rounded-lg border border-zinc-800">
              <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              <span>Live Telemetry</span>
            </div>
          </div>
        </div>

        {/* Security & System Status Footer */}
        <div className="text-center space-y-1">
          <p className="text-[11px] text-zinc-500 font-mono">
            Protected by Google Identity Services & Django Session Security
          </p>
          <p className="text-[10px] text-zinc-600 font-mono">
            Database: zomato-scraper | Schema: zomato | Zero MongoDB
          </p>
        </div>
      </div>
    </div>
  );
}
