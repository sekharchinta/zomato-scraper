import React from 'react';
import { LoginPage } from './components/LoginPage';
import { AuthenticatedDashboard } from './components/AuthenticatedDashboard';
import { useAuth } from './hooks/useAuth';

export default function App() {
  const auth = useAuth();

  // 1. Initial Session Verification Screen
  if (auth.loading) {
    return (
      <div className="min-h-screen bg-[#070a10] bg-engineering-grid flex items-center justify-center selection:bg-emerald-500 selection:text-black">
        <div className="flex flex-col items-center gap-3.5 text-zinc-400 font-mono text-xs">
          <div className="w-9 h-9 border-2 border-emerald-400 border-t-transparent rounded-full animate-spin" />
          <span className="tracking-wider text-zinc-300">VERIFYING USER SESSION...</span>
        </div>
      </div>
    );
  }

  // 2. Authentication Gate: When unauthenticated, render ONLY <LoginPage />
  // AuthenticatedDashboard is NEVER mounted; zero background data hooks execute!
  if (!auth.isAuthenticated || !auth.user) {
    return <LoginPage auth={auth} />;
  }

  // 3. Authenticated User Dashboard: Mounted ONLY when auth is verified!
  return <AuthenticatedDashboard auth={auth} />;
}
