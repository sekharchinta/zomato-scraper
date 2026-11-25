import React from 'react';
import { Navbar } from './components/Navbar';
import { MetricsCards } from './components/MetricsCards';
import { ExtractionController } from './components/ExtractionController';
import { ProgressTracker } from './components/ProgressTracker';
import { DataTable } from './components/DataTable';
import { ModalPopup } from './components/ModalPopup';
import { LoginPage } from './components/LoginPage';
import { useCollectionStats } from './hooks/useCollectionStats';
import { useLocationSearch } from './hooks/useLocationSearch';
import { useScrapeManager } from './hooks/useScrapeManager';
import { useAuth } from './hooks/useAuth';

export default function App() {
  const auth = useAuth();
  const { stats, refreshStats } = useCollectionStats();
  const { suggestions, loading: isSearchingLocations, searchLocations } = useLocationSearch();

  const {
    currentRequestId,
    taskStatus,
    progressPct,
    progressTitle,
    progressStepDesc,
    counts,
    queuedRequests,
    completedBatches,
    isLoadingCompleted,
    scrapeData,
    tableFields,
    toast,
    modalConfig,
    closeModal,
    isSubmitting,
    isTerminating,
    clearToast,
    startPipeline,
    terminateProcess,
    cancelQueuedBatch,
    resetSession,
    inspectCompletedBatch,
    downloadBatchCsv,
    testSlackNotification,
    fetchCompletedBatches,
  } = useScrapeManager({ onStatsRefresh: refreshStats });

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

  // 2. Authentication Gate: Require Verified Google Sign-In before displaying dashboard
  if (!auth.isAuthenticated || !auth.user) {
    return <LoginPage auth={auth} />;
  }

  // 3. Authenticated User Dashboard
  return (
    <div className="min-h-screen bg-engineering-grid text-white font-sans antialiased selection:bg-emerald-500 selection:text-black">
      {/* High-Precision Top Navigation Bar with Zomato Restaurant Extraction Theme */}
      <Navbar
        onResetSession={resetSession}
        onTestSlack={testSlackNotification}
        auth={auth}
      />

      {/* Main Engineering Container */}
      <main className="max-w-[1540px] w-full mx-auto px-4 sm:px-6 py-5 space-y-4">
        {/* Top Telemetry Parameters */}
        <MetricsCards stats={stats} />

        {/* 2-Column Responsive Workspace: Setup (Left) + Live Progress & Queue (Right) */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-4 items-start">
          {/* Left Column: Extraction Parameters Setup */}
          <div className="lg:col-span-5">
            <ExtractionController
              onStartPipeline={startPipeline}
              isSubmitting={isSubmitting}
              suggestions={suggestions}
              isSearchingLocations={isSearchingLocations}
              onSearchLocations={searchLocations}
            />
          </div>

          {/* Right Column: Progress Telemetry, Dynamic Queue & Completed Requests */}
          <div className="lg:col-span-7">
            <ProgressTracker
              currentRequestId={currentRequestId}
              taskStatus={taskStatus}
              progressPct={progressPct}
              progressTitle={progressTitle}
              progressStepDesc={progressStepDesc}
              counts={counts}
              queuedRequests={queuedRequests}
              completedBatches={completedBatches}
              onInspectBatch={inspectCompletedBatch}
              onDownloadCsv={downloadBatchCsv}
              onRefreshCompleted={fetchCompletedBatches}
              isLoadingCompleted={isLoadingCompleted}
              toast={toast}
              onDismissToast={clearToast}
              onTerminate={terminateProcess}
              isTerminating={isTerminating}
              onCancelQueuedBatch={cancelQueuedBatch}
            />
          </div>
        </div>

        {/* Extracted Records Preview Telemetry Table */}
        <DataTable
          currentRequestId={currentRequestId}
          scrapeData={scrapeData}
          tableFields={tableFields}
          onDownloadCsv={downloadBatchCsv}
        />
      </main>

      {/* Cyber Engineering Modal Popup Dialog (Black, White, Green) */}
      <ModalPopup {...modalConfig} onCancel={closeModal} />
    </div>
  );
}
