import React, { useState, useEffect } from 'react';
import { Search, Building2, Play, Loader2, SlidersHorizontal } from 'lucide-react';
import { FieldSelector, FALLBACK_DEFAULT_FIELDS } from './FieldSelector';

export function ExtractionController({
  onStartPipeline,
  isSubmitting,
  suggestions,
  isSearchingLocations,
  onSearchLocations,
}) {
  const [city, setCity] = useState('Hyderabad');
  const [selectedLocation, setSelectedLocation] = useState('ALL');
  const [selectedFields, setSelectedFields] = useState(FALLBACK_DEFAULT_FIELDS);

  // Load saved city or search on mount
  useEffect(() => {
    const saved = localStorage.getItem('zomato_active_city') || 'Hyderabad';
    setCity(saved);
    onSearchLocations(saved);
  }, []);

  // Automatically sync selected locality whenever new suggestions arrive for a city
  useEffect(() => {
    if (suggestions && suggestions.length > 0) {
      setSelectedLocation(JSON.stringify(suggestions[0]));
    } else {
      setSelectedLocation('ALL');
    }
  }, [suggestions]);

  const handleSearchCity = () => {
    if (!city.trim()) return;
    localStorage.setItem('zomato_active_city', city.trim());
    onSearchLocations(city.trim());
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      handleSearchCity();
    }
  };

  const handleStart = () => {
    let locPayload = selectedLocation;
    let reqId = null;
    if (selectedLocation && selectedLocation !== 'ALL') {
      try {
        locPayload = JSON.parse(selectedLocation);
        reqId = locPayload.request_id;
      } catch (e) {
        locPayload = selectedLocation;
      }
    } else if (suggestions.length > 0) {
      reqId = suggestions[0].request_id;
    }
    onStartPipeline({
      city: city.trim(),
      location: locPayload,
      selectedFields,
      requestId: reqId,
    });
  };

  return (
    <div className="bg-[#0e1422] rounded-xl border-2 border-zinc-700 shadow-xl overflow-hidden">
      {/* Precision Header with Distinct High-Contrast Border */}
      <div className="px-5 py-3.5 border-b-2 border-zinc-700 flex items-center justify-between bg-[#0a0e17]">
        <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-lg bg-emerald-500/15 text-emerald-400 border-2 border-emerald-500/40 flex items-center justify-center">
            <SlidersHorizontal className="w-3.5 h-3.5" />
          </div>
          <div>
            <h2 className="text-xs font-bold text-white tracking-tight">
              Extraction Configuration
            </h2>
            <p className="text-[10px] font-mono text-zinc-400">
              SETUP PARAMETERS & TARGET
            </p>
          </div>
        </div>
        <span className="font-mono text-[10px] font-semibold text-emerald-300 bg-emerald-950/80 border-2 border-emerald-500/40 px-2 py-0.5 rounded-md">
          STEP 01 // DISPATCH
        </span>
      </div>

      <div className="p-5 space-y-4">
        {/* 1. City Input with Visible Border */}
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <label htmlFor="city-input" className="text-[11px] font-mono font-semibold text-zinc-200 tracking-wider uppercase flex items-center gap-1.5">
              <span>Target City</span>
              <span className="text-emerald-400">*</span>
            </label>
            <span className="text-[10px] font-mono text-zinc-400">INPUT: TEXT</span>
          </div>

          <div className="relative">
            <span className="absolute inset-y-0 left-0 pl-3.5 flex items-center pointer-events-none text-zinc-400">
              <Building2 className="w-4 h-4 text-zinc-400" />
            </span>
            <input
              id="city-input"
              type="text"
              value={city}
              onChange={(e) => setCity(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="e.g. Hyderabad, Mumbai, Bangalore..."
              className="w-full pl-9 pr-36 py-2.5 bg-[#070a10] border-2 border-zinc-700 rounded-lg text-white placeholder-zinc-500 focus:border-emerald-400 focus:ring-1 focus:ring-emerald-400/40 transition text-xs font-medium outline-hidden"
            />
            <button
              onClick={handleSearchCity}
              disabled={isSearchingLocations}
              type="button"
              className="absolute right-1.5 top-1.5 bottom-1.5 px-3.5 bg-emerald-600 hover:bg-emerald-500 active:scale-95 disabled:opacity-60 text-black font-bold font-mono rounded-md text-xs transition flex items-center gap-1.5 cursor-pointer border border-emerald-400/60 shadow-[0_0_10px_rgba(16,185,129,0.3)]"
            >
              {isSearchingLocations ? (
                <>
                  <Loader2 className="w-3 h-3 animate-spin text-black" />
                  <span>Searching...</span>
                </>
              ) : (
                <>
                  <Search className="w-3 h-3 text-black" />
                  <span>Find Localities</span>
                </>
              )}
            </button>
          </div>
        </div>

        {/* 2. Target Locality Select with Visible Border */}
        <div>
          <div className="flex items-center justify-between mb-1.5">
            <label htmlFor="location-select" className="text-[11px] font-mono font-semibold text-zinc-200 tracking-wider uppercase flex items-center gap-1.5">
              <span>Target Locality</span>
              <span className="text-emerald-400">*</span>
            </label>
            <span className="text-[10px] font-mono text-zinc-400">
              {suggestions.length > 0 ? `${suggestions.length} FOUND` : 'GEOGRAPHY'}
            </span>
          </div>

          <div className="relative">
            <select
              id="location-select"
              value={selectedLocation}
              onChange={(e) => setSelectedLocation(e.target.value)}
              className="w-full px-3.5 py-2.5 bg-[#070a10] border-2 border-zinc-700 rounded-lg text-white focus:border-emerald-400 focus:ring-1 focus:ring-emerald-400/40 transition text-xs font-medium cursor-pointer outline-hidden"
            >
              <option value="ALL" className="bg-[#0e1422] text-white">
                🌐 All Locations in {city || 'City'}
              </option>
              {suggestions.map((s, idx) => {
                const subtitle =
                  s.entity_name && s.entity_name !== s.entity_title ? ` (${s.entity_name})` : '';
                return (
                  <option key={idx} value={JSON.stringify(s)} className="bg-[#0e1422] text-white">
                    📍 {s.entity_title}
                    {subtitle}
                  </option>
                );
              })}
            </select>
          </div>
          <p className="text-[11px] font-mono text-zinc-400 mt-1.5 truncate">
            {suggestions.length > 0
              ? `[STATUS: ${suggestions.length} localities mapped for "${city}"]`
              : `Select a targeted locality or process full municipality.`}
          </p>
        </div>

        {/* Field Customization Architecture */}
        <FieldSelector
          selectedFields={selectedFields}
          onSelectedFieldsChange={setSelectedFields}
        />

        {/* Start Extraction Action Button (Vibrant Emerald Gradient + Border + Glow) */}
        <div className="pt-1">
          <button
            onClick={handleStart}
            disabled={isSubmitting}
            type="button"
            className="w-full py-3.5 bg-gradient-to-r from-emerald-500 via-emerald-600 to-teal-500 hover:from-emerald-400 hover:to-teal-400 active:scale-[0.98] disabled:opacity-60 text-black rounded-xl font-black text-xs tracking-wider border-2 border-emerald-400 shadow-[0_0_22px_rgba(16,185,129,0.35)] transition-all duration-200 flex items-center justify-center gap-2 cursor-pointer relative overflow-hidden group"
          >
            {/* Shimmer light effect */}
            <span className="absolute inset-0 w-1/2 h-full bg-white/25 skew-x-12 -translate-x-full group-hover:animate-shimmer pointer-events-none"></span>

            {isSubmitting ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin text-black" />
                <span className="font-mono">INITIALIZING PIPELINE...</span>
              </>
            ) : (
              <>
                <Play className="w-4 h-4 fill-current group-hover:scale-110 transition-transform" />
                <span className="font-mono tracking-wider">START EXTRACTION PIPELINE</span>
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}
