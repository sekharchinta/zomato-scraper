import React, { useState, useEffect } from 'react';
import { Sliders, ChevronDown } from 'lucide-react';
import { api } from '../api/client';

export const FALLBACK_DEFAULT_FIELDS = [
  'restaurant_id',
  'name',
  'city',
  'location',
  'rating',
  'votes',
  'cost_for_two',
  'cuisines',
  'address',
  'phone_numbers',
  'timing',
  'page_url',
  'canonical_url',
  'total_reviews',
  'scraped_at',
];

export const FALLBACK_ALL_FIELDS = [
  'restaurant_id',
  'name',
  'city',
  'location',
  'rating',
  'votes',
  'cost_for_two',
  'cuisines',
  'address',
  'phone_numbers',
  'timing',
  'page_url',
  'canonical_url',
  'total_reviews',
  'scraped_at',
  'latitude',
  'longitude',
  'highlights',
  'photos_count',
  'reviews',
];

export function FieldSelector({ selectedFields, onSelectedFieldsChange }) {
  const [isOpen, setIsOpen] = useState(false);
  const [allFields, setAllFields] = useState(FALLBACK_ALL_FIELDS);
  const [defaultFields, setDefaultFields] = useState(FALLBACK_DEFAULT_FIELDS);

  useEffect(() => {
    async function loadFields() {
      try {
        const res = await api.getSchemaFields();
        if (res.ok && res.data && res.data.status === 'success') {
          if (Array.isArray(res.data.all_fields)) setAllFields(res.data.all_fields);
          if (Array.isArray(res.data.default_fields)) setDefaultFields(res.data.default_fields);
        }
      } catch (e) {
        // use fallbacks
      }
    }
    loadFields();
  }, []);

  const handleToggle = (field) => {
    if (selectedFields.includes(field)) {
      onSelectedFieldsChange(selectedFields.filter((f) => f !== field));
    } else {
      onSelectedFieldsChange([...selectedFields, field]);
    }
  };

  const handleSelectAll = () => {
    onSelectedFieldsChange([...allFields]);
  };

  const handleResetDefault = () => {
    onSelectedFieldsChange([...defaultFields]);
  };

  const handleDeselectAll = () => {
    onSelectedFieldsChange([]);
  };

  return (
    <div className="border-2 border-zinc-700 rounded-xl overflow-hidden bg-[#070a10] transition shadow-md">
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="w-full px-3.5 py-2.5 flex justify-between items-center text-left hover:bg-zinc-900/60 transition cursor-pointer select-none"
      >
        <div className="flex items-center gap-2">
          <Sliders className="w-3.5 h-3.5 text-emerald-400" />
          <span className="text-xs font-semibold text-zinc-100">Schema Field Architecture</span>
          <span className="font-mono text-[10px] bg-emerald-950/80 text-emerald-300 border-2 border-emerald-500/50 px-2 py-0.5 rounded-md font-semibold">
            {selectedFields.length} / {allFields.length} ACTIVE
          </span>
        </div>
        <ChevronDown
          className={`w-3.5 h-3.5 text-zinc-400 transition-transform duration-200 ${
            isOpen ? 'rotate-180' : ''
          }`}
        />
      </button>

      {isOpen && (
        <div className="p-3.5 border-t-2 border-zinc-700 bg-[#0a0e17] space-y-2.5">
          <div className="flex justify-between items-center pb-2 border-b border-zinc-700">
            <span className="font-mono text-[10px] font-semibold text-zinc-400 uppercase tracking-wider">
              DATA FIELDS ({allFields.length})
            </span>
            <div className="flex gap-2 text-xs font-mono">
              <button
                type="button"
                onClick={handleSelectAll}
                className="text-emerald-400 hover:text-emerald-300 font-semibold cursor-pointer"
              >
                All
              </button>
              <span className="text-zinc-600">&bull;</span>
              <button
                type="button"
                onClick={handleResetDefault}
                className="text-zinc-300 hover:text-white font-semibold cursor-pointer"
              >
                Default
              </button>
              <span className="text-zinc-600">&bull;</span>
              <button
                type="button"
                onClick={handleDeselectAll}
                className="text-zinc-400 hover:text-zinc-200 cursor-pointer"
              >
                Clear
              </button>
            </div>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-3 gap-1.5 max-h-48 overflow-y-auto pr-1 scrollbar-thin scrollbar-thumb-zinc-700">
            {allFields.map((f) => {
              const isChecked = selectedFields.includes(f);
              return (
                <label
                  key={f}
                  className={`flex items-center space-x-1.5 p-1.5 rounded-lg text-xs transition cursor-pointer select-none font-mono ${
                    isChecked
                      ? 'bg-emerald-950/60 border-2 border-emerald-500 text-emerald-300 font-medium shadow-xs'
                      : 'border-2 border-zinc-700/80 bg-zinc-900/40 hover:border-zinc-500 hover:bg-zinc-900 text-zinc-300'
                  }`}
                >
                  <input
                    type="checkbox"
                    value={f}
                    checked={isChecked}
                    onChange={() => handleToggle(f)}
                    className="w-3.5 h-3.5 accent-emerald-500 rounded border-zinc-600 focus:ring-0 cursor-pointer"
                  />
                  <span className="truncate text-[11px]">{f}</span>
                </label>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
