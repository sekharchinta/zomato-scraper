import { useState, useCallback } from 'react';
import { api } from '../api/client';

export function useLocationSearch() {
  const [suggestions, setSuggestions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const searchLocations = useCallback(async (city) => {
    const cleanCity = (city || '').trim();
    if (!cleanCity) {
      setSuggestions([]);
      return [];
    }

    setLoading(true);
    setError(null);

    try {
      const res = await api.searchLocations(cleanCity);
      if (res.ok && res.data && res.data.status === 'success') {
        const sugs = res.data.suggestions || [];
        setSuggestions(sugs);
        return sugs;
      } else {
        const msg = res.data?.message || 'Could not discover localities for this city.';
        setError(msg);
        setSuggestions([]);
        return [];
      }
    } catch (err) {
      console.warn('Location search error:', err);
      setError('Network error while searching locations.');
      setSuggestions([]);
      return [];
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    suggestions,
    loading,
    error,
    searchLocations,
  };
}
