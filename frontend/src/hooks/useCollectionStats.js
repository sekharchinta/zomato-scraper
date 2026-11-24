import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';

export function useCollectionStats(pollIntervalMs = 8000) {
  const [stats, setStats] = useState({
    total_batches: 0,
    restaurants_urls: 0,
    restaurants_data: 0,
    delivery_data: 0,
  });

  const refreshStats = useCallback(async () => {
    try {
      const res = await api.getCollectionStats();
      if (res.ok && res.data && res.data.stats) {
        setStats(res.data.stats);
      }
    } catch (err) {
      console.warn('Failed to fetch collection stats:', err);
    }
  }, []);

  useEffect(() => {
    refreshStats();
    const timer = setInterval(refreshStats, pollIntervalMs);
    return () => clearInterval(timer);
  }, [refreshStats, pollIntervalMs]);

  return { stats, refreshStats };
}
