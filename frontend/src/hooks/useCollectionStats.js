import { useState, useEffect, useCallback } from 'react';
import { api } from '../api/client';

export function useCollectionStats(options = {}) {
  // Support both useCollectionStats(8000) and useCollectionStats({ user, pollIntervalMs })
  const pollIntervalMs = typeof options === 'number' ? options : (options?.pollIntervalMs || 8000);
  const user = typeof options === 'object' ? options?.user : null;
  const userId = user?.id;

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
  }, [refreshStats, pollIntervalMs, userId]);

  return { stats, refreshStats };
}
