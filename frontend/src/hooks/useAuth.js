import { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../api/client';

export function useAuth() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [authError, setAuthError] = useState(null);
  const [clientId, setClientId] = useState('');
  const gisInitializedRef = useRef(false);

  // Check existing session on load
  const checkSession = useCallback(async () => {
    try {
      const res = await api.getCurrentUser();
      if (res.ok && res.data && res.data.authenticated) {
        setUser(res.data.user);
      } else {
        setUser(null);
      }
    } catch (err) {
      console.error('Session check failed:', err);
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  // Fetch Google Client ID config
  const fetchConfig = useCallback(async () => {
    try {
      const res = await api.getGoogleAuthConfig();
      if (res.ok && res.data && res.data.client_id) {
        setClientId(res.data.client_id);
      }
    } catch (err) {
      console.error('Failed to fetch Google auth config:', err);
    }
  }, []);

  useEffect(() => {
    checkSession();
    fetchConfig();
  }, [checkSession, fetchConfig]);

  // Handle Google credential callback
  const handleCredentialResponse = useCallback(async (response) => {
    if (!response || !response.credential) {
      setAuthError('No credential received from Google.');
      return;
    }
    setLoading(true);
    setAuthError(null);
    try {
      const result = await api.sendGoogleCredential(response.credential);
      if (result.ok && result.data && result.data.status === 'success') {
        setUser(result.data.user);
      } else {
        setAuthError(result.data?.message || 'Google sign-in failed on server.');
      }
    } catch (err) {
      setAuthError(err.message || 'Network error during Google authentication.');
    } finally {
      setLoading(false);
    }
  }, []);

  // Initialize and render Google button into a container DOM element
  const renderGoogleButton = useCallback((containerElement) => {
    if (!containerElement || !clientId) return;

    const checkGisLoaded = () => {
      if (window.google && window.google.accounts && window.google.accounts.id) {
        if (!gisInitializedRef.current) {
          window.google.accounts.id.initialize({
            client_id: clientId,
            callback: handleCredentialResponse,
            auto_select: false,
            cancel_on_tap_outside: true,
          });
          gisInitializedRef.current = true;
        }

        // Render the official Google Sign-In button
        containerElement.innerHTML = '';
        window.google.accounts.id.renderButton(containerElement, {
          theme: 'filled_black',
          size: 'medium',
          shape: 'pill',
          text: 'signin_with',
          logo_alignment: 'left',
        });
      } else {
        // Retry if Google script is still loading
        setTimeout(checkGisLoaded, 150);
      }
    };

    checkGisLoaded();
  }, [clientId, handleCredentialResponse]);

  // Logout handler
  const logout = useCallback(async () => {
    setLoading(true);
    try {
      await api.logout();
      setUser(null);
      if (window.google?.accounts?.id?.disableAutoSelect) {
        window.google.accounts.id.disableAutoSelect();
      }
    } catch (err) {
      console.error('Logout error:', err);
    } finally {
      setLoading(false);
    }
  }, []);

  return {
    user,
    loading,
    authError,
    isAuthenticated: !!user,
    renderGoogleButton,
    logout,
    refreshSession: checkSession,
  };
}
