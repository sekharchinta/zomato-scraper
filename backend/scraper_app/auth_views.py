"""
Authentication views for Google OAuth 2.0 Integration.
Provides endpoints for Google configuration, token verification, session login,
session inspection, and logout.
"""

import functools
import json
import logging
from typing import Any, Callable, Dict

from django.conf import settings
from django.contrib.auth import login, logout
from django.contrib.auth.models import User
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from scraper_app.models import UserProfile

logger = logging.getLogger("auth")


def api_login_required(view_func: Callable) -> Callable:
    """
    Decorator for REST API views requiring user authentication.
    Returns HTTP 401 Unauthorized with standardized JSON error payload if not authenticated.
    """
    @functools.wraps(view_func)
    def _wrapped_view(request, *args, **kwargs):
        if not getattr(request, "user", None) or not request.user.is_authenticated:
            return JsonResponse(
                {
                    "status": "error",
                    "code": "unauthenticated",
                    "message": "Authentication required. Please sign in with Google.",
                },
                status=401,
            )
        return view_func(request, *args, **kwargs)
    return _wrapped_view


@require_GET
def google_auth_config(request) -> JsonResponse:
    """Returns Google Client ID dynamically to frontend."""
    return JsonResponse(
        {
            "client_id": getattr(settings, "GOOGLE_CLIENT_ID", ""),
            "status": "success",
        }
    )


@csrf_exempt
@require_POST
def google_auth_callback(request) -> JsonResponse:
    """
    Validates Google ID Token received from Google Identity Services (GIS).
    Creates or updates the User and UserProfile, then initializes a Django session.
    """
    try:
        body = json.loads(request.body.decode("utf-8") or "{}")
        credential = body.get("credential")

        if not credential:
            return JsonResponse({"status": "error", "message": "Missing Google ID token credential"}, status=400)

        google_client_id = getattr(settings, "GOOGLE_CLIENT_ID", "")
        if not google_client_id:
            return JsonResponse({"status": "error", "message": "Server Google Client ID not configured"}, status=500)

        # Verify Google ID Token
        id_info = id_token.verify_oauth2_token(
            credential,
            google_requests.Request(),
            google_client_id,
        )

        google_id = id_info.get("sub")
        email = id_info.get("email", "").strip().lower()
        name = id_info.get("name", "")
        picture = id_info.get("picture", "")
        email_verified = id_info.get("email_verified", True)

        if not google_id or not email:
            return JsonResponse({"status": "error", "message": "Invalid Google token payload"}, status=400)

        # First, search User by profile google_id
        profile = UserProfile.objects.filter(google_id=google_id).first()
        if profile:
            user = profile.user
            if email and user.email != email:
                user.email = email
                user.save(update_fields=["email"])
        else:
            # Look up existing user by email, or create new user
            user = User.objects.filter(email=email).first()
            if not user:
                # Use email or username derived from email
                username = email.split("@")[0]
                base_username = username
                counter = 1
                while User.objects.filter(username=username).exists():
                    username = f"{base_username}_{counter}"
                    counter += 1

                user = User.objects.create(
                    username=username,
                    email=email,
                    is_active=True,
                )

            # Associate / create UserProfile
            profile, _ = UserProfile.objects.update_or_create(
                user=user,
                defaults={
                    "google_id": google_id,
                    "avatar_url": picture,
                    "email_verified": bool(email_verified),
                },
            )

        # Update name if provided
        if name:
            parts = name.split(" ", 1)
            first_name = parts[0]
            last_name = parts[1] if len(parts) > 1 else ""
            if user.first_name != first_name or user.last_name != last_name:
                user.first_name = first_name
                user.last_name = last_name
                user.save(update_fields=["first_name", "last_name"])

        # Update profile avatar if changed
        if picture and profile.avatar_url != picture:
            profile.avatar_url = picture
            profile.save(update_fields=["avatar_url"])

        # Log in the user into the Django session
        login(request, user)

        display_name = f"{user.first_name} {user.last_name}".strip() or user.username
        logger.info(f"User '{user.email}' logged in successfully via Google OAuth 2.0 (Google ID: {google_id})")

        return JsonResponse(
            {
                "status": "success",
                "message": "Authenticated successfully",
                "user": {
                    "id": user.id,
                    "email": user.email,
                    "name": display_name,
                    "avatar": profile.avatar_url,
                    "is_staff": user.is_staff,
                },
            }
        )

    except ValueError as val_err:
        logger.warning(f"Google OAuth ID token verification failed: {val_err}")
        return JsonResponse({"status": "error", "message": f"Token verification failed: {str(val_err)}"}, status=401)
    except Exception as exc:
        logger.exception(f"Unexpected error in Google OAuth callback: {exc}")
        return JsonResponse({"status": "error", "message": f"Authentication error: {str(exc)}"}, status=500)


@require_GET
def get_current_user(request) -> JsonResponse:
    """Returns currently authenticated user session details, or unauthenticated status."""
    if not request.user.is_authenticated:
        return JsonResponse({"authenticated": False, "user": None})

    user = request.user
    avatar = ""
    try:
        if hasattr(user, "profile"):
            avatar = user.profile.avatar_url or ""
    except Exception:
        avatar = ""

    display_name = f"{user.first_name} {user.last_name}".strip() or user.username

    return JsonResponse(
        {
            "authenticated": True,
            "user": {
                "id": user.id,
                "email": user.email,
                "name": display_name,
                "avatar": avatar,
                "is_staff": user.is_staff,
            },
        }
    )


@csrf_exempt
@require_POST
def logout_view(request) -> JsonResponse:
    """Terminates user session and removes authentication cookies."""
    user_email = request.user.email if request.user.is_authenticated else "Anonymous"
    logout(request)
    logger.info(f"User '{user_email}' logged out successfully.")
    return JsonResponse({"status": "success", "message": "Logged out successfully"})
