"""
Zomato Restaurant Scraper with Keyword-to-Latlums Auto-Discovery & MongoDB Storage.

Workflow:
1. Input: City name and optional location (e.g. 'hydera', 'Hyderabad', 'Bengaluru').
2. Search API: Calls webroutes/location/search?q={city} to get location suggestions.
3. Latlums Scraping: Finds the nearest keyword match to the search input and extracts
   its latlums (latitude & longitude).
4. Location Resolution: Resolves exact location metadata (entityId, cityId, etc.) and
   canonical location URL via webroutes/location/get.
5. Location-Specific Output Folder: Creates output/{location}_{city}/ for each scraped location.
6. Restaurants API: Paginates webroutes/search/home:
   - solr_offset increases by 12
   - page increases by 1
   - Scrapes up to N pages OR until a page contains no restaurants with status 200.
7. File Persistence: Saves full JSON responses into output/{location}_{city}/{offset}_{page}.json.
8. MongoDB Database Persistence:
   - Connects to MongoDB server.
   - Database: 'zomato'
   - Collection: 'restaurants'
   - Primary Key: '_id' is set to restaurant ID ('restaurant_id') enforcing strict uniqueness.
   - Automatically stores/upserts every restaurant on scrape.
"""

import argparse
from datetime import datetime, timezone
import json
import logging
import re
import sys
import time
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote

import requests

try:
    import pymongo
    from pymongo import ReplaceOne
    PYMONGO_AVAILABLE = True
except ImportError:
    pymongo = None  # type: ignore
    ReplaceOne = None  # type: ignore
    PYMONGO_AVAILABLE = False

from config import (
    BASE_URL,
    CSRF_API_URL,
    DEFAULT_HEADERS,
    DEFAULT_MONGO_COLLECTION,
    DEFAULT_MONGO_DETAILS_COLLECTION,
    DEFAULT_MONGO_REVIEWS_COLLECTION,
    DEFAULT_MONGO_DELIVERY_COLLECTION,
    DEFAULT_MONGO_DB,
    DEFAULT_MONGO_URI,
    DETAILS_DIR,
    REVIEWS_DIR,
    DELIVERY_DIR,
    LOCATION_GET_API_URL,
    LOCATION_SEARCH_API_URL,
    OFFSET_INCREMENT,
    PAGE_INCREMENT,
    RESTAURANTS_API_URL,
    RESTAURANT_DETAILS_API_URL,
    REVIEWS_API_URL,
    Config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("zomato_scraper")


def slugify(text: str) -> str:
    """Converts a text string into a clean lowercase snake_case slug."""
    cleaned = re.sub(r"[^\w\s-]", "", str(text))
    return re.sub(r"[\s_-]+", "_", cleaned).strip("_").lower()


def extract_zomato_preloaded_state(html_text: str) -> Optional[Dict[str, Any]]:
    """
    Extracts the window.__PRELOADED_STATE__ JSON object embedded inside Zomato
    HTML pages. Contains complete data for entities (REVIEWS, RATING, RESTAURANTS)
    and sections (SECTION_BASIC_INFO, SECTION_RES_DETAILS, SECTION_RES_CONTACT, SECTION_REVIEWS).
    Bypasses 401 UNAUTHORISED API blocks by reading server preloaded state.
    """
    if not html_text:
        return None

    # Pattern 1: window.__PRELOADED_STATE__ = JSON.parse("...");
    m1 = re.search(r'window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\((.*?)\);', html_text, re.DOTALL)
    if m1:
        raw = m1.group(1).strip()
        try:
            val = json.loads(raw)
            if isinstance(val, str):
                return json.loads(val)
            return val
        except Exception:
            pass

    # Pattern 2: window.__PRELOADED_STATE__ = { ... };
    m2 = re.search(r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});\s*(?:window\.|<\/script>)', html_text, re.DOTALL)
    if m2:
        try:
            return json.loads(m2.group(1))
        except Exception:
            pass

    # Pattern 3: <script id="__NEXT_DATA__">...</script>
    m3 = re.search(r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html_text, re.DOTALL)
    if m3:
        try:
            return json.loads(m3.group(1))
        except Exception:
            pass

    return None


CITY_URL_ALIASES = {
    "bengaluru": "bangalore",
    "bombay": "mumbai",
    "calcutta": "kolkata",
    "madras": "chennai",
    "cochin": "kochi",
    "trivandrum": "thiruvananthapuram",
    "gurgaon": "ncr",
    "noida": "ncr",
}


def fetch_city_html_fallback(
    session: Optional[requests.Session],
    city_query: str,
    timeout: int = 10,
) -> List[Dict[str, Any]]:
    """
    Universal fallback for location discovery and coordinates:
    Fetches the public HTML webpage https://www.zomato.com/{city_slug} and parses
    window.__PRELOADED_STATE__ for exact currentLocation coordinates (lat/lon)
    and any localities in SECTION_CITY_MAGIC_LINKS.
    Bypasses 401 Unauthorized API blocks.
    """
    clean_city = re.sub(r"[^\w\s-]", "", str(city_query)).strip()
    if not clean_city:
        return []
    base_slug = clean_city.replace(" ", "-").lower()

    # Determine candidate URL slugs (e.g. 'bengaluru' -> 'bangalore', 'amalapuram' -> 'amalapuram')
    slug_candidates = [base_slug]
    if base_slug in CITY_URL_ALIASES and CITY_URL_ALIASES[base_slug] not in slug_candidates:
        slug_candidates.insert(0, CITY_URL_ALIASES[base_slug])

    req_session = session or requests.Session()
    # Avoid inheriting previous city stickiness
    for cookie_name in ("fbcity", "locus", "ltv", "lty"):
        req_session.cookies.pop(cookie_name, None)

    for slug in slug_candidates:
        url = f"{BASE_URL}/{slug}"
        headers = dict(DEFAULT_HEADERS)
        headers["referer"] = BASE_URL + "/"
        try:
            resp = req_session.get(url, headers=headers, timeout=timeout)
            if resp.status_code != 200:
                continue

            st = extract_zomato_preloaded_state(resp.text)
            if not st:
                continue

            curr = st.get("location", {}).get("currentLocation", {})
            lat = curr.get("latitude") or curr.get("userDefinedLatitude")
            lon = curr.get("longitude") or curr.get("userDefinedLongitude")
            city_name = curr.get("cityName") or curr.get("entityName") or clean_city.title()
            entity_id = curr.get("cityId") or curr.get("entityId") or 0

            suggestions: List[Dict[str, Any]] = []
            if lat is not None and lon is not None:
                try:
                    lat_f = float(lat)
                    lon_f = float(lon)
                    suggestions.append({
                        "entity_id": entity_id,
                        "entity_type": "city",
                        "entity_title": city_name,
                        "entity_name": f"{city_name}, India",
                        "latitude": lat_f,
                        "longitude": lon_f,
                        "raw_suggestion": curr,
                    })
                except (ValueError, TypeError):
                    pass

            search_pages = st.get("pages", {}).get("search", {})
            for _, page_data in search_pages.items():
                if not isinstance(page_data, dict):
                    continue
                magic = page_data.get("sections", {}).get("SECTION_CITY_MAGIC_LINKS", {})
                group_data = magic.get("groupCityData", {}).get("groupData", [])
                for g in group_data:
                    name = str(g.get("name", "")).strip()
                    if name:
                        suggestions.append({
                            "entity_id": 0,
                            "entity_type": "subzone",
                            "entity_title": name,
                            "entity_name": f"{name}, {city_name}",
                            "latitude": float(lat) if lat else None,
                            "longitude": float(lon) if lon else None,
                            "raw_suggestion": g,
                        })

            if suggestions:
                return suggestions
        except Exception as exc:
            logger.warning("HTML fallback request failed for '%s' (url=%s): %s", city_query, url, exc)

    return []


def get_location_city_folder(location: str, city: str) -> str:
    """
    Constructs a sanitized '{location}_{city}' folder name.
    Examples:
      - location='prashanth nagar', city='Hyderabad' -> 'prashanth_nagar_hyderabad'
      - location='Majestic', city='Bengaluru'         -> 'majestic_bengaluru'
      - location='Hyderabad', city='Hyderabad'       -> 'hyderabad_hyderabad'
    """
    loc_slug = slugify(location) or "location"
    city_slug = slugify(city) or "city"
    if loc_slug.endswith(city_slug) and loc_slug != city_slug:
        return loc_slug
    return f"{loc_slug}_{city_slug}"


def extract_latlums_from_suggestion(suggestion: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Extracts latitude and longitude from a location suggestion item."""
    lat = suggestion.get("entity_latitude")
    if lat is None or lat == 0:
        place = suggestion.get("place")
        if isinstance(place, dict):
            lat = place.get("latitude")

    lon = suggestion.get("entity_longitude")
    if lon is None or lon == 0:
        place = suggestion.get("place")
        if isinstance(place, dict):
            lon = place.get("longitude")

    try:
        f_lat = float(lat) if lat is not None else None
        f_lon = float(lon) if lon is not None else None
        return f_lat, f_lon
    except (TypeError, ValueError):
        return None, None


def score_suggestion_proximity(query: str, suggestion: Dict[str, Any]) -> float:
    """
    Scores how closely a suggestion matches the search keyword.
    Considers title similarity, exact matching, administrative hierarchy (city/state),
    and distinguishes actual city localities from random streets/expressways in other states.
    """
    q = query.strip().lower()
    title = str(suggestion.get("entity_title") or suggestion.get("display_title") or "").strip().lower()
    name = str(suggestion.get("entity_name") or "").strip().lower()
    etype = str(suggestion.get("entity_type") or "").strip().lower()
    ltype = str(suggestion.get("location_type") or "").strip().lower()

    name_parts = [p.strip().lower() for p in name.split(",") if p.strip()]
    admin_parts = name_parts[1:] if len(name_parts) > 1 else []

    # Similarity ratios
    ratio_title = SequenceMatcher(None, q, title).ratio()
    ratio_name = SequenceMatcher(None, q, name).ratio()
    score = max(ratio_title, ratio_name)

    # Exact equality on title or administrative parts (city/state)
    if title == q:
        score += 1.0
    elif any(p == q or p == f"new {q}" or p == f"{q} ncr" for p in admin_parts):
        # The entity is actually located within the queried city/state
        score += 0.8
    elif title.startswith(q) or q.startswith(title):
        score += 0.3

    # Primary city/region bonus
    if etype == "city" or ltype == "geocode":
        score += 0.5
    elif etype == "delivery_subzone":
        score += 0.15

    # Penalize street/road/hospital/school/expressway/building when query is just a city name
    street_words = [" rd", " road", " school", " hospital", " eye ", " expressway", " expy", " highway", " building"]
    if any(k in title for k in street_words) and not any(k in q for k in ["rd", "road", "school", "hospital", "expressway"]):
        score -= 0.4

    return score


def find_nearest_suggestion(query: str, suggestions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Finds the suggestion with the nearest keyword match to the user's input."""
    valid_suggestions = [
        s for s in suggestions
        if extract_latlums_from_suggestion(s)[0] is not None
    ]
    if not valid_suggestions:
        raise ValueError(f"No suggestions with valid coordinates found for query: '{query}'")

    best_match = max(valid_suggestions, key=lambda s: score_suggestion_proximity(query, s))
    return best_match


def format_restaurant_document(
    item: Dict[str, Any],
    city: str = "",
    location: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Extracts structured restaurant details and full raw response for storage in MongoDB.
    Sets _id to restaurant_id (integer) to enforce uniqueness as primary key.
    """
    if not isinstance(item, dict):
        return None
    info = item.get("info", {})
    if not info or not isinstance(info, dict):
        return None

    raw_res_id = info.get("resId")
    if raw_res_id is None:
        return None

    try:
        res_id = int(raw_res_id)
    except (ValueError, TypeError):
        return None

    name = info.get("name")
    cuisines = [c.get("name") for c in info.get("cuisine", []) if isinstance(c, dict) and c.get("name")]

    rating_obj = info.get("rating", {})
    aggregate_rating = rating_obj.get("aggregate_rating") if isinstance(rating_obj, dict) else None
    rating_text = rating_obj.get("rating_text") if isinstance(rating_obj, dict) else None
    rating_subtitle = rating_obj.get("rating_subtitle") if isinstance(rating_obj, dict) else None
    votes = rating_obj.get("votes") if isinstance(rating_obj, dict) else None

    cft = info.get("cft", {})
    cost_text = cft.get("text") if isinstance(cft, dict) else None

    locality_info = info.get("locality", {})
    locality_name = locality_info.get("name") if isinstance(locality_info, dict) else ""

    timing_info = info.get("timing", {})
    timing_text = timing_info.get("text") if isinstance(timing_info, dict) else None

    card_action = item.get("cardAction") if isinstance(item.get("cardAction"), dict) else {}
    order = item.get("order") if isinstance(item.get("order"), dict) else {}
    action_info = order.get("actionInfo") if isinstance(order.get("actionInfo"), dict) else {}
    restaurant_url = (
        card_action.get("clickUrl")
        or action_info.get("clickUrl")
        or info.get("o2Url")
        or ""
    )
    if restaurant_url and not restaurant_url.startswith("http"):
        restaurant_url = f"{BASE_URL}{restaurant_url}"

    image_obj = info.get("image", {})
    image_url = image_obj.get("url") if isinstance(image_obj, dict) else None

    doc = {
        "_id": res_id,                      # Primary key in MongoDB (Strict Uniqueness)
        "restaurant_id": res_id,            # Explicit restaurant ID
        "name": name,
        "city": city,
        "location": location,
        "locality": locality_name,
        "rating": aggregate_rating,
        "rating_text": rating_text,
        "rating_subtitle": rating_subtitle,
        "votes": votes,
        "cost_for_two": cost_text,
        "cuisines": cuisines,
        "timing": timing_text,
        "url": restaurant_url,
        "image_url": image_url,
        "raw_data": item,                   # Complete raw restaurant response
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    return doc


def extract_restaurant_page_url(item_or_doc: Dict[str, Any]) -> Optional[str]:
    """
    Extracts the relative restaurant page_url path (e.g. '/hyderabad/exotica-banjara-hills/info')
    suitable for passing to Zomato's webroutes/getPage API.
    Handles raw search items, MongoDB documents, and parsed restaurant dictionaries.
    """
    if not isinstance(item_or_doc, dict):
        return None

    # Check direct page_url or url field
    for field in ("page_url", "url"):
        val = item_or_doc.get(field)
        if isinstance(val, str) and val.strip():
            url = val.strip()
            if url.startswith("http"):
                url = re.sub(r"^https?://[^/]+", "", url)
            if not url.startswith("/"):
                url = f"/{url}"
            return url

    # Check raw_data if this is a MongoDB document
    raw_data = item_or_doc.get("raw_data")
    target = raw_data if isinstance(raw_data, dict) else item_or_doc

    # Check cardAction.clickUrl
    card_action = target.get("cardAction")
    if isinstance(card_action, dict):
        click_url = card_action.get("clickUrl")
        if isinstance(click_url, str) and click_url.strip():
            url = click_url.strip()
            if url.startswith("http"):
                url = re.sub(r"^https?://[^/]+", "", url)
            if not url.startswith("/"):
                url = f"/{url}"
            return url

    # Check order.actionInfo.clickUrl
    order = target.get("order")
    if isinstance(order, dict):
        action_info = order.get("actionInfo")
        if isinstance(action_info, dict):
            click_url = action_info.get("clickUrl")
            if isinstance(click_url, str) and click_url.strip():
                url = click_url.strip()
                if url.startswith("http"):
                    url = re.sub(r"^https?://[^/]+", "", url)
                if not url.startswith("/"):
                    url = f"/{url}"
                return url

    # Check info
    info = target.get("info")
    if isinstance(info, dict):
        for k in ("o2Url", "clickUrl", "url", "orderUrl"):
            val = info.get(k)
            if isinstance(val, str) and val.strip():
                url = val.strip()
                if url.startswith("http"):
                    url = re.sub(r"^https?://[^/]+", "", url)
                if not url.startswith("/"):
                    url = f"/{url}"
                return url

    return None


def build_locus_cookie(city_id: Any, lat: float, lon: float, dsz_id: Any, city_name: str) -> str:
    """
    Constructs the URL-encoded locus cookie required for Zomato delivery searches.
    Encodes exact client localized coordinates and delivery subzone ID.
    """
    clean_city_id = int(city_id) if str(city_id).isdigit() else 0
    clean_dsz_id = int(dsz_id) if str(dsz_id).isdigit() else 0
    locus_dict = {
        "addressId": 0,
        "lat": float(lat),
        "lng": float(lon),
        "cityId": clean_city_id,
        "ltv": clean_city_id,
        "lty": "city",
        "fetchFromGoogle": False,
        "dszId": clean_dsz_id,
        "fen": f"{city_name}, India",
    }
    return quote(json.dumps(locus_dict, separators=(",", ":")))


def format_restaurant_detail_document(
    detail_data: Dict[str, Any],
    restaurant_id: Optional[int] = None,
    page_url: str = "",
    city: str = "",
    location: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Transforms the full JSON response from Zomato's webroutes/getPage API
    into a structured document ready for insertion into MongoDB collection 'restaurants_data'.
    Strictly enforces unique primary key _id = restaurant_id.
    """
    if not isinstance(detail_data, dict):
        return None

    page_info = detail_data.get("page_info") if isinstance(detail_data.get("page_info"), dict) else {}
    res_id = restaurant_id or page_info.get("resId")

    page_data = detail_data.get("page_data") if isinstance(detail_data.get("page_data"), dict) else {}
    sections = page_data.get("sections") if isinstance(page_data, dict) else {}
    if not isinstance(sections, dict):
        sections = {}

    basic_info = sections.get("SECTION_BASIC_INFO") if isinstance(sections.get("SECTION_BASIC_INFO"), dict) else {}
    contact = sections.get("SECTION_RES_CONTACT") if isinstance(sections.get("SECTION_RES_CONTACT"), dict) else {}
    header_details = sections.get("SECTION_RES_HEADER_DETAILS") if isinstance(sections.get("SECTION_RES_HEADER_DETAILS"), dict) else {}
    res_details = sections.get("SECTION_RES_DETAILS") if isinstance(sections.get("SECTION_RES_DETAILS"), dict) else {}
    rating_data = sections.get("SECTION_RATING_DATA") if isinstance(sections.get("SECTION_RATING_DATA"), dict) else {}

    if not res_id:
        res_id = basic_info.get("res_id")

    if not res_id:
        return None

    try:
        res_id = int(res_id)
    except (ValueError, TypeError):
        pass

    name = basic_info.get("name") or page_info.get("pageTitle", "").split(",")[0].strip() or page_info.get("name") or ""
    phone_details = contact.get("phoneDetails") if isinstance(contact.get("phoneDetails"), dict) else {}
    phone_str = phone_details.get("phoneStr") or ""
    address = contact.get("address") or basic_info.get("address") or ""
    cuisines = basic_info.get("cuisine_string") or ""
    timing = basic_info.get("timing", {}).get("timing_desc") if isinstance(basic_info.get("timing"), dict) else None

    rating_val = None
    rating_obj = basic_info.get("rating")
    if isinstance(rating_obj, dict):
        rating_val = rating_obj.get("aggregate_rating")

    doc = {
        "_id": res_id,                      # Primary key in MongoDB (Strict Uniqueness)
        "restaurant_id": res_id,            # Explicit restaurant ID
        "name": name,
        "city": city or contact.get("city_name") or "",
        "location": location or contact.get("locality_verbose") or "",
        "locality": contact.get("locality_verbose") or location or "",
        "address": address,
        "phone_numbers": phone_str,
        "cuisines": cuisines,
        "timing": timing,
        "rating": rating_val,
        "page_url": page_url or page_info.get("pageUrl") or "",
        "canonical_url": page_info.get("canonicalUrl") or "",
        "sections": {
            "basic_info": basic_info,
            "res_details": res_details,
            "contact": contact,
            "header_details": header_details,
            "rating_data": rating_data,
        },
        "raw_data": detail_data,            # Complete hotel response from getPage
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    return doc


def format_restaurant_reviews_document(
    restaurant_id: int,
    name: str = "",
    city: str = "",
    location: str = "",
    page_url: str = "",
    number_of_pages: int = 0,
    pages_scraped: int = 0,
    reviews: Optional[List[Dict[str, Any]]] = None,
    raw_reviews_by_id: Optional[Dict[str, Any]] = None,
    ratings: Optional[Dict[str, Any]] = None,
    comments: Optional[Dict[str, Any]] = None,
    raw_pages: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Consolidates all paginated customer reviews for a single restaurant into a unified document
    ready for insertion into MongoDB collection 'reviews'.
    Strictly enforces unique primary key _id = restaurant_id so that each restaurant has exactly
    one consolidated document with all review pages merged together.
    """
    try:
        res_id = int(restaurant_id)
    except (ValueError, TypeError):
        res_id = restaurant_id

    rev_list = reviews if reviews is not None else []
    doc = {
        "_id": res_id,                      # Primary key in MongoDB (Strict Uniqueness per restaurant)
        "restaurant_id": res_id,            # Explicit restaurant ID
        "name": name,
        "city": city,
        "location": location,
        "page_url": page_url,
        "total_reviews": len(rev_list),
        "number_of_pages": number_of_pages,
        "pages_scraped": pages_scraped,
        "reviews": rev_list,                # Consolidated list of all reviews across all pages
        "raw_reviews_by_id": raw_reviews_by_id if raw_reviews_by_id is not None else {},
        "ratings": ratings if ratings is not None else {},
        "comments": comments if comments is not None else {},
        "raw_pages": raw_pages if raw_pages is not None else [],
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }
    return doc


def build_delivery_data_document(
    detail_data: Dict[str, Any],
    reviews_data: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
    restaurant_id: Optional[int] = None,
    city: str = "",
    location: str = "",
    page_url: str = "",
) -> Optional[Dict[str, Any]]:
    """
    Parses restaurant hotel details and customer reviews into a standardized
    delivery document ready for insertion into MongoDB collection 'delivery_data'.

    Extracted and normalized fields:
      - restaurant_id (and primary key _id)
      - name
      - city
      - location
      - address
      - phone_numbers
      - timing
      - rating (general/overall rating)
      - canonical_url (and cannonical_url)
      - rating.dining: aggregate_rating, votes
      - rating.delivery: rating, reviewcount
      - static_map_url (and staticmapurl)
      - reviews: list of {user_name, rating, review_text, like_count} for all users
    """
    if not isinstance(detail_data, dict):
        return None

    # Handle sections / raw_data variations (from MongoDB document or raw getPage JSON)
    sections = detail_data.get("sections")
    if not isinstance(sections, dict):
        sections = detail_data.get("page_data", {}).get("sections", {})
    if not isinstance(sections, dict):
        sections = {}

    page_info = detail_data.get("page_info")
    if not isinstance(page_info, dict):
        page_info = detail_data.get("raw_data", {}).get("page_info", {})
    if not isinstance(page_info, dict):
        page_info = {}

    basic_info = sections.get("basic_info") or sections.get("SECTION_BASIC_INFO") or {}
    if not isinstance(basic_info, dict):
        basic_info = {}

    contact = sections.get("contact") or sections.get("SECTION_RES_CONTACT") or {}
    if not isinstance(contact, dict):
        contact = {}

    # Extract restaurant ID
    res_id = (
        restaurant_id
        or detail_data.get("restaurant_id")
        or detail_data.get("_id")
        or basic_info.get("res_id")
        or page_info.get("resId")
    )
    if res_id:
        try:
            res_id = int(res_id)
        except (ValueError, TypeError):
            pass

    if not res_id:
        return None

    # Core attributes
    name = (
        detail_data.get("name")
        or basic_info.get("name")
        or page_info.get("name")
        or (page_info.get("pageTitle", "").split(",")[0].strip() if page_info.get("pageTitle") else "")
        or str(res_id)
    )

    # Resolve City
    raw_city = city or detail_data.get("city")
    if isinstance(raw_city, str) and raw_city.strip():
        resolved_city = raw_city.strip()
    else:
        resolved_city = contact.get("city_name") or ""
        if not resolved_city:
            loc_dict = detail_data.get("location") if isinstance(detail_data.get("location"), dict) else {}
            resolved_city = loc_dict.get("cityName") or ""

    # Resolve Location
    raw_loc = location or detail_data.get("location") or detail_data.get("locality")
    if isinstance(raw_loc, str) and raw_loc.strip():
        resolved_location = raw_loc.strip()
    elif isinstance(raw_loc, dict):
        resolved_location = (
            raw_loc.get("locality_verbose")
            or raw_loc.get("entityName")
            or raw_loc.get("orderLocationName")
            or raw_loc.get("placeName")
            or ""
        )
    else:
        resolved_location = contact.get("locality_verbose") or ""

    if not resolved_location:
        resolved_location = contact.get("locality_verbose") or ""

    address = (
        detail_data.get("address")
        or contact.get("address")
        or basic_info.get("address")
        or ""
    )

    phone_numbers = (
        detail_data.get("phone_numbers")
        or contact.get("phoneDetails", {}).get("phoneStr")
        or ""
    )

    raw_timing = detail_data.get("timing") or basic_info.get("timing")
    if isinstance(raw_timing, dict):
        timing = raw_timing.get("timing_desc") or ""
    elif isinstance(raw_timing, str):
        timing = raw_timing
    else:
        timing = ""

    raw_rating = detail_data.get("rating")
    if raw_rating is not None and str(raw_rating).strip():
        rating = str(raw_rating).strip()
    else:
        b_rating = basic_info.get("rating")
        if isinstance(b_rating, dict):
            rating = str(b_rating.get("aggregate_rating") or "").strip()
        else:
            rating = str(b_rating or "").strip()

    canonical_url = (
        detail_data.get("canonical_url")
        or page_info.get("canonicalUrl")
        or detail_data.get("raw_data", {}).get("page_info", {}).get("canonicalUrl")
        or detail_data.get("page_url")
        or page_url
        or ""
    )

    # Ratings extraction: Dining (aggregate_rating, votes) and Delivery (rating, reviewcount)
    rating_new = basic_info.get("rating_new", {}) if isinstance(basic_info, dict) else {}
    ratings_map = rating_new.get("ratings", {}) if isinstance(rating_new, dict) else {}
    dining_obj = ratings_map.get("DINING") or ratings_map.get("dining") or {}
    if not isinstance(dining_obj, dict):
        dining_obj = {}

    basic_rating_obj = basic_info.get("rating", {}) if isinstance(basic_info, dict) else {}
    if not isinstance(basic_rating_obj, dict):
        basic_rating_obj = {}

    dining_agg_rating = str(
        dining_obj.get("rating")
        or dining_obj.get("ratingV2")
        or basic_rating_obj.get("aggregate_rating")
        or ""
    ).strip()

    dining_votes = (
        dining_obj.get("reviewCount")
        or basic_rating_obj.get("votes")
        or 0
    )

    delivery_obj = ratings_map.get("DELIVERY") or ratings_map.get("delivery") or {}
    if not isinstance(delivery_obj, dict):
        delivery_obj = {}

    delivery_rating = str(
        delivery_obj.get("rating")
        or delivery_obj.get("ratingV2")
        or ""
    ).strip()

    delivery_review_count = (
        delivery_obj.get("reviewCount")
        or 0
    )

    static_map_url = str(contact.get("static_map_url") or "").strip()

    # User reviews extraction
    reviews_list: List[Dict[str, Any]] = []
    raw_reviews = []
    if isinstance(reviews_data, dict):
        raw_reviews = reviews_data.get("reviews", [])
    elif isinstance(reviews_data, list):
        raw_reviews = reviews_data

    for rev in raw_reviews:
        if not isinstance(rev, dict):
            continue
        u_name = (
            rev.get("user_name")
            or rev.get("userName")
            or (rev.get("user", {}).get("name") if isinstance(rev.get("user"), dict) else "")
            or ""
        )
        # Resolve user rating (handles string, number, or entity reference object)
        raw_r = rev.get("rating")
        u_rating = ""
        if isinstance(raw_r, (int, float)):
            u_rating = str(raw_r)
        elif isinstance(raw_r, str) and raw_r.strip() and not raw_r.startswith("{"):
            u_rating = raw_r.strip()
        elif isinstance(raw_r, dict):
            # Resolve from entity_ids in ratings map
            entity_ids = []
            for ent in raw_r.get("entities", []):
                entity_ids.extend(ent.get("entity_ids", []))
            rev_ratings_map = (
                (reviews_data.get("ratings") if isinstance(reviews_data, dict) else {})
                or (detail_data.get("ratings") if isinstance(detail_data, dict) else {})
            )
            for eid in entity_ids:
                r_entry = rev_ratings_map.get(str(eid)) or rev_ratings_map.get(eid)
                if isinstance(r_entry, dict) and r_entry.get("rating") is not None:
                    u_rating = str(r_entry.get("rating"))
                    break
        if not u_rating:
            u_rating = str(rev.get("rating_text") or "").strip()
        u_text = str(rev.get("review_text") or rev.get("reviewText") or "").strip()
        raw_likes = rev.get("like_count") if rev.get("like_count") is not None else rev.get("likeCount", 0)
        try:
            u_likes = int(raw_likes)
        except (ValueError, TypeError):
            u_likes = 0

        reviews_list.append({
            "user_name": str(u_name),
            "rating": u_rating,
            "review_text": u_text,
            "like_count": u_likes,
        })

    # User specified requirement: total_reviews is the count of reviews which we scraped
    total_reviews = len(reviews_list)

    doc = {
        "_id": res_id,                                # Primary key in MongoDB collection 'delivery_data'
        "restaurant_id": res_id,
        "name": name,
        "city": resolved_city,
        "location": resolved_location,
        "address": address,
        "phone_numbers": phone_numbers,
        "timing": timing,
        "rating": rating,
        "canonical_url": canonical_url,
        "aggregate_rating": dining_agg_rating,
        "votes": dining_votes,
        "delivery_rating": delivery_rating,
        "reviewcount": delivery_review_count,
        "total_reviews": total_reviews,
        "static_map_url": static_map_url,
        "reviews": reviews_list,
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }
    return doc


class MongoDBManager:
    """
    Manages MongoDB connection, database 'zomato', and collections:
      - 'restaurants' (search results)
      - 'restaurants_data' (hotel details)
      - 'reviews' (all-page customer reviews consolidated per restaurant)
    Enforces uniqueness using restaurant_id as primary key (_id) and supports upsert operations.
    """

    def __init__(
        self,
        uri: str = DEFAULT_MONGO_URI,
        db_name: str = DEFAULT_MONGO_DB,
        collection_name: str = DEFAULT_MONGO_COLLECTION,
        details_collection_name: str = DEFAULT_MONGO_DETAILS_COLLECTION,
        reviews_collection_name: str = DEFAULT_MONGO_REVIEWS_COLLECTION,
        delivery_collection_name: str = DEFAULT_MONGO_DELIVERY_COLLECTION,
    ) -> None:
        self.uri = uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.details_collection_name = details_collection_name
        self.reviews_collection_name = reviews_collection_name
        self.delivery_collection_name = delivery_collection_name
        self.client: Any = None
        self.db: Any = None
        self.collection: Any = None
        self.details_collection: Any = None
        self.reviews_collection: Any = None
        self.delivery_collection: Any = None
        self._connected = False

    def connect(self) -> bool:
        """Connects to MongoDB, validates connection, and ensures collections and indexes exist."""
        if not PYMONGO_AVAILABLE:
            logger.warning("pymongo is not installed. MongoDB operations disabled.")
            self._connected = False
            return False

        try:
            self.client = pymongo.MongoClient(self.uri, serverSelectionTimeoutMS=3000)
            self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.details_collection = self.db[self.details_collection_name]
            self.reviews_collection = self.db[self.reviews_collection_name]
            self.delivery_collection = self.db[self.delivery_collection_name]

            # Ensure unique index on restaurant_id (in addition to primary key _id)
            self.collection.create_index("restaurant_id", unique=True)
            self.collection.create_index("city")
            self.collection.create_index("location")

            # Ensure indexes on restaurants_data collection
            self.details_collection.create_index("restaurant_id", unique=True)
            self.details_collection.create_index("city")
            self.details_collection.create_index("location")
            self.details_collection.create_index("page_url")

            # Ensure indexes on reviews collection
            self.reviews_collection.create_index("restaurant_id", unique=True)
            self.reviews_collection.create_index("city")
            self.reviews_collection.create_index("location")
            self.reviews_collection.create_index("name")

            # Ensure indexes on delivery_data collection
            self.delivery_collection.create_index("restaurant_id", unique=True)
            self.delivery_collection.create_index("city")
            self.delivery_collection.create_index("location")
            self.delivery_collection.create_index("name")

            self._connected = True
            logger.info(
                "Connected to MongoDB at %s (Database: '%s', Collections: '%s', '%s', '%s', '%s')",
                self.uri,
                self.db_name,
                self.collection_name,
                self.details_collection_name,
                self.reviews_collection_name,
                self.delivery_collection_name,
            )
            return True
        except Exception as exc:
            logger.warning("MongoDB connection failed (%s): %s", self.uri, exc)
            self._connected = False
            return False

    def is_connected(self) -> bool:
        return self._connected and self.collection is not None

    def get_total_count(self) -> int:
        if not self.is_connected():
            return 0
        try:
            return self.collection.count_documents({})
        except Exception:
            return 0

    def get_details_count(self) -> int:
        if not self.is_connected() or self.details_collection is None:
            return 0
        try:
            return self.details_collection.count_documents({})
        except Exception:
            return 0

    def get_reviews_count(self) -> int:
        if not self.is_connected() or self.reviews_collection is None:
            return 0
        try:
            return self.reviews_collection.count_documents({})
        except Exception:
            return 0

    def get_delivery_count(self) -> int:
        if not self.is_connected() or self.delivery_collection is None:
            return 0
        try:
            return self.delivery_collection.count_documents({})
        except Exception:
            return 0

    def upsert_restaurants(
        self,
        restaurants: List[Dict[str, Any]],
        city: str = "",
        location: str = "",
    ) -> Tuple[int, int]:
        """
        Upserts restaurant items into MongoDB 'restaurants' collection.
        Ensures uniqueness by setting _id = restaurant_id.
        Returns:
            Tuple[int, int]: (upserted_count, modified_count)
        """
        if not self.is_connected() or not restaurants:
            return 0, 0

        operations = []
        for item in restaurants:
            doc = format_restaurant_document(item, city=city, location=location)
            if doc:
                operations.append(
                    ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                )

        if not operations:
            return 0, 0

        try:
            result = self.collection.bulk_write(operations, ordered=False)
            upserted = result.upserted_count
            modified = result.modified_count
            return upserted, modified
        except Exception as exc:
            logger.error("Failed to write restaurants to MongoDB: %s", exc)
            return 0, 0

    def upsert_restaurant_detail(
        self,
        detail_data: Dict[str, Any],
        restaurant_id: Optional[int] = None,
        page_url: str = "",
        city: str = "",
        location: str = "",
    ) -> Tuple[int, int]:
        """
        Upserts a single restaurant detail response into MongoDB 'restaurants_data' collection.
        Enforces uniqueness using restaurant_id as primary key (_id).
        Returns:
            Tuple[int, int]: (upserted_count, modified_count)
        """
        if not self.is_connected() or self.details_collection is None or not detail_data:
            return 0, 0

        doc = format_restaurant_detail_document(
            detail_data=detail_data,
            restaurant_id=restaurant_id,
            page_url=page_url,
            city=city,
            location=location,
        )
        if not doc:
            return 0, 0

        try:
            res = self.details_collection.replace_one(
                {"_id": doc["_id"]},
                doc,
                upsert=True,
            )
            upserted = 1 if res.upserted_id is not None else 0
            modified = res.modified_count
            return upserted, modified
        except Exception as exc:
            logger.error("Failed to upsert restaurant detail for ID %s into MongoDB: %s", doc.get("_id"), exc)
            return 0, 0

    def upsert_restaurant_reviews(
        self,
        doc: Dict[str, Any],
    ) -> Tuple[int, int]:
        """
        Upserts a consolidated restaurant reviews document into MongoDB 'reviews' collection.
        Enforces uniqueness using restaurant_id as primary key (_id).
        All review pages are stored within this single document per restaurant.
        Returns:
            Tuple[int, int]: (upserted_count, modified_count)
        """
        if not self.is_connected() or self.reviews_collection is None or not doc:
            return 0, 0

        res_id = doc.get("_id") or doc.get("restaurant_id")
        if not res_id:
            return 0, 0

        try:
            res = self.reviews_collection.replace_one(
                {"_id": doc["_id"]},
                doc,
                upsert=True,
            )
            upserted = 1 if res.upserted_id is not None else 0
            modified = res.modified_count
            return upserted, modified
        except Exception as exc:
            logger.error("Failed to upsert reviews for restaurant ID %s into MongoDB: %s", doc.get("_id"), exc)
            return 0, 0

    def upsert_delivery_data(
        self,
        doc: Dict[str, Any],
    ) -> Tuple[int, int]:
        """
        Upserts a parsed delivery document into MongoDB 'delivery_data' collection.
        Enforces uniqueness using restaurant_id as primary key (_id).
        Returns:
            Tuple[int, int]: (upserted_count, modified_count)
        """
        if not self.is_connected() or self.delivery_collection is None or not doc:
            return 0, 0

        res_id = doc.get("_id") or doc.get("restaurant_id")
        if not res_id:
            return 0, 0

        try:
            res = self.delivery_collection.replace_one(
                {"_id": doc["_id"]},
                doc,
                upsert=True,
            )
            upserted = 1 if res.upserted_id is not None else 0
            modified = res.modified_count
            return upserted, modified
        except Exception as exc:
            logger.error("Failed to upsert delivery data for restaurant ID %s into MongoDB: %s", doc.get("_id"), exc)
            return 0, 0

    def sync_delivery_data(
        self,
        city: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Parses all existing records from 'restaurants_data' and 'reviews' collections
        and stores them into 'delivery_data' collection.
        """
        if not self.is_connected() or self.details_collection is None:
            return {"processed": 0, "upserted": 0, "modified": 0, "total_in_db": 0}

        query: Dict[str, Any] = {}
        if city:
            query["city"] = {"$regex": f"^{re.escape(city)}$", "$options": "i"}

        cursor = self.details_collection.find(query)
        if limit and limit > 0:
            cursor = cursor.limit(limit)

        processed = 0
        total_upserted = 0
        total_modified = 0

        for detail_doc in cursor:
            res_id = detail_doc.get("restaurant_id") or detail_doc.get("_id")
            if not res_id:
                continue

            # Fetch matching reviews document if available
            reviews_doc = None
            if self.reviews_collection is not None:
                try:
                    reviews_doc = self.reviews_collection.find_one({"restaurant_id": res_id})
                    if not reviews_doc:
                        reviews_doc = self.reviews_collection.find_one({"_id": res_id})
                except Exception:
                    pass

            deliv_doc = build_delivery_data_document(
                detail_data=detail_doc,
                reviews_data=reviews_doc,
                restaurant_id=res_id,
            )
            if deliv_doc:
                ups, mod = self.upsert_delivery_data(deliv_doc)
                total_upserted += ups
                total_modified += mod
                processed += 1

        return {
            "processed": processed,
            "upserted": total_upserted,
            "modified": total_modified,
            "total_in_db": self.get_delivery_count(),
        }

    def get_restaurants_for_details(
        self,
        city: Optional[str] = None,
        limit: Optional[int] = None,
        unscraped_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves restaurant records from collection 'restaurants' for detail scraping.
        If unscraped_only is True, filters out restaurant IDs already in 'restaurants_data'.
        """
        if not self.is_connected():
            return []

        query: Dict[str, Any] = {}
        if city:
            query["city"] = {"$regex": f"^{re.escape(city)}$", "$options": "i"}

        if unscraped_only and self.details_collection is not None:
            try:
                scraped_ids = self.details_collection.distinct("_id")
                if scraped_ids:
                    query["_id"] = {"$nin": scraped_ids}
            except Exception as exc:
                logger.warning("Could not query distinct IDs from details collection: %s", exc)

        cursor = self.collection.find(query)
        if limit and limit > 0:
            cursor = cursor.limit(limit)

        return list(cursor)

    def get_restaurants_for_reviews(
        self,
        city: Optional[str] = None,
        limit: Optional[int] = None,
        unscraped_only: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves restaurant records from collection 'restaurants' for review scraping.
        If unscraped_only is True, filters out restaurant IDs already in 'reviews'.
        """
        if not self.is_connected():
            return []

        query: Dict[str, Any] = {}
        if city:
            query["city"] = {"$regex": f"^{re.escape(city)}$", "$options": "i"}

        if unscraped_only and self.reviews_collection is not None:
            try:
                scraped_ids = self.reviews_collection.distinct("_id")
                if scraped_ids:
                    query["_id"] = {"$nin": scraped_ids}
            except Exception as exc:
                logger.warning("Could not query distinct IDs from reviews collection: %s", exc)

        cursor = self.collection.find(query)
        if limit and limit > 0:
            cursor = cursor.limit(limit)

        return list(cursor)

    def close(self) -> None:
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
            self._connected = False


def sync_output_folder_to_mongo(
    output_dir: Path,
    mongo_manager: MongoDBManager,
) -> Dict[str, Any]:
    """
    Scans all saved response JSON files in output/{location}_{city}/*.json
    and upserts all unique restaurants into MongoDB collection zomato.restaurants.
    """
    if not mongo_manager.is_connected():
        logger.error("Cannot sync to MongoDB: MongoDB is not connected.")
        return {"status": "error", "message": "MongoDB is not connected"}

    json_files = sorted(output_dir.glob("*/*.json"))
    logger.info("Found %d JSON file(s) in %s to sync into MongoDB...", len(json_files), output_dir)

    total_files = 0
    total_items = 0
    total_upserted = 0
    total_modified = 0

    for file_path in json_files:
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.warning("Failed to read JSON from %s: %s", file_path, exc)
            continue

        folder_name = file_path.parent.name
        # Infer location and city from folder name (e.g. table_space_ub_city_bengaluru)
        parts = folder_name.split("_")
        if len(parts) >= 2:
            inferred_city = parts[-1].capitalize()
            inferred_loc = " ".join(parts[:-1]).title()
        else:
            inferred_city = ""
            inferred_loc = folder_name

        sections = data.get("sections", {}) if isinstance(data, dict) else {}
        restaurants = sections.get("SECTION_SEARCH_RESULT", [])
        if not restaurants or not isinstance(restaurants, list):
            continue

        total_files += 1
        total_items += len(restaurants)

        upserted, modified = mongo_manager.upsert_restaurants(
            restaurants=restaurants,
            city=inferred_city,
            location=inferred_loc,
        )
        total_upserted += upserted
        total_modified += modified

    final_count = mongo_manager.get_total_count()
    logger.info(
        "Sync complete: %d file(s) processed, %d raw restaurant item(s), %d newly inserted, %d updated. Total in DB: %d",
        total_files,
        total_items,
        total_upserted,
        total_modified,
        final_count,
    )
    return {
        "status": "success",
        "files_processed": total_files,
        "items_processed": total_items,
        "upserted": total_upserted,
        "modified": total_modified,
        "total_in_db": final_count,
    }


def sync_details_folder_to_mongo(
    details_dir: Path,
    mongo_manager: MongoDBManager,
) -> Dict[str, Any]:
    """
    Scans all saved hotel detail JSON files in output_details/*/*.json (or output_details/*.json)
    and upserts all unique records into MongoDB collection 'restaurants_data'.
    """
    if not mongo_manager.is_connected() or mongo_manager.details_collection is None:
        logger.error("Cannot sync details to MongoDB: MongoDB is not connected.")
        return {"status": "error", "message": "MongoDB is not connected"}

    json_files = sorted(details_dir.glob("*/*.json"))
    if not json_files:
        json_files = sorted(details_dir.glob("*.json"))

    logger.info("Found %d hotel detail JSON file(s) in %s to sync into MongoDB...", len(json_files), details_dir)

    total_files = 0
    total_upserted = 0
    total_modified = 0

    operations = []
    for file_path in json_files:
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.warning("Failed to read JSON from %s: %s", file_path, exc)
            continue

        folder_name = file_path.parent.name
        parts = folder_name.split("_")
        if len(parts) >= 2:
            inferred_city = parts[-1].capitalize()
            inferred_loc = " ".join(parts[:-1]).title()
        else:
            inferred_city = ""
            inferred_loc = folder_name

        stem = file_path.stem
        res_id = int(stem) if stem.isdigit() else None

        doc = format_restaurant_detail_document(
            detail_data=data,
            restaurant_id=res_id,
            city=inferred_city,
            location=inferred_loc,
        )
        if doc:
            operations.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
            total_files += 1

    if operations:
        try:
            result = mongo_manager.details_collection.bulk_write(operations, ordered=False)
            total_upserted = result.upserted_count
            total_modified = result.modified_count
        except Exception as exc:
            logger.error("Failed to sync hotel details to MongoDB: %s", exc)

    final_count = mongo_manager.get_details_count()
    logger.info(
        "Details sync complete: %d file(s) processed, %d newly inserted, %d updated. Total in '%s': %d",
        total_files,
        total_upserted,
        total_modified,
        mongo_manager.details_collection_name,
        final_count,
    )
    return {
        "status": "success",
        "files_processed": total_files,
        "upserted": total_upserted,
        "modified": total_modified,
        "total_in_db": final_count,
    }


def sync_reviews_folder_to_mongo(
    reviews_dir: Path,
    mongo_manager: MongoDBManager,
) -> Dict[str, Any]:
    """
    Scans all saved reviews JSON files in output_reviews/*/*.json (or output_reviews/*.json)
    and upserts all consolidated records into MongoDB collection 'reviews'.
    Enforces uniqueness using restaurant_id as primary key (_id).
    """
    if not mongo_manager.is_connected() or mongo_manager.reviews_collection is None:
        logger.error("Cannot sync reviews to MongoDB: MongoDB is not connected.")
        return {"status": "error", "message": "MongoDB is not connected"}

    json_files = sorted(reviews_dir.glob("*/*.json"))
    if not json_files:
        json_files = sorted(reviews_dir.glob("*.json"))

    logger.info("Found %d review JSON file(s) in %s to sync into MongoDB...", len(json_files), reviews_dir)

    total_files = 0
    total_upserted = 0
    total_modified = 0

    operations = []
    for file_path in json_files:
        try:
            with file_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.warning("Failed to read JSON from %s: %s", file_path, exc)
            continue

        if not isinstance(data, dict):
            continue

        stem = file_path.stem
        res_id = data.get("_id") or data.get("restaurant_id")
        if not res_id and stem.isdigit():
            res_id = int(stem)

        if not res_id:
            continue

        try:
            res_id = int(res_id)
        except (ValueError, TypeError):
            pass

        data["_id"] = res_id
        data["restaurant_id"] = res_id
        operations.append(ReplaceOne({"_id": data["_id"]}, data, upsert=True))
        total_files += 1

    if operations:
        try:
            result = mongo_manager.reviews_collection.bulk_write(operations, ordered=False)
            total_upserted = result.upserted_count
            total_modified = result.modified_count
        except Exception as exc:
            logger.error("Failed to sync reviews to MongoDB: %s", exc)

    final_count = mongo_manager.get_reviews_count()
    logger.info(
        "Reviews sync complete: %d file(s) processed, %d newly inserted, %d updated. Total in '%s': %d",
        total_files,
        total_upserted,
        total_modified,
        mongo_manager.reviews_collection_name,
        final_count,
    )
    return {
        "status": "success",
        "files_processed": total_files,
        "upserted": total_upserted,
        "modified": total_modified,
        "total_in_db": final_count,
    }


def sync_delivery_folder_to_mongo(
    details_dir: Path,
    reviews_dir: Path,
    delivery_dir: Path,
    mongo_manager: Optional[MongoDBManager] = None,
) -> Dict[str, Any]:
    """
    Scans all hotel details and matching reviews, parses them using build_delivery_data_document,
    saves the parsed JSON documents into output_delivery/*/*.json, and upserts all unique records
    into MongoDB collection 'delivery_data'.
    """
    total_processed = 0
    total_upserted = 0
    total_modified = 0

    detail_files = sorted(details_dir.glob("*/*.json"))
    if not detail_files:
        detail_files = sorted(details_dir.glob("*.json"))

    logger.info("Parsing and syncing %d hotel detail file(s) into 'delivery_data'...", len(detail_files))

    operations = []
    seen_ids = set()

    for d_path in detail_files:
        try:
            with d_path.open("r", encoding="utf-8") as fh:
                detail_json = json.load(fh)
        except Exception as exc:
            logger.warning("Failed to read detail JSON from %s: %s", d_path, exc)
            continue

        folder_name = d_path.parent.name
        res_id_str = d_path.stem
        try:
            res_id: Any = int(res_id_str)
        except (ValueError, TypeError):
            res_id = res_id_str

        # Check matching review file on disk
        rev_data = None
        rev_path = reviews_dir / folder_name / f"{res_id_str}.json"
        if not rev_path.exists():
            matches = list(reviews_dir.glob(f"*/{res_id_str}.json"))
            if matches:
                rev_path = matches[0]

        if rev_path.exists():
            try:
                with rev_path.open("r", encoding="utf-8") as rfh:
                    rev_data = json.load(rfh)
            except Exception:
                pass

        # If not found on disk but Mongo is available, check Mongo
        if rev_data is None and mongo_manager and mongo_manager.is_connected() and mongo_manager.reviews_collection is not None:
            try:
                rev_data = mongo_manager.reviews_collection.find_one({"restaurant_id": res_id})
                if not rev_data:
                    rev_data = mongo_manager.reviews_collection.find_one({"_id": res_id})
            except Exception:
                pass

        deliv_doc = build_delivery_data_document(
            detail_data=detail_json,
            reviews_data=rev_data,
            restaurant_id=res_id,
        )
        if not deliv_doc:
            continue

        seen_ids.add(deliv_doc["_id"])

        # Save to output_delivery/{folder_name}/{res_id}.json
        target_folder = delivery_dir / folder_name
        target_folder.mkdir(parents=True, exist_ok=True)
        out_path = target_folder / f"{res_id}.json"
        try:
            with out_path.open("w", encoding="utf-8") as ofh:
                json.dump(deliv_doc, ofh, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning("Could not write delivery file %s: %s", out_path, exc)

        if mongo_manager and mongo_manager.is_connected() and mongo_manager.delivery_collection is not None:
            operations.append(ReplaceOne({"_id": deliv_doc["_id"]}, deliv_doc, upsert=True))

        total_processed += 1

    # Also process any remaining MongoDB restaurants_data documents not already processed from disk
    if mongo_manager and mongo_manager.is_connected() and mongo_manager.details_collection is not None:
        try:
            mongo_details = list(mongo_manager.details_collection.find({}))
            for m_doc in mongo_details:
                m_rid = m_doc.get("restaurant_id") or m_doc.get("_id")
                if m_rid in seen_ids:
                    continue

                m_rev = None
                if mongo_manager.reviews_collection is not None:
                    m_rev = mongo_manager.reviews_collection.find_one({"restaurant_id": m_rid})
                    if not m_rev:
                        m_rev = mongo_manager.reviews_collection.find_one({"_id": m_rid})

                deliv_doc = build_delivery_data_document(
                    detail_data=m_doc,
                    reviews_data=m_rev,
                    restaurant_id=m_rid,
                )
                if deliv_doc:
                    seen_ids.add(deliv_doc["_id"])
                    operations.append(ReplaceOne({"_id": deliv_doc["_id"]}, deliv_doc, upsert=True))
                    total_processed += 1
        except Exception as exc:
            logger.warning("Error processing remaining MongoDB details for delivery_data: %s", exc)

    if operations and mongo_manager and mongo_manager.delivery_collection is not None:
        try:
            res = mongo_manager.delivery_collection.bulk_write(operations, ordered=False)
            total_upserted = res.upserted_count
            total_modified = res.modified_count
        except Exception as exc:
            logger.error("Failed to bulk write delivery data to MongoDB: %s", exc)

    final_count = mongo_manager.get_delivery_count() if mongo_manager and mongo_manager.is_connected() else len(seen_ids)
    logger.info(
        "Delivery data sync complete: %d record(s) processed, %d newly inserted, %d updated. Total in collection: %d",
        total_processed,
        total_upserted,
        total_modified,
        final_count,
    )
    return {
        "status": "success",
        "processed": total_processed,
        "upserted": total_upserted,
        "modified": total_modified,
        "total_in_db": final_count,
    }


class ZomatoScraper:
    """Orchestrates keyword discovery, location resolution, and paginated restaurant scraping."""

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.csrf_token: Optional[str] = None

        if self.config.zomato_cookie:
            self.session.headers["cookie"] = self.config.zomato_cookie

        self.last_scraped_restaurants: List[Dict[str, Any]] = []

        # Initialize MongoDB connection to 'zomato' database and 'restaurants', 'restaurants_data', 'reviews', 'delivery_data' collections
        self.mongo_manager: Optional[MongoDBManager] = None
        if self.config.enable_mongo:
            self.mongo_manager = MongoDBManager(
                uri=self.config.mongo_uri,
                db_name=self.config.mongo_db,
                collection_name=self.config.mongo_collection,
                details_collection_name=self.config.mongo_details_collection,
                reviews_collection_name=self.config.mongo_reviews_collection,
                delivery_collection_name=self.config.mongo_delivery_collection,
            )
            self.mongo_manager.connect()

    def close(self) -> None:
        """Closes HTTP session and MongoDB client."""
        if self.mongo_manager:
            self.mongo_manager.close()
        try:
            self.session.close()
        except Exception:
            pass

    def __enter__(self) -> "ZomatoScraper":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def warm_session(self, force: bool = False) -> None:
        """
        Visits BASE_URL to acquire Akamai bot protection cookies ('ak_bmsc')
        so subsequent requests to /webroutes/* are not rejected with 401 Unauthorized.
        """
        if not force and self.session.cookies.get("ak_bmsc"):
            return

        try:
            logger.info("Warming session via base URL %s to acquire Akamai cookies...", BASE_URL)
            self.session.headers.update(DEFAULT_HEADERS)
            resp = self.session.get(BASE_URL, timeout=self.config.request_timeout)
            if self.session.cookies.get("csrf"):
                self.csrf_token = self.session.cookies.get("csrf")
            logger.info("Session warm complete. Cookies: %s", list(self.session.cookies.keys()))
        except Exception as exc:
            logger.warning("Session warm attempt encountered error: %s", exc)

    def ensure_csrf_token(self, force_refresh: bool = False) -> str:
        """Obtain a valid CSRF token from session cookie, API, or config override."""
        if not force_refresh:
            if self.config.zomato_csrf:
                self.csrf_token = self.config.zomato_csrf
                return self.csrf_token

            cookie_csrf = self.session.cookies.get("csrf")
            if cookie_csrf:
                self.csrf_token = cookie_csrf
                return self.csrf_token

        # Ensure headers have valid desktop browser User-Agent
        self.session.headers.setdefault("user-agent", DEFAULT_HEADERS["user-agent"])
        self.session.headers.setdefault("sec-ch-ua-platform", DEFAULT_HEADERS.get("sec-ch-ua-platform", '"Windows"'))

        try:
            logger.info("Fetching fresh CSRF token from %s...", CSRF_API_URL)
            resp = self.session.get(CSRF_API_URL, timeout=self.config.request_timeout)
            if resp.status_code == 200:
                data = resp.json()
                self.csrf_token = data.get("csrf") or self.session.cookies.get("csrf")
                if self.csrf_token:
                    logger.info("Acquired CSRF token successfully.")
                    return self.csrf_token
        except Exception as exc:
            logger.warning("Failed to fetch CSRF from endpoint: %s", exc)

        try:
            logger.info("Warming session via base URL %s...", BASE_URL)
            resp = self.session.get(BASE_URL, timeout=self.config.request_timeout)
            self.csrf_token = self.session.cookies.get("csrf")
            if self.csrf_token:
                logger.info("Acquired CSRF token from base URL session.")
                return self.csrf_token
        except Exception as exc:
            logger.warning("Failed to warm session: %s", exc)

        if self.session.cookies.get("csrf"):
            self.csrf_token = self.session.cookies.get("csrf")
            return self.csrf_token

        raise RuntimeError("Unable to obtain a valid Zomato CSRF token.")

    def get_location_suggestions(self, query: str) -> List[Dict[str, Any]]:
        """
        Calls Search API (webroutes/location/search?q={query}) and returns
        all valid location suggestions with coordinates (latlums).
        Automatically warms session and falls back to preloaded state HTML scraping
        if the API returns 401 Unauthorized or has no suggestions.
        """
        self.warm_session()
        logger.info("Searching Location API for '%s'...", query)
        resp = None
        try:
            resp = self.session.get(
                LOCATION_SEARCH_API_URL,
                params={"q": query},
                timeout=self.config.request_timeout,
            )
            if resp.status_code == 401:
                logger.info("Received 401 Unauthorized for location search; warming session & refreshing...")
                self.warm_session(force=True)
                resp = self.session.get(
                    LOCATION_SEARCH_API_URL,
                    params={"q": query},
                    timeout=self.config.request_timeout,
                )
        except Exception as exc:
            logger.warning("Location API request error for '%s': %s", query, exc)

        valid_suggestions: List[Dict[str, Any]] = []

        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                raw_suggestions = data.get("locationSuggestions", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                for s in raw_suggestions:
                    lat, lon = extract_latlums_from_suggestion(s)
                    if lat is not None and lon is not None:
                        title = str(s.get("entity_title") or s.get("display_title") or s.get("entity_name") or "Location").strip()
                        full_name = str(s.get("entity_name") or title).strip()
                        valid_suggestions.append({
                            "entity_id": s.get("entity_id"),
                            "entity_type": s.get("entity_type"),
                            "entity_title": title,
                            "entity_name": full_name,
                            "latitude": lat,
                            "longitude": lon,
                            "raw_suggestion": s,
                        })
            except Exception as exc:
                logger.warning("Failed to parse JSON from location search response: %s", exc)

        # Universal Resilient Fallback: If Search API returned 0 valid suggestions or failed with 401,
        # fallback to extracting window.__PRELOADED_STATE__ from city HTML
        if not valid_suggestions:
            logger.info("Search API returned no suggestions or failed; attempting preloaded state HTML fallback for '%s'...", query)
            html_suggestions = fetch_city_html_fallback(self.session, query, timeout=self.config.request_timeout)
            if html_suggestions:
                valid_suggestions.extend(html_suggestions)
                logger.info("Preloaded HTML fallback succeeded: found %d location suggestions for '%s'", len(valid_suggestions), query)

        logger.info("Retrieved %d valid location suggestion(s) with latlums for '%s'.", len(valid_suggestions), query)
        return valid_suggestions

    def search_latlums_by_keyword(self, query: str) -> Tuple[float, float, Dict[str, Any]]:
        """
        Calls Search API with city/keyword input and scrapes the latlums (latitude & longitude)
        from the nearest matching location suggestion.
        Falls back to HTML preloaded state if Search API fails or 401s.
        """
        self.warm_session()
        logger.info("Searching Location API for nearest keyword match to '%s'...", query)
        resp = None
        try:
            resp = self.session.get(
                LOCATION_SEARCH_API_URL,
                params={"q": query},
                timeout=self.config.request_timeout,
            )
            if resp.status_code == 401:
                logger.info("Received 401 Unauthorized for keyword search; warming session...")
                self.warm_session(force=True)
                resp = self.session.get(
                    LOCATION_SEARCH_API_URL,
                    params={"q": query},
                    timeout=self.config.request_timeout,
                )
        except Exception as exc:
            logger.warning("Keyword search request error for '%s': %s", query, exc)

        suggestions = []
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
                suggestions = data.get("locationSuggestions", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            except Exception:
                suggestions = []

        if not suggestions:
            # Universal HTML Fallback
            logger.info("Search API returned no suggestions for '%s'; trying HTML preloaded state fallback...", query)
            html_sugs = fetch_city_html_fallback(self.session, query, timeout=self.config.request_timeout)
            if html_sugs:
                first = html_sugs[0]
                lat = first.get("latitude")
                lon = first.get("longitude")
                if lat is not None and lon is not None:
                    name = first.get("entity_name") or first.get("entity_title") or query
                    logger.info("HTML fallback matched: '%s' -> Scraped Latlums: lat=%s, lon=%s", name, lat, lon)
                    return lat, lon, first

            raise ValueError(f"No location suggestions returned for search keyword: '{query}'")

        logger.info("Received %d suggestion(s) from Search API.", len(suggestions))
        nearest = find_nearest_suggestion(query, suggestions)
        lat, lon = extract_latlums_from_suggestion(nearest)

        if lat is None or lon is None:
            # Check HTML fallback as well
            html_sugs = fetch_city_html_fallback(self.session, query, timeout=self.config.request_timeout)
            if html_sugs and html_sugs[0].get("latitude") is not None:
                lat = html_sugs[0]["latitude"]
                lon = html_sugs[0]["longitude"]
                nearest = html_sugs[0]
            else:
                raise ValueError(f"Unable to extract lat/lon coordinates from suggestion: {nearest}")

        name = nearest.get("entity_name") or nearest.get("entity_title") or query
        logger.info("Nearest keyword matched: '%s' -> Scraped Latlums: lat=%s, lon=%s", name, lat, lon)
        return lat, lon, nearest

    def resolve_location(
        self,
        city: str,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        location: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Resolves location:
        1. If lat/lon are not provided, auto-discovers them from Search API using the location/city keyword.
        2. Queries webroutes/location/get using the latlums to get full spatial IDs.
        3. Constructs the '{location}_{city}' folder name.
        4. Returns resolved location metadata and exact location URL.
        """
        matched_suggestion: Optional[Dict[str, Any]] = None
        search_query = location if location else city

        # Auto-discover latlums from keyword if not explicitly passed
        if lat is None or lon is None:
            lat, lon, matched_suggestion = self.search_latlums_by_keyword(search_query)

        logger.info("Resolving location details for coordinates lat=%s, lon=%s...", lat, lon)
        loc_data: Dict[str, Any] = {}

        try:
            resp = self.session.get(
                LOCATION_GET_API_URL,
                params={"lat": lat, "lon": lon},
                timeout=self.config.request_timeout,
            )
            if resp.status_code == 401:
                self.warm_session(force=True)
                resp = self.session.get(
                    LOCATION_GET_API_URL,
                    params={"lat": lat, "lon": lon},
                    timeout=self.config.request_timeout,
                )
            if resp.status_code == 200:
                loc_data = resp.json().get("locationDetails", {})
                logger.info("Search API (location/get) returned status 200.")
        except Exception as exc:
            logger.warning("Search API (location/get) request failed: %s", exc)

        # Resolve verified parent city name to avoid malformed landmark URLs
        clean_city_name = str(city or loc_data.get("cityName") or "").strip()
        if "," in clean_city_name:
            clean_city_name = clean_city_name.split(",")[-1].strip()
        city_slug = clean_city_name.lower().replace(" ", "-")
        exact_url = f"{BASE_URL}/{city_slug}/restaurants"

        # Determine clean location name for the '{location}_{city}' folder
        raw_place = loc_data.get("placeName") or loc_data.get("entityName") or (matched_suggestion.get("entity_name") if matched_suggestion else "")
        sug_title = (matched_suggestion.get("entity_title") or matched_suggestion.get("display_title") if matched_suggestion else "") or ""
        loc_locality = str(raw_place).split(",")[0].strip() if raw_place else ""

        city_name = str(loc_data.get("cityName") or clean_city_name or city).strip()

        if location:
            primary_location = location
        elif sug_title and sug_title.lower() != city_name.lower():
            primary_location = sug_title
        elif loc_locality and loc_locality.lower() != city_name.lower():
            primary_location = loc_locality
        else:
            primary_location = sug_title or loc_locality or city_name

        folder_name = get_location_city_folder(primary_location, city_name)

        resolved_info = {
            "entity_id": str(loc_data.get("entityId") or (matched_suggestion.get("entity_id") if matched_suggestion else "0")),
            "entity_type": loc_data.get("entityType") or (matched_suggestion.get("entity_type") if matched_suggestion else "subzone"),
            "city_id": str(loc_data.get("cityId") or (matched_suggestion.get("entity_id") if matched_suggestion and matched_suggestion.get("entity_type") == "city" else "0")),
            "city_name": city_name,
            "location_name": primary_location,
            "folder_name": folder_name,
            "latitude": loc_data.get("latitude") or lat,
            "longitude": loc_data.get("longitude") or lon,
            "user_defined_latitude": loc_data.get("userDefinedLatitude") or lat,
            "user_defined_longitude": loc_data.get("userDefinedLongitude") or lon,
            "country_id": str(loc_data.get("countryId") or "1"),
            "country_name": loc_data.get("countryName") or "India",
            "place_id": str(loc_data.get("placeId") or "0"),
            "cell_id": str(loc_data.get("cellId") or "0"),
            "delivery_subzone_id": loc_data.get("deliverySubzoneId") or 0,
            "place_type": loc_data.get("placeType") or "DSZ",
            "place_name": loc_data.get("placeName") or loc_data.get("entityName") or (matched_suggestion.get("entity_name") if matched_suggestion else primary_location),
            "exact_location_url": exact_url,
        }

        logger.info(
            "Resolved location: '%s', City: '%s' -> Folder: 'output/%s'",
            resolved_info["location_name"],
            resolved_info["city_name"],
            resolved_info["folder_name"],
        )
        return resolved_info

    def build_restaurants_payload(
        self,
        loc: Dict[str, Any],
        page: int,
        offset: int,
        search_metadata: Optional[Dict[str, Any]] = None,
        context: str = "delivery",
    ) -> Dict[str, Any]:
        """Builds the POST payload for Restaurants API supporting delivery and dineout contexts."""
        if context == "delivery":
            filters_obj: Dict[str, Any] = {
                "searchMetadata": search_metadata or {},
                "dineoutAdsMetaData": {},
                "appliedFilter": [
                    {
                        "filterType": "category_sheet",
                        "filterValue": "delivery_home",
                        "isHidden": True,
                        "isApplied": True,
                        "postKey": '{"category_context":"delivery_home"}',
                    }
                ],
                "urlParamsForAds": {},
            }
            payload: Dict[str, Any] = {
                "context": "delivery",
                "filters": json.dumps(filters_obj, separators=(",", ":")),
                "addressId": 0,
                "entityId": int(loc.get("entity_id", 0)) if str(loc.get("entity_id", "0")).isdigit() else 0,
                "entityType": str(loc.get("entity_type", "city")),
                "locationType": "",
                "isOrderLocation": 1,
                "cityId": int(loc.get("city_id", 0)) if str(loc.get("city_id", "0")).isdigit() else 0,
                "latitude": str(float(loc.get("latitude") or 17.3850)),
                "longitude": str(float(loc.get("longitude") or 78.4867)),
                "userDefinedLatitude": float(loc.get("user_defined_latitude") or loc.get("latitude") or 17.3850),
                "userDefinedLongitude": float(loc.get("user_defined_longitude") or loc.get("longitude") or 78.4867),
                "entityName": str(loc.get("place_name") or loc.get("location_name") or loc.get("city_name", "City")),
                "orderLocationName": str(loc.get("place_name") or loc.get("location_name") or loc.get("city_name", "City")),
                "cityName": str(loc.get("city_name", "City")),
                "countryId": int(loc.get("country_id", 1)) if str(loc.get("country_id", "1")).isdigit() else 1,
                "countryName": str(loc.get("country_name", "India")),
                "displayTitle": str(loc.get("location_name") or loc.get("city_name", "City")),
                "o2Serviceable": True,
                "placeId": str(loc.get("place_id", "0")),
                "cellId": str(loc.get("cell_id", "0")),
                "deliverySubzoneId": int(loc.get("delivery_subzone_id", 0)) if str(loc.get("delivery_subzone_id", "0")).isdigit() else 0,
                "placeType": str(loc.get("place_type", "DSZ")),
                "placeName": str(loc.get("place_name") or loc.get("location_name") or loc.get("city_name", "City")),
                "isO2City": True,
                "fetchFromGoogle": False,
                "fetchedFromCookie": False,
                "isO2OnlyCity": True,
                "address_template": [],
                "otherRestaurantsUrl": "",
                "solr_offset": offset,
                "page": page,
                "total_restaurants_shown": offset,
            }
        else:
            payload = {
                "context": "dineout",
                "entityId": str(loc.get("entity_id", "0")),
                "entityType": str(loc.get("entity_type", "subzone")),
                "cityId": str(loc.get("city_id", "6")),
                "latitude": float(loc.get("latitude") or 17.3850),
                "longitude": float(loc.get("longitude") or 78.4867),
                "userDefinedLatitude": float(loc.get("user_defined_latitude") or loc.get("latitude") or 17.3850),
                "userDefinedLongitude": float(loc.get("user_defined_longitude") or loc.get("longitude") or 78.4867),
                "cityName": str(loc.get("city_name", "City")),
                "countryId": str(loc.get("country_id", "1")),
                "countryName": str(loc.get("country_name", "India")),
                "placeId": str(loc.get("place_id", "0")),
                "deliverySubzoneId": loc.get("delivery_subzone_id", 0),
                "placeType": str(loc.get("place_type", "DSZ")),
                "cellId": str(loc.get("cell_id", "0")),
                "filters": json.dumps({"searchMetadata": search_metadata}) if search_metadata else "{}",
                "solr_offset": offset,
                "page": page,
                "total_restaurants_shown": offset,
                "total_results_shown": offset,
                "vg_set": True,
            }
        return payload

    def check_status(
        self,
        city: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        location: Optional[str] = None,
    ) -> bool:
        """
        Pre-flight status check:
        1. Checks Search API status (auto-scraps latlums from nearest keyword if lat/lon not given).
        2. Probes Restaurants API (Offset=0, Page=1) to verify live data with HTTP 200.
        """
        c = city or self.config.city
        la = lat if lat is not None else self.config.latitude
        lo = lon if lon is not None else self.config.longitude

        print("\n" + "=" * 60)
        print("          PRE-FLIGHT API STATUS CHECK")
        print("=" * 60)
        print(f"Target City Input:     {c}")
        if location:
            print(f"Target Location Input: {location}")

        # 1. Check Search API & Latlums
        print("\n[1/3] Checking Search API (Keyword & Latlums discovery)...")
        search_ok = False
        loc: Dict[str, Any] = {}
        try:
            loc = self.resolve_location(city=c, lat=la, lon=lo, location=location)
            print(f"  ✓ Search API Status:       200 OK")
            print(f"  ✓ Scraped Latlums:         lat={loc['latitude']}, lon={loc['longitude']}")
            print(f"  ✓ Resolved Location:       {loc['location_name']}")
            print(f"  ✓ Target Output Folder:    output/{loc['folder_name']}/")
            print(f"  ✓ Exact Location URL:      {loc['exact_location_url']}")
            search_ok = True
        except Exception as exc:
            print(f"  ✗ Search API Error: {exc}")
            return False

        # 2. Check CSRF Token
        csrf = self.ensure_csrf_token()
        print(f"  ✓ CSRF Token Status:       Acquired ({csrf[:10]}...)")

        # 2. Check Restaurants API (Probe request: page=1, offset=0)
        print("\n[2/3] Checking Restaurants API (Offset=0, Page=1)...")
        restaurants_ok = False
        try:
            payload = self.build_restaurants_payload(loc, page=1, offset=0)
            headers = {
                "user-agent": DEFAULT_HEADERS["user-agent"],
                "origin": BASE_URL,
                "referer": loc["exact_location_url"],
                "x-zomato-csrft": csrf,
                "content-type": "application/json",
            }
            resp = self.session.post(
                RESTAURANTS_API_URL,
                json=payload,
                headers=headers,
                timeout=self.config.request_timeout,
            )
            print(f"  ✓ Restaurants API HTTP Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                sections = data.get("sections", {})
                results = sections.get("SECTION_SEARCH_RESULT", [])
                restaurant_count = len(results)
                print(f"  ✓ Response Payload Size:       {len(resp.text):,} bytes")
                print(f"  ✓ Restaurants in Page 1:       {restaurant_count} restaurants found")
                if restaurant_count > 0:
                    sample = results[0].get("info", {}).get("name", "N/A")
                    print(f"  ✓ Sample Restaurant:           '{sample}'")
                    restaurants_ok = True
                else:
                    print("  ⚠ Notice: 0 restaurants returned for this exact coordinate.")
            else:
                print(f"  ✗ Restaurants API failed with status {resp.status_code}: {resp.text[:200]}")
        except Exception as exc:
            print(f"  ✗ Restaurants API Error: {exc}")
            return False

        # 3. Check MongoDB Connection
        print(f"\n[3/3] Checking MongoDB Connection ({self.config.mongo_db}.{self.config.mongo_collection})...")
        mongo_ok = True
        if self.config.enable_mongo:
            if self.mongo_manager and self.mongo_manager.is_connected():
                doc_count = self.mongo_manager.get_total_count()
                print(f"  ✓ MongoDB Server Status:   Connected ({self.config.mongo_uri})")
                print(f"  ✓ Target Database:         {self.config.mongo_db}")
                print(f"  ✓ Target Collection:       {self.config.mongo_collection} (Primary Key: restaurant_id / _id)")
                print(f"  ✓ Current Stored Records:  {doc_count:,} unique restaurant(s)")
            else:
                print(f"  ⚠ MongoDB Status:          Disconnected or connection failed to {self.config.mongo_uri}")
                mongo_ok = False
        else:
            print(f"  • MongoDB Status:          Disabled in configuration (ENABLE_MONGO=false)")

        print("\n" + "=" * 60)
        status_str = "SUCCESS (Ready to scrape data)" if (search_ok and restaurants_ok) else "PARTIAL / FAILED"
        print(f"Overall Status: {status_str}")
        print("=" * 60 + "\n")
        return search_ok and restaurants_ok

    def scrape(
        self,
        city: Optional[str] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
        location: Optional[str] = None,
        max_pages: Optional[int] = None,
        start_page: Optional[int] = None,
        start_offset: Optional[int] = None,
    ) -> Tuple[Path, List[Path]]:
        """
        Scrapes restaurant data:
        1. Auto-discovers latlums from city/location keyword via Search API.
        2. Creates output/{location}_{city}/ folder.
        3. Loops through pages with offset += 12 and page += 1.
        4. Saves full responses to output/{location}_{city}/{offset}_{page}.json.
        5. Terminates when max_pages is reached OR when a page contains no restaurants with status 200.
        Returns:
          Tuple[Path, List[Path]]: (target_folder_path, list_of_saved_file_paths)
        """
        c = city or self.config.city
        la = lat if lat is not None else self.config.latitude
        lo = lon if lon is not None else self.config.longitude

        pages_limit = max_pages if max_pages is not None else self.config.max_pages
        page = start_page if start_page is not None else self.config.start_page
        offset = start_offset if start_offset is not None else self.config.start_offset

        # Step 1: Resolve location, latlums & output folder
        loc = self.resolve_location(city=c, lat=la, lon=lo, location=location)
        csrf = self.ensure_csrf_token()

        # Step 2: Create location_city output folder
        target_folder = self.config.output_dir / loc["folder_name"]
        target_folder.mkdir(parents=True, exist_ok=True)
        logger.info("Destination folder for scraped files: %s", target_folder)

        headers = {
            "user-agent": DEFAULT_HEADERS["user-agent"],
            "origin": BASE_URL,
            "referer": loc["exact_location_url"],
            "x-zomato-csrft": csrf,
            "content-type": "application/json",
        }

        saved_files: List[Path] = []
        self.last_scraped_restaurants = []
        search_metadata: Optional[Dict[str, Any]] = None
        pages_scraped = 0
        session_seen_res_ids: set = set()

        logger.info(
            "Starting scrape: city='%s', location='%s', limit=%s pages, start_page=%d, start_offset=%d, increment=%d",
            c,
            loc["location_name"],
            pages_limit if pages_limit else "unlimited (until page contains no restaurants)",
            page,
            offset,
            OFFSET_INCREMENT,
        )

        while True:
            if pages_limit and pages_scraped >= pages_limit:
                logger.info("Reached requested max pages limit (%d). Scraping finished.", pages_limit)
                break

            payload = self.build_restaurants_payload(
                loc=loc,
                page=page,
                offset=offset,
                search_metadata=search_metadata,
            )

            logger.info("Scraping Page %d with Offset %d...", page, offset)
            resp = self.session.post(
                RESTAURANTS_API_URL,
                json=payload,
                headers=headers,
                timeout=self.config.request_timeout,
            )

            if resp.status_code != 200:
                logger.error(
                    "Restaurants API returned non-200 status (HTTP %d) on Page %d (Offset %d): %s",
                    resp.status_code,
                    page,
                    offset,
                    resp.text[:200],
                )
                break

            try:
                data = resp.json()
            except ValueError as exc:
                logger.error("Response on Page %d was not valid JSON: %s", page, exc)
                break

            # Save full response into file named {offset}_{page}.json inside target_folder
            file_name = f"{offset}_{page}.json"
            file_path = target_folder / file_name
            with file_path.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)

            saved_files.append(file_path)

            sections = data.get("sections") if isinstance(data, dict) else {}
            if not isinstance(sections, dict):
                sections = {}

            restaurants = sections.get("SECTION_SEARCH_RESULT")
            if not isinstance(restaurants, list):
                restaurants = []

            meta_info = sections.get("SECTION_SEARCH_META_INFO") if isinstance(sections, dict) else {}
            if not isinstance(meta_info, dict):
                meta_info = {}

            search_metadata = meta_info.get("searchMetaData") if isinstance(meta_info, dict) else {}
            if not isinstance(search_metadata, dict):
                search_metadata = {}

            # Stopping condition 1: stop when page contains SECTION_SEARCH_RESULT: [] (empty list)
            # Do NOT rely on hasMore (API returns hasMore=true even when SECTION_SEARCH_RESULT is []
            # and total_restaurants_shown >= 501 limit is reached).
            if not restaurants or len(restaurants) == 0:
                has_more_flag = search_metadata.get("hasMore")
                total_res = search_metadata.get("totalResults")
                total_shown = None
                postback = search_metadata.get("postbackParams")
                if isinstance(postback, str):
                    try:
                        pb_dict = json.loads(postback)
                        total_shown = pb_dict.get("total_restaurants_shown") or pb_dict.get("total_results_shown")
                    except Exception:
                        pass
                elif isinstance(postback, dict):
                    total_shown = postback.get("total_restaurants_shown") or postback.get("total_results_shown")

                meta_parts = []
                if total_shown is not None:
                    meta_parts.append(f"total_shown={total_shown}")
                if has_more_flag is not None:
                    meta_parts.append(f"hasMore={has_more_flag}")
                if total_res is not None:
                    meta_parts.append(f"totalResults={total_res}")
                meta_info_str = f" ({', '.join(meta_parts)})" if meta_parts else ""

                logger.info(
                    "✓ Saved terminal response to %s/%s (0 restaurants found)",
                    loc["folder_name"],
                    file_path.name,
                )
                logger.info(
                    "Page %d contains empty SECTION_SEARCH_RESULT: []%s. "
                    "Reached end of available restaurants. Stopping pagination.",
                    page,
                    meta_info_str,
                )
                break

            # Stopping condition 2: Solr search engine wrap-around/repeat detection
            # Check if this page contains ANY new restaurants not yet seen in this scrape run
            current_page_res_ids = [
                item.get("info", {}).get("resId")
                for item in restaurants
                if isinstance(item, dict) and item.get("info", {}).get("resId")
            ]
            new_res_ids = [rid for rid in current_page_res_ids if rid not in session_seen_res_ids]

            if len(new_res_ids) == 0:
                logger.info(
                    "Page %d returned %d restaurants, but ALL were already seen in this session (0 new unique restaurants). "
                    "Reached end of unique results. Stopping pagination.",
                    page,
                    len(restaurants),
                )
                # Remove this wrap-around duplicate file so target_folder stays clean
                file_path.unlink(missing_ok=True)
                saved_files.pop()
                break

            session_seen_res_ids.update(new_res_ids)
            self.last_scraped_restaurants.extend(restaurants)

            logger.info(
                "✓ Saved full response to %s/%s (%d restaurants found, %d new unique)",
                loc["folder_name"],
                file_path.name,
                len(restaurants),
                len(new_res_ids),
            )

            # Store unique restaurants into MongoDB (zomato.restaurants) with _id = restaurant_id
            if self.mongo_manager and self.mongo_manager.is_connected() and restaurants:
                upserted, modified = self.mongo_manager.upsert_restaurants(
                    restaurants=restaurants,
                    city=loc.get("city_name", c),
                    location=loc.get("location_name", ""),
                )
                total_in_db = self.mongo_manager.get_total_count()
                logger.info(
                    "✓ Stored in MongoDB [%s.%s]: %d new, %d updated (Total in DB: %d unique)",
                    self.config.mongo_db,
                    self.config.mongo_collection,
                    upserted,
                    modified,
                    total_in_db,
                )

            pages_scraped += 1
            page += PAGE_INCREMENT
            offset += OFFSET_INCREMENT

            if self.config.request_delay > 0:
                time.sleep(self.config.request_delay)

        logger.info(
            "Scraping complete. Total %d response file(s) saved into: %s",
            len(saved_files),
            target_folder,
        )
        return target_folder, saved_files

    def scrape_all_locations(
        self,
        city: str,
        suggestions: List[Dict[str, Any]],
        max_pages: Optional[int] = None,
        start_page: int = 1,
        start_offset: int = 0,
        scrape_details: bool = True,
        details_limit: Optional[int] = None,
        reviews_max_pages: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Sequentially extracts and scrapes restaurants across all discovered locations with latlums for a city.
        For each location:
          1. Scrapes search results into output/{location}_{city}/{offset}_{page}.json & zomato.restaurants.
          2. If scrape_details is True, simultaneously scrapes hotel details & customer reviews
             into output_details/, output_reviews/, and MongoDB collections 'restaurants_data' & 'reviews'.
          3. Exports all discovered location latlums to output/{city}_locations.json.
        """
        if not suggestions:
            logger.warning("No location suggestions provided to scrape.")
            return {"status": "error", "message": "No suggestions provided"}

        # Export all discovered location latlums for this city
        city_slug = slugify(city) or "city"
        locs_file = self.config.output_dir / f"{city_slug}_locations.json"
        try:
            self.config.output_dir.mkdir(parents=True, exist_ok=True)
            with locs_file.open("w", encoding="utf-8") as fh:
                json.dump(suggestions, fh, ensure_ascii=False, indent=2)
            logger.info("Saved %d discovered location(s) with latlums to %s", len(suggestions), locs_file)
        except Exception as exc:
            logger.warning("Could not save locations file %s: %s", locs_file, exc)

        total_locs = len(suggestions)
        logger.info("=" * 90)
        logger.info("Starting Batch All-Locations Scraping: %d location(s) for '%s'", total_locs, city)
        logger.info("=" * 90)

        total_search_files = 0
        total_details_files = 0
        locations_completed = 0
        locations_summary: List[Dict[str, Any]] = []

        for idx, item in enumerate(suggestions, 1):
            loc_title = item.get("entity_title") or f"Location_{idx}"
            lat = item.get("latitude")
            lon = item.get("longitude")

            if lat is None or lon is None:
                logger.warning("[%d/%d] Skipping '%s': Missing coordinates (lat=%s, lon=%s)", idx, total_locs, loc_title, lat, lon)
                continue

            print("\n" + "=" * 90)
            print(f">>> [{idx}/{total_locs}] Processing Location: '{loc_title}' (lat={lat:.6f}, lon={lon:.6f})")
            print("=" * 90 + "\n")

            try:
                target_folder, saved_files = self.scrape(
                    city=city,
                    lat=lat,
                    lon=lon,
                    location=loc_title,
                    max_pages=max_pages,
                    start_page=start_page,
                    start_offset=start_offset,
                )
            except Exception as exc:
                logger.error("[%d/%d] Scraping failed for '%s': %s", idx, total_locs, loc_title, exc)
                continue

            locations_completed += 1
            total_search_files += len(saved_files)
            details_count_for_loc = 0

            # Step 3: Hotel Details & Reviews for this location
            if scrape_details:
                extracted = list(self.last_scraped_restaurants)
                if not extracted and saved_files:
                    for sf in saved_files:
                        try:
                            with sf.open("r", encoding="utf-8") as fh:
                                d = json.load(fh)
                            items = d.get("sections", {}).get("SECTION_SEARCH_RESULT", [])
                            if isinstance(items, list):
                                extracted.extend(items)
                        except Exception:
                            pass

                # Deduplicate
                seen: set = set()
                deduped = []
                for r in extracted:
                    rid = (
                        r.get("info", {}).get("resId")
                        if isinstance(r.get("info"), dict)
                        else None
                    ) or r.get("restaurant_id") or r.get("_id")
                    if rid and rid not in seen:
                        seen.add(rid)
                        deduped.append(r)

                if deduped:
                    logger.info(
                        "[%d/%d] Fetching hotel details & reviews for %d restaurant(s) in '%s'...",
                        idx,
                        total_locs,
                        len(deduped),
                        loc_title,
                    )
                    det_files = self.scrape_all_restaurant_details(
                        restaurants=deduped,
                        city=city,
                        location=loc_title,
                        folder_name=target_folder.name,
                        limit=details_limit,
                        reviews_max_pages=reviews_max_pages,
                    )
                    details_count_for_loc = len(det_files)
                    total_details_files += details_count_for_loc

            locations_summary.append({
                "index": idx,
                "location": loc_title,
                "lat": lat,
                "lon": lon,
                "folder": target_folder.name,
                "search_files": len(saved_files),
                "details_files": details_count_for_loc,
            })

            if self.config.request_delay > 0 and idx < total_locs:
                time.sleep(self.config.request_delay)

        # Final Batch Summary
        print("\n" + "=" * 90)
        print(f"             BATCH ALL-LOCATIONS SCRAPING COMPLETE: '{city}'")
        print("=" * 90)
        print(f"Total Locations Discovered:  {total_locs}")
        print(f"Locations Processed:         {locations_completed} / {total_locs}")
        print(f"Discovered Latlums Exported: {locs_file}")
        print(f"Total Search Files Saved:    {total_search_files}")
        if scrape_details:
            print(f"Total Details & Reviews:     {total_details_files}")
        if self.mongo_manager and self.mongo_manager.is_connected():
            print(f"MongoDB Total Restaurants:   {self.mongo_manager.get_total_count():,} unique")
            print(f"MongoDB Total Details:       {self.mongo_manager.get_details_count():,} unique")
            print(f"MongoDB Total Reviews:       {self.mongo_manager.get_reviews_count():,} unique")
        print("=" * 90 + "\n")

        return {
            "status": "success",
            "city": city,
            "locations_total": total_locs,
            "locations_completed": locations_completed,
            "locations_file": str(locs_file),
            "total_search_files": total_search_files,
            "total_details_files": total_details_files,
            "summary": locations_summary,
        }

    def scrape_restaurant_detail(
        self,
        page_url: str,
        restaurant_id: Optional[int] = None,
        city: str = "",
        location: str = "",
        target_folder: Optional[Path] = None,
        fetch_reviews: bool = True,
        reviews_max_pages: Optional[int] = None,
        reviews_target_folder: Optional[Path] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Step 3: Fetches full hotel/restaurant details from Zomato webroutes/getPage API.
        Saves the full JSON response into target_folder/{restaurant_id}.json
        and upserts into MongoDB collection 'restaurants_data' with _id = restaurant_id.
        Simultaneously extracts all customer reviews via webroutes/reviews/loadMore
        and stores into output_reviews/{folder_slug}/{restaurant_id}.json and MongoDB 'reviews'.
        """
        clean_url = str(page_url).strip()
        if clean_url.startswith("http"):
            clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
        if not clean_url.startswith("/"):
            clean_url = f"/{clean_url}"

        # Ensure CSRF token
        try:
            csrf_token = self.ensure_csrf_token()
        except Exception:
            csrf_token = self.config.zomato_csrf or ""

        headers = dict(self.session.headers)
        headers["referer"] = f"{BASE_URL}{clean_url}"
        if csrf_token:
            headers["x-zomato-csrft"] = csrf_token

        params = {
            "page_url": clean_url,
            "location": "",
            "isMobile": 0,
        }

        try:
            resp = self.session.get(
                RESTAURANT_DETAILS_API_URL,
                params=params,
                headers=headers,
                timeout=self.config.request_timeout,
            )
        except Exception as exc:
            logger.error("Failed to request hotel details for '%s': %s", clean_url, exc)
            return None

        data = None
        if resp is not None and resp.status_code == 200:
            try:
                data = resp.json()
            except ValueError:
                data = None

        # Fallback to direct HTML preloaded state extraction if getPage returned non-200
        if not data:
            logger.info("getPage API returned non-200 for '%s'; extracting details via web HTML page...", clean_url)
            try:
                full_page_url = f"{BASE_URL}{clean_url}"
                resp_html = self.session.get(full_page_url, headers=headers, timeout=self.config.request_timeout)
                if resp_html.status_code == 200:
                    st = extract_zomato_preloaded_state(resp_html.text)
                    if st:
                        curr = st.get("pages", {}).get("restaurant", {})
                        sections = {}
                        res_id_found = None
                        for rk, rv in curr.items():
                            if isinstance(rv, dict) and "sections" in rv:
                                sections = rv["sections"]
                                res_id_found = rk
                                break
                        data = {
                            "page_data": {"sections": sections},
                            "page_info": {"resId": int(res_id_found) if res_id_found and res_id_found.isdigit() else restaurant_id},
                            "entities": st.get("entities", {}),
                            "status": "success",
                        }
            except Exception as e_html:
                logger.warning("HTML details extraction error for '%s': %s", clean_url, e_html)

        if not data:
            logger.warning("Unable to fetch hotel details for '%s'.", clean_url)
            return None

        # Determine restaurant ID
        page_info = data.get("page_info") if isinstance(data.get("page_info"), dict) else {}
        sections = data.get("page_data", {}).get("sections", {}) if isinstance(data.get("page_data"), dict) else {}
        basic_info = sections.get("SECTION_BASIC_INFO", {}) if isinstance(sections, dict) else {}

        res_id = restaurant_id or page_info.get("resId") or basic_info.get("res_id")
        if res_id:
            try:
                res_id = int(res_id)
            except (ValueError, TypeError):
                pass
        else:
            res_id = slugify(clean_url.strip("/").split("/")[-1])

        # Target folder for saving JSON response
        if target_folder is None:
            folder_slug = get_location_city_folder(location or "details", city or "general")
            dest_folder = self.config.details_dir / folder_slug
        else:
            dest_folder = Path(target_folder)

        dest_folder.mkdir(parents=True, exist_ok=True)
        file_path = dest_folder / f"{res_id}.json"

        with file_path.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)

        hotel_name = basic_info.get("name") or page_info.get("pageTitle", "").split(",")[0].strip() or str(res_id)

        # Store in MongoDB collection 'restaurants_data' with _id = res_id
        if self.mongo_manager and self.mongo_manager.is_connected() and isinstance(res_id, int):
            self.mongo_manager.upsert_restaurant_detail(
                detail_data=data,
                restaurant_id=res_id,
                page_url=clean_url,
                city=city,
                location=location,
            )

        logger.info(
            "✓ Saved hotel details for '%s' (ID: %s) -> %s/%s",
            hotel_name,
            res_id,
            dest_folder.name,
            file_path.name,
        )

        # Simultaneous Review Scraping (Step 3)
        reviews_doc: Optional[Dict[str, Any]] = None
        if fetch_reviews and isinstance(res_id, int):
            try:
                if reviews_target_folder is None:
                    folder_slug = get_location_city_folder(location or "reviews", city or "general")
                    r_target_folder = self.config.reviews_dir / folder_slug
                else:
                    r_target_folder = Path(reviews_target_folder)

                reviews_doc = self.scrape_restaurant_reviews(
                    restaurant_id=res_id,
                    name=hotel_name,
                    city=city,
                    location=location,
                    page_url=clean_url,
                    max_pages=reviews_max_pages,
                    target_folder=r_target_folder,
                )
            except Exception as exc:
                logger.error("Failed scraping reviews for restaurant ID %s: %s", res_id, exc)

        # Build and store parsed delivery document (Collection: 'delivery_data' & output_delivery/)
        try:
            delivery_doc = build_delivery_data_document(
                detail_data=data,
                reviews_data=reviews_doc,
                restaurant_id=res_id,
                city=city,
                location=location,
                page_url=clean_url,
            )
            if delivery_doc:
                deliv_folder = self.config.delivery_dir / dest_folder.name
                deliv_folder.mkdir(parents=True, exist_ok=True)
                deliv_file = deliv_folder / f"{res_id}.json"
                with deliv_file.open("w", encoding="utf-8") as fh:
                    json.dump(delivery_doc, fh, ensure_ascii=False, indent=2)

                if self.mongo_manager and self.mongo_manager.is_connected():
                    self.mongo_manager.upsert_delivery_data(delivery_doc)
        except Exception as exc:
            logger.warning("Could not build or save delivery_data for restaurant ID %s: %s", res_id, exc)

        return data

    def scrape_restaurant_reviews(
        self,
        restaurant_id: int,
        name: str = "",
        city: str = "",
        location: str = "",
        page_url: str = "",
        max_pages: Optional[int] = None,
        target_folder: Optional[Path] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Extracts all customer reviews for a given restaurant using Zomato's webroutes/reviews/loadMore API:
        https://www.zomato.com/webroutes/reviews/loadMore?res_id={restaurant_id}&page={page}&sort=dd&filter=reviews-dining

        Workflow:
        1. Calls page 1 to read 'numberOfPages' from page_data.sections.SECTION_REVIEWS.numberOfPages.
        2. Loops from page 1 up to numberOfPages (or max_pages if provided) fetching all review pages.
        3. Merges all reviews across all pages into a single consolidated document for the restaurant.
        4. Saves the consolidated JSON into output_reviews/{location}_{city}/{restaurant_id}.json.
        5. Upserts into MongoDB collection 'reviews' with primary key _id = restaurant_id to prevent duplicates.
        """
        try:
            res_id = int(restaurant_id)
        except (ValueError, TypeError):
            logger.error("Invalid restaurant_id for reviews: %s", restaurant_id)
            return None

        # Build clean referer URL
        clean_url = str(page_url or "").strip()
        if clean_url.startswith("http"):
            clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
        if clean_url and not clean_url.startswith("/"):
            clean_url = f"/{clean_url}"
        if clean_url:
            clean_url_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", clean_url)
            reviews_referer_path = f"{clean_url_base}/reviews"
        else:
            clean_url_base = ""
            reviews_referer_path = "/reviews"

        # If clean_url_base is not set, attempt to resolve from MongoDB
        if not clean_url_base and self.mongo_manager and self.mongo_manager.is_connected():
            try:
                d_doc = self.mongo_manager.details_collection.find_one(
                    {"_id": res_id}, {"page_url": 1, "canonical_url": 1}
                )
                c_url = (d_doc.get("canonical_url") or d_doc.get("page_url")) if d_doc else ""
                if not c_url:
                    r_doc = self.mongo_manager.collection.find_one({"_id": res_id}, {"url": 1})
                    c_url = r_doc.get("url") if r_doc else ""
                if c_url:
                    c_url = re.sub(r"^https?://[^/]+", "", str(c_url).strip())
                    clean_url_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", c_url)
                    reviews_referer_path = f"{clean_url_base}/reviews"
            except Exception:
                pass

        # Ensure CSRF token
        try:
            csrf_token = self.ensure_csrf_token()
        except Exception:
            csrf_token = self.config.zomato_csrf or ""

        headers = dict(self.session.headers)
        headers["referer"] = f"{BASE_URL}{reviews_referer_path}"
        if csrf_token:
            headers["x-zomato-csrft"] = csrf_token

        # Fetch Page 1
        params = {
            "res_id": res_id,
            "page": 1,
            "sort": "dd",
            "filter": "reviews-dining",
        }

        # Enforce 1.0s polite delay for reviews request
        time.sleep(getattr(self.config, "reviews_request_delay", 1.0))

        use_getpage_fallback = False
        resp = None
        try:
            resp = self.session.get(
                REVIEWS_API_URL,
                params=params,
                headers=headers,
                timeout=self.config.request_timeout,
            )
        except Exception as exc:
            logger.warning("Failed to request reviews page 1 from loadMore for restaurant ID %s: %s", res_id, exc)

        p1_data = None
        # If 401 login popup triggered or request failed on loadMore, fallback to getPage or direct HTML web route
        if (resp is None or resp.status_code == 401) and clean_url_base:
            try:
                logger.info(
                    "Reviews page 1 prompted login (401); switching to web route for restaurant ID %s...",
                    res_id,
                )
                use_getpage_fallback = True
                fallback_path = f"{reviews_referer_path}?page=1"
                resp = self.session.get(
                    RESTAURANT_DETAILS_API_URL,
                    params={"page_url": fallback_path},
                    headers=headers,
                    timeout=self.config.request_timeout,
                )
                if resp.status_code != 200:
                    # Direct HTML page fetch to bypass API 401
                    full_html_url = f"{BASE_URL}{reviews_referer_path}?page=1"
                    h_resp = self.session.get(full_html_url, headers=headers, timeout=self.config.request_timeout)
                    if h_resp.status_code == 200:
                        st = extract_zomato_preloaded_state(h_resp.text)
                        if st:
                            p1_data = st
            except Exception as exc:
                logger.error("Fallback reviews page 1 failed for restaurant ID %s: %s", res_id, exc)

        if p1_data is None:
            if resp is None or resp.status_code != 200:
                # Try direct HTML fetch as final attempt
                if clean_url_base:
                    try:
                        full_html_url = f"{BASE_URL}{reviews_referer_path}?page=1"
                        h_resp = self.session.get(full_html_url, headers=headers, timeout=self.config.request_timeout)
                        if h_resp.status_code == 200:
                            st = extract_zomato_preloaded_state(h_resp.text)
                            if st:
                                p1_data = st
                    except Exception:
                        pass

            if p1_data is None:
                if resp is None or resp.status_code != 200:
                    logger.warning(
                        "Reviews request page 1 for restaurant ID %s returned status %s: %s",
                        res_id,
                        getattr(resp, "status_code", "None"),
                        resp.text[:150] if resp else "",
                    )
                    return None

                try:
                    p1_data = resp.json()
                except ValueError as exc:
                    logger.error("Reviews response page 1 for restaurant ID %s was not valid JSON: %s", res_id, exc)
                    return None

        # Read numberOfPages from page_data.sections.SECTION_REVIEWS
        sec_reviews = (
            p1_data.get("page_data", {})
            .get("sections", {})
            .get("SECTION_REVIEWS", {})
            if isinstance(p1_data.get("page_data"), dict)
            else {}
        )
        total_pages = 0
        if isinstance(sec_reviews, dict):
            raw_nop = sec_reviews.get("numberOfPages")
            try:
                total_pages = int(raw_nop) if raw_nop is not None else 0
            except (ValueError, TypeError):
                total_pages = 0

        pages_to_scrape = total_pages
        effective_max = max_pages if max_pages is not None else self.config.max_review_pages
        if effective_max is not None and effective_max > 0:
            pages_to_scrape = min(pages_to_scrape, effective_max)

        if total_pages > 0:
            logger.info(
                "Fetching customer reviews for '%s' (ID: %s): %d total page(s) available (scraping %d page(s))...",
                name or res_id,
                res_id,
                total_pages,
                pages_to_scrape,
            )

        # Accumulator structures
        raw_pages: List[Dict[str, Any]] = [{"page": 1, "data": p1_data}]
        raw_reviews_by_id: Dict[str, Any] = {}
        ratings: Dict[str, Any] = {}
        comments: Dict[str, Any] = {}
        normalized_reviews: List[Dict[str, Any]] = []

        def _extract_page_content(page_json: Dict[str, Any]) -> None:
            entities = page_json.get("entities")
            if not isinstance(entities, dict):
                return

            p_ratings = entities.get("RATING")
            if isinstance(p_ratings, dict):
                ratings.update(p_ratings)

            p_comments = entities.get("REVIEW_COMMENTS")
            if isinstance(p_comments, dict):
                comments.update(p_comments)

            p_reviews = entities.get("REVIEWS")
            rev_items: List[Dict[str, Any]] = []
            if isinstance(p_reviews, dict):
                for k, v in p_reviews.items():
                    if isinstance(v, dict):
                        raw_reviews_by_id[str(k)] = v
                        rev_items.append(v)
            elif isinstance(p_reviews, list):
                for v in p_reviews:
                    if isinstance(v, dict):
                        rk = str(v.get("reviewId") or len(raw_reviews_by_id) + 1)
                        raw_reviews_by_id[rk] = v
                        rev_items.append(v)

            for item in rev_items:
                rid = item.get("reviewId")
                rating_val = item.get("ratingV2")
                rating_text = item.get("ratingV2Text")
                raw_r = item.get("rating")
                if (rating_val is None or isinstance(rating_val, dict)) and isinstance(raw_r, (int, float, str)):
                    rating_val = raw_r
                elif (rating_val is None or isinstance(rating_val, dict)) and isinstance(raw_r, dict):
                    for ent in raw_r.get("entities", []):
                        for eid in ent.get("entity_ids", []):
                            r_obj = ratings.get(str(eid)) or ratings.get(eid)
                            if isinstance(r_obj, dict):
                                if "rating" in r_obj:
                                    rating_val = r_obj.get("rating")
                                if "text" in r_obj and not rating_text:
                                    rating_text = r_obj.get("text")
                                break

                norm = {
                    "review_id": rid,
                    "user_name": item.get("userName"),
                    "user_id": item.get("reviewUserId"),
                    "user_profile_url": item.get("userProfileUrl"),
                    "rating": rating_val,
                    "rating_text": rating_text,
                    "review_text": item.get("reviewText"),
                    "timestamp": item.get("timestamp"),
                    "experience": item.get("experience"),
                    "like_count": item.get("likeCount", 0),
                    "comment_count": item.get("commentCount", 0),
                    "review_url": item.get("reviewUrl"),
                    "photos": item.get("reviewPhotos", []),
                }
                normalized_reviews.append(norm)

        # Extract page 1
        _extract_page_content(p1_data)

        # Scrape subsequent pages (page 2 to pages_to_scrape)
        if pages_to_scrape > 1:
            for p_num in range(2, pages_to_scrape + 1):
                # Enforce 1.0s polite delay for reviews pagination
                time.sleep(getattr(self.config, "reviews_request_delay", 1.0))

                # Keep CSRF header in sync with session cookies if rotated
                current_csrf = self.session.cookies.get("csrf")
                if current_csrf:
                    headers["x-zomato-csrft"] = current_csrf

                p_json = None

                # If login popup was encountered, route directly via getPage web route to get status 200
                if use_getpage_fallback and clean_url_base:
                    try:
                        fallback_path = f"{reviews_referer_path}?page={p_num}"
                        fb_resp = self.session.get(
                            RESTAURANT_DETAILS_API_URL,
                            params={"page_url": fallback_path},
                            headers=headers,
                            timeout=self.config.request_timeout,
                        )
                        if fb_resp.status_code == 200:
                            p_json = fb_resp.json()
                        else:
                            # Direct HTML reviews page fetch
                            full_html_url = f"{BASE_URL}{reviews_referer_path}?page={p_num}"
                            h_resp = self.session.get(full_html_url, headers=headers, timeout=self.config.request_timeout)
                            if h_resp.status_code == 200:
                                p_json = extract_zomato_preloaded_state(h_resp.text)
                            else:
                                logger.warning(
                                    "Reviews fallback for %d (ID: %s) returned status %d",
                                    p_num,
                                    res_id,
                                    h_resp.status_code,
                                )
                    except Exception as exc:
                        logger.warning("Error fetching reviews fallback getPage %d for ID %s: %s", p_num, res_id, exc)
                else:
                    p_params = {
                        "res_id": res_id,
                        "page": p_num,
                        "sort": "dd",
                        "filter": "reviews-dining",
                    }
                    try:
                        p_resp = self.session.get(
                            REVIEWS_API_URL,
                            params=p_params,
                            headers=headers,
                            timeout=self.config.request_timeout,
                        )
                        if p_resp.status_code == 200:
                            p_json = p_resp.json()
                        elif p_resp.status_code == 401 and clean_url_base:
                            logger.info(
                                "Reviews loadMore page %d for restaurant ID %s prompted login (401); switching to getPage web route...",
                                p_num,
                                res_id,
                            )
                            use_getpage_fallback = True
                            fallback_path = f"{reviews_referer_path}?page={p_num}"
                            fb_resp = self.session.get(
                                RESTAURANT_DETAILS_API_URL,
                                params={"page_url": fallback_path},
                                headers=headers,
                                timeout=self.config.request_timeout,
                            )
                            if fb_resp.status_code == 200:
                                p_json = fb_resp.json()
                            else:
                                logger.warning(
                                    "Reviews fallback getPage %d for restaurant ID %s returned status %d",
                                    p_num,
                                    res_id,
                                    fb_resp.status_code,
                                )
                        else:
                            logger.warning(
                                "Reviews page %d for restaurant ID %s returned status %d",
                                p_num,
                                res_id,
                                p_resp.status_code,
                            )
                    except Exception as exc:
                        logger.warning("Error fetching reviews page %d for restaurant ID %s: %s", p_num, res_id, exc)

                if p_json is not None:
                    raw_pages.append({"page": p_num, "data": p_json})
                    _extract_page_content(p_json)
                    if p_num <= 5 or p_num % 5 == 0 or p_num == pages_to_scrape:
                        logger.info(
                            "   ↳ Reviews progress for '%s' (ID: %s): page %d/%d (%d reviews captured so far)...",
                            name or res_id,
                            res_id,
                            p_num,
                            pages_to_scrape,
                            len(normalized_reviews),
                        )

        pages_scraped_count = len(raw_pages) if total_pages > 0 else 0

        # Build consolidated document
        doc = format_restaurant_reviews_document(
            restaurant_id=res_id,
            name=name,
            city=city,
            location=location,
            page_url=reviews_referer_path,
            number_of_pages=total_pages,
            pages_scraped=pages_scraped_count,
            reviews=normalized_reviews,
            raw_reviews_by_id=raw_reviews_by_id,
            ratings=ratings,
            comments=comments,
            raw_pages=raw_pages,
        )

        # Target folder for saving consolidated JSON
        if target_folder is None:
            folder_slug = get_location_city_folder(location or "reviews", city or "general")
            dest_folder = self.config.reviews_dir / folder_slug
        else:
            dest_folder = Path(target_folder)

        dest_folder.mkdir(parents=True, exist_ok=True)
        file_path = dest_folder / f"{res_id}.json"

        with file_path.open("w", encoding="utf-8") as fh:
            json.dump(doc, fh, ensure_ascii=False, indent=2)

        # Upsert into MongoDB collection 'reviews'
        if self.mongo_manager and self.mongo_manager.is_connected():
            self.mongo_manager.upsert_restaurant_reviews(doc)

        logger.info(
            "✓ Saved reviews for '%s' (ID: %s) -> %d review(s) across %d/%d page(s) -> %s/%s",
            name or str(res_id),
            res_id,
            len(normalized_reviews),
            pages_scraped_count,
            total_pages,
            dest_folder.name,
            file_path.name,
        )
        return doc

    def scrape_all_restaurant_details(
        self,
        restaurants: List[Dict[str, Any]],
        city: str = "",
        location: str = "",
        folder_name: str = "",
        limit: Optional[int] = None,
        fetch_reviews: bool = True,
        reviews_max_pages: Optional[int] = None,
    ) -> List[Path]:
        """
        Loops through a list of restaurant items, extracts their page_urls,
        and scrapes full hotel details for each one via webroutes/getPage.
        Simultaneously extracts all customer reviews across all pages for each restaurant
        via webroutes/reviews/loadMore and stores into output_reviews/ and MongoDB 'reviews'.
        """
        if not restaurants:
            logger.info("No restaurants provided for details scraping.")
            return []

        unique_restaurants: Dict[Any, Dict[str, Any]] = {}
        for r in restaurants:
            if not isinstance(r, dict):
                continue
            rid = (
                r.get("info", {}).get("resId")
                if isinstance(r.get("info"), dict)
                else None
            ) or r.get("restaurant_id") or r.get("_id")
            if rid and rid not in unique_restaurants:
                unique_restaurants[rid] = r

        items_to_scrape = list(unique_restaurants.values())
        if limit and limit > 0:
            items_to_scrape = items_to_scrape[:limit]

        total = len(items_to_scrape)
        logger.info(
            "Starting Step 3: Scraping hotel details & reviews for %d restaurant(s)...",
            total,
        )

        folder_slug = folder_name or get_location_city_folder(location or "details", city or "general")
        dest_folder = self.config.details_dir / folder_slug
        dest_folder.mkdir(parents=True, exist_ok=True)
        reviews_dest_folder = self.config.reviews_dir / folder_slug
        reviews_dest_folder.mkdir(parents=True, exist_ok=True)

        saved_detail_files: List[Path] = []
        for idx, item in enumerate(items_to_scrape, 1):
            rid = (
                item.get("info", {}).get("resId")
                if isinstance(item.get("info"), dict)
                else None
            ) or item.get("restaurant_id") or item.get("_id")

            page_url = extract_restaurant_page_url(item)
            if not page_url:
                logger.warning(
                    "[%d/%d] Skipping restaurant ID %s: No page_url found.",
                    idx,
                    total,
                    rid,
                )
                continue

            r_city = item.get("city") or city
            r_loc = item.get("location") or location
            r_name = (
                item.get("info", {}).get("name")
                if isinstance(item.get("info"), dict)
                else None
            ) or item.get("name") or str(rid)

            logger.info(
                "[%d/%d] Requesting hotel details & reviews: '%s' (ID: %s, url: %s)",
                idx,
                total,
                r_name,
                rid,
                page_url,
            )

            res_data = self.scrape_restaurant_detail(
                page_url=page_url,
                restaurant_id=int(rid) if rid and str(rid).isdigit() else None,
                city=r_city,
                location=r_loc,
                target_folder=dest_folder,
                fetch_reviews=fetch_reviews,
                reviews_max_pages=reviews_max_pages,
                reviews_target_folder=reviews_dest_folder,
            )

            if res_data:
                saved_detail_files.append(dest_folder / f"{rid}.json")

            if self.config.request_delay > 0 and idx < total:
                time.sleep(self.config.request_delay)

        logger.info(
            "Step 3 Complete! Total %d hotel detail response(s) saved into: %s",
            len(saved_detail_files),
            dest_folder,
        )
        if self.mongo_manager and self.mongo_manager.is_connected():
            logger.info(
                "Total records in MongoDB collection [%s.%s]: %d",
                self.config.mongo_db,
                self.config.mongo_details_collection,
                self.mongo_manager.get_details_count(),
            )
            if fetch_reviews:
                logger.info(
                    "Total records in MongoDB collection [%s.%s]: %d",
                    self.config.mongo_db,
                    self.config.mongo_reviews_collection,
                    self.mongo_manager.get_reviews_count(),
                )

        return saved_detail_files

    def scrape_all_restaurant_reviews(
        self,
        restaurants: List[Dict[str, Any]],
        city: str = "",
        location: str = "",
        folder_name: str = "",
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[Path]:
        """
        Loops through a list of restaurant items and scrapes customer reviews
        for each one across all pages using webroutes/reviews/loadMore.
        """
        if not restaurants:
            logger.info("No restaurants provided for reviews scraping.")
            return []

        unique_restaurants: Dict[Any, Dict[str, Any]] = {}
        for r in restaurants:
            if not isinstance(r, dict):
                continue
            rid = (
                r.get("info", {}).get("resId")
                if isinstance(r.get("info"), dict)
                else None
            ) or r.get("restaurant_id") or r.get("_id")
            if rid and rid not in unique_restaurants:
                unique_restaurants[rid] = r

        items_to_scrape = list(unique_restaurants.values())
        if limit and limit > 0:
            items_to_scrape = items_to_scrape[:limit]

        total = len(items_to_scrape)
        logger.info(
            "Starting Reviews Scraping for %d restaurant(s)...",
            total,
        )

        folder_slug = folder_name or get_location_city_folder(location or "reviews", city or "general")
        dest_folder = self.config.reviews_dir / folder_slug
        dest_folder.mkdir(parents=True, exist_ok=True)

        saved_review_files: List[Path] = []
        for idx, item in enumerate(items_to_scrape, 1):
            rid = (
                item.get("info", {}).get("resId")
                if isinstance(item.get("info"), dict)
                else None
            ) or item.get("restaurant_id") or item.get("_id")

            page_url = extract_restaurant_page_url(item) or ""
            r_city = item.get("city") or city
            r_loc = item.get("location") or location
            r_name = (
                item.get("info", {}).get("name")
                if isinstance(item.get("info"), dict)
                else None
            ) or item.get("name") or str(rid)

            logger.info(
                "[%d/%d] Requesting reviews: '%s' (ID: %s)",
                idx,
                total,
                r_name,
                rid,
            )

            res_doc = self.scrape_restaurant_reviews(
                restaurant_id=int(rid) if rid and str(rid).isdigit() else rid,
                name=r_name,
                city=r_city,
                location=r_loc,
                page_url=page_url,
                max_pages=max_pages,
                target_folder=dest_folder,
            )

            if res_doc:
                saved_review_files.append(dest_folder / f"{rid}.json")

            if self.config.request_delay > 0 and idx < total:
                time.sleep(self.config.request_delay)

        logger.info(
            "Completed reviews scraping: %d/%d file(s) saved into: %s",
            len(saved_review_files),
            total,
            dest_folder,
        )
        return saved_review_files


def print_location_suggestions(
    suggestions: List[Dict[str, Any]],
    city_input: str,
    extra_flags: str = "",
) -> None:
    """
    Prints all locations from Search API with latlums formatted as runnable CLI commands:
    [1] Location: <Title>
        Address:  <Full Address>
        Command:  python3 scraper.py --location "<Title>" --city "<City>" --lat <lat> --lon <lon>
    """
    print("\n" + "=" * 90)
    print(f"             AVAILABLE LOCATIONS FOR '{city_input}' (SEARCH API)")
    print("=" * 90)
    print("Select a location from below or run its command directly with the exact latlums:\n")

    for idx, s in enumerate(suggestions, 1):
        title = s.get("entity_title") or "Location"
        full_name = s.get("entity_name") or ""
        lat = s.get("latitude")
        lon = s.get("longitude")

        lat_str = f"{float(lat):.6f}" if lat is not None else "0.0"
        lon_str = f"{float(lon):.6f}" if lon is not None else "0.0"

        safe_title = str(title).replace('"', '')
        safe_city = str(city_input).replace('"', '')

        print(f"[{idx}] Location: {title}")
        if full_name and full_name != title:
            print(f"    Address:  {full_name}")
        cmd = f'python3 scraper.py --location "{safe_title}" --city "{safe_city}" --lat {lat_str} --lon {lon_str}{extra_flags}'
        print(f"    Command:  {cmd}\n")

    # Option at the end to extract and scrape all discovered locations at once
    all_idx = len(suggestions) + 1
    safe_city = str(city_input).replace('"', '')
    print(f"[{all_idx}] ALL LOCATIONS: Extract & Scrape All {len(suggestions)} Locations Sequentially")
    print(f"    Command:  python3 scraper.py --city \"{safe_city}\" --all{extra_flags}\n")

    print("=" * 90 + "\n")


def parse_restaurant_data(response_file_or_data: Any) -> List[Dict[str, Any]]:
    """
    Parser utility: Extracts structured restaurant details from full raw response.
    Can be used on any saved {offset}_{page}.json file or parsed dictionary.
    """
    if isinstance(response_file_or_data, (str, Path)):
        with Path(response_file_or_data).open("r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = response_file_or_data

    sections = data.get("sections", {}) if isinstance(data, dict) else {}
    results = sections.get("SECTION_SEARCH_RESULT", [])

    parsed_restaurants: List[Dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        info = item.get("info", {})
        if not info:
            continue

        res_id = info.get("resId")
        name = info.get("name")
        cuisines = [c.get("name") for c in info.get("cuisine", []) if isinstance(c, dict) and c.get("name")]
        rating_obj = info.get("rating", {})
        aggregate_rating = rating_obj.get("aggregate_rating") if isinstance(rating_obj, dict) else None
        rating_text = rating_obj.get("rating_text") if isinstance(rating_obj, dict) else None

        cft = info.get("cft", {})
        cost_text = cft.get("text") if isinstance(cft, dict) else None

        order = item.get("order", {})
        action_info = order.get("actionInfo", {}) if isinstance(order, dict) else {}
        restaurant_url = action_info.get("clickUrl") or info.get("o2Url") or ""
        if restaurant_url and not restaurant_url.startswith("http"):
            restaurant_url = f"{BASE_URL}{restaurant_url}"

        parsed_restaurants.append({
            "restaurant_id": res_id,
            "name": name,
            "rating": aggregate_rating,
            "rating_text": rating_text,
            "cost_for_two": cost_text,
            "cuisines": cuisines,
            "locality": info.get("locality", {}).get("name") if isinstance(info.get("locality"), dict) else "",
            "url": restaurant_url,
        })

    return parsed_restaurants


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Zomato Restaurant Scraper with Keyword-to-Latlums Auto-Discovery, Location Folders & MongoDB Storage"
    )
    parser.add_argument("--city", type=str, help="City name or keyword input (e.g. hydera, Hyderabad, Bengaluru)")
    parser.add_argument("--location", type=str, help="Specific locality/area name (e.g. Majestic, Nampally)")
    parser.add_argument("--lat", type=float, help="Optional Latitude (auto-discovered from keyword if omitted)")
    parser.add_argument("--lon", type=float, help="Optional Longitude (auto-discovered from keyword if omitted)")
    parser.add_argument(
        "--pages",
        type=int,
        help="Max pages to scrape (omit or set 0 to scrape until page contains no restaurants)",
    )
    parser.add_argument("--start-page", type=int, help="Starting page number (default: 1)")
    parser.add_argument("--start-offset", type=int, help="Starting offset value (default: 0)")
    parser.add_argument(
        "--select",
        type=str,
        help="Select location index directly (1-based index) or 'all' to scrape all locations",
    )
    parser.add_argument(
        "--all",
        "--all-locations",
        action="store_true",
        help="Extract and scrape all discovered locations with latlums for the city sequentially",
    )
    parser.add_argument(
        "--search-locations",
        action="store_true",
        help="Search and list all matching locations with runnable commands",
    )
    parser.add_argument(
        "--check-status",
        action="store_true",
        help="Run pre-flight check to verify if APIs and MongoDB return data",
    )
    parser.add_argument(
        "--parse",
        action="store_true",
        help="Also parse and display restaurant records after scraping",
    )
    parser.add_argument(
        "--sync-mongo",
        action="store_true",
        help="Sync all previously scraped JSON response files from output/ into MongoDB collection",
    )
    parser.add_argument(
        "--details",
        "-d",
        action="store_true",
        help="Continue directly to Step 3: Scrape hotel details for all extracted restaurants",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Auto-confirm all interactive prompts (including Step 3 continuation)",
    )
    parser.add_argument(
        "--no-details",
        action="store_true",
        help="Skip Step 3 hotel details scraping without asking",
    )
    parser.add_argument(
        "--scrape-details",
        action="store_true",
        help="Standalone Step 3: Scrape hotel details for restaurants saved in MongoDB or output folders",
    )
    parser.add_argument(
        "--restaurant-url",
        type=str,
        help="Scrape hotel details for a single specific restaurant page URL (e.g. /hyderabad/exotica-banjara-hills/info)",
    )
    parser.add_argument(
        "--sync-details-mongo",
        action="store_true",
        help="Sync all saved hotel detail JSON files from output_details/ into MongoDB collection 'restaurants_data'",
    )
    parser.add_argument(
        "--sync-delivery",
        "--parse-delivery",
        action="store_true",
        help="Parse hotel details & reviews into MongoDB collection 'delivery_data' and output_delivery/",
    )
    parser.add_argument(
        "--details-limit",
        type=int,
        help="Limit the number of hotel details to scrape (e.g. 10)",
    )
    parser.add_argument(
        "--max-review-pages",
        type=int,
        help="Maximum review pages to scrape per restaurant (e.g. 5, 10, 20). If omitted, scrapes all available pages.",
    )
    parser.add_argument(
        "--no-mongo",
        action="store_true",
        help="Disable MongoDB storage for this run",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        help="MongoDB connection URI override (default: mongodb://localhost:27017/)",
    )

    args = parser.parse_args()
    config = Config()

    if args.no_mongo:
        config.enable_mongo = False
    if args.mongo_uri:
        config.mongo_uri = args.mongo_uri
    if args.max_review_pages is not None:
        config.max_review_pages = args.max_review_pages

    # Handle sync-mongo standalone mode
    if args.sync_mongo:
        scraper = ZomatoScraper(config)
        if not scraper.mongo_manager or not scraper.mongo_manager.is_connected():
            logger.error("Failed to connect to MongoDB. Cannot sync files.")
            sys.exit(1)
        print("\n" + "=" * 60)
        print("          SYNCING EXISTING OUTPUT TO MONGODB")
        print("=" * 60)
        res = sync_output_folder_to_mongo(config.output_dir, scraper.mongo_manager)
        print(f"Files Processed:    {res.get('files_processed', 0)}")
        print(f"Items Processed:    {res.get('items_processed', 0)}")
        print(f"Newly Upserted:     {res.get('upserted', 0)}")
        print(f"Updated/Modified:   {res.get('modified', 0)}")
        print(f"Total Unique in DB: {res.get('total_in_db', 0):,}")
        print("=" * 60 + "\n")
        return

    # Handle sync-details-mongo standalone mode (automatically syncs details, reviews, and delivery data)
    if args.sync_details_mongo:
        scraper = ZomatoScraper(config)
        if not scraper.mongo_manager or not scraper.mongo_manager.is_connected():
            logger.error("Failed to connect to MongoDB. Cannot sync detail and review files.")
            sys.exit(1)
        print("\n" + "=" * 60)
        print("    SYNCING HOTEL DETAILS & REVIEWS TO MONGODB")
        print("=" * 60)
        res_details = sync_details_folder_to_mongo(config.details_dir, scraper.mongo_manager)
        print(f"Details Files:      {res_details.get('files_processed', 0)}")
        print(f"Details Upserted:   {res_details.get('upserted', 0)}")
        print(f"Details in DB:      {res_details.get('total_in_db', 0):,}")
        res_reviews = sync_reviews_folder_to_mongo(config.reviews_dir, scraper.mongo_manager)
        print(f"Reviews Files:      {res_reviews.get('files_processed', 0)}")
        print(f"Reviews Upserted:   {res_reviews.get('upserted', 0)}")
        print(f"Reviews in DB:      {res_reviews.get('total_in_db', 0):,}")
        res_delivery = sync_delivery_folder_to_mongo(
            details_dir=config.details_dir,
            reviews_dir=config.reviews_dir,
            delivery_dir=config.delivery_dir,
            mongo_manager=scraper.mongo_manager,
        )
        print(f"Delivery Parsed:    {res_delivery.get('processed', 0)}")
        print(f"Delivery Upserted:  {res_delivery.get('upserted', 0)}")
        print(f"Delivery in DB:     {res_delivery.get('total_in_db', 0):,}")
        print("=" * 60 + "\n")
        return

    # Handle sync-delivery standalone mode (parses details & reviews into delivery_data)
    if args.sync_delivery:
        scraper = ZomatoScraper(config)
        print("\n" + "=" * 60)
        print("    PARSING & SYNCING DELIVERY DATA TO MONGODB")
        print("=" * 60)
        res_deliv = sync_delivery_folder_to_mongo(
            details_dir=config.details_dir,
            reviews_dir=config.reviews_dir,
            delivery_dir=config.delivery_dir,
            mongo_manager=scraper.mongo_manager,
        )
        print(f"Records Processed:  {res_deliv.get('processed', 0)}")
        print(f"Newly Upserted:     {res_deliv.get('upserted', 0)}")
        print(f"Updated/Modified:   {res_deliv.get('modified', 0)}")
        if scraper.mongo_manager and scraper.mongo_manager.is_connected():
            print(f"Total in DB ({scraper.mongo_manager.delivery_collection_name}): {res_deliv.get('total_in_db', 0):,}")
        print(f"Output Directory:   {config.delivery_dir}/")
        print("=" * 60 + "\n")
        return

    # Handle single restaurant-url standalone mode (automatically scrapes details and reviews)
    if args.restaurant_url:
        scraper = ZomatoScraper(config)
        print("\n" + "=" * 60)
        print("       SCRAPING SINGLE RESTAURANT DETAILS & REVIEWS")
        print("=" * 60)
        print(f"Target URL: {args.restaurant_url}")
        res = scraper.scrape_restaurant_detail(
            page_url=args.restaurant_url,
            city=args.city or "",
            location=args.location or "",
        )
        if res:
            print("✓ Successfully scraped and saved hotel details & reviews.")
        else:
            print("✗ Failed to scrape hotel details.")
        print("=" * 60 + "\n")
        return

    # Handle scrape-details standalone mode (for existing restaurants, automatically scrapes reviews)
    if args.scrape_details:
        scraper = ZomatoScraper(config)
        restaurants_to_detail: List[Dict[str, Any]] = []

        if scraper.mongo_manager and scraper.mongo_manager.is_connected():
            restaurants_to_detail = scraper.mongo_manager.get_restaurants_for_details(
                city=args.city,
                limit=args.details_limit,
                unscraped_only=True,
            )

        if not restaurants_to_detail:
            # Fallback to output folder JSON files
            json_files = sorted(config.output_dir.glob("*/*.json"))
            for jf in json_files:
                try:
                    with jf.open("r", encoding="utf-8") as fh:
                        d = json.load(fh)
                    items = d.get("sections", {}).get("SECTION_SEARCH_RESULT", [])
                    if isinstance(items, list):
                        restaurants_to_detail.extend(items)
                except Exception:
                    pass

        # Deduplicate
        seen_rids = set()
        deduped = []
        for r in restaurants_to_detail:
            rid = (
                r.get("info", {}).get("resId")
                if isinstance(r.get("info"), dict)
                else None
            ) or r.get("restaurant_id") or r.get("_id")
            if rid and rid not in seen_rids:
                seen_rids.add(rid)
                deduped.append(r)
        restaurants_to_detail = deduped

        if args.details_limit and args.details_limit > 0:
            restaurants_to_detail = restaurants_to_detail[:args.details_limit]

        if not restaurants_to_detail:
            logger.info("No restaurants found to scrape details for.")
            return

        print("\n" + "=" * 80)
        print("             STEP 3: HOTEL DETAILS & REVIEWS SCRAPING")
        print("=" * 80)
        print(f"Found {len(restaurants_to_detail)} restaurant(s) ready for extraction.")
        print(f"Details API:        {RESTAURANT_DETAILS_API_URL}")
        print(f"Reviews API:        {REVIEWS_API_URL}")
        print(f"Target Details:     {config.details_dir}/")
        print(f"Target Reviews:     {config.reviews_dir}/")
        print(f"MongoDB Collection: {config.mongo_db}.{config.mongo_details_collection} & {config.mongo_db}.{config.mongo_reviews_collection}")
        print("=" * 80)

        should_proceed = args.yes or args.details
        if not should_proceed:
            try:
                ans = input(f"Do you want to continue to scrape hotel details and customer reviews for these {len(restaurants_to_detail)} restaurant(s)? [y/N]: ").strip().lower()
                should_proceed = ans in ("y", "yes")
            except (KeyboardInterrupt, EOFError):
                should_proceed = False

        if should_proceed:
            scraper.scrape_all_restaurant_details(
                restaurants=restaurants_to_detail,
                city=args.city or "",
                location=args.location or "",
                limit=args.details_limit,
                reviews_max_pages=args.max_review_pages,
            )
        else:
            print("\nAborted hotel details & reviews scraping.\n")
        return

    scraper = ZomatoScraper(config)

    target_city = args.city or config.city
    target_lat = args.lat
    target_lon = args.lon
    target_location = args.location

    # If lat/lon are not provided (or --search-locations / --all is passed), list all matching locations with runnable commands
    if target_lat is None or target_lon is None or args.search_locations or args.all:
        search_query = args.location or target_city
        suggestions = scraper.get_location_suggestions(search_query)
        if not suggestions:
            logger.error("No location suggestions found for query: '%s'", search_query)
            sys.exit(1)

        extra_flags = []
        if args.pages:
            extra_flags.append(f"--pages {args.pages}")
        if args.parse:
            extra_flags.append("--parse")
        if args.no_mongo:
            extra_flags.append("--no-mongo")
        extra_str = " " + " ".join(extra_flags) if extra_flags else ""

        print_location_suggestions(suggestions, city_input=target_city, extra_flags=extra_str)

        if args.search_locations:
            return

        all_option_idx = len(suggestions) + 1
        scrape_all = False
        selected_idx: Optional[int] = None

        if args.all:
            scrape_all = True
        elif args.select is not None:
            select_val = str(args.select).strip().lower()
            if select_val in ("all", "a") or select_val == str(all_option_idx):
                scrape_all = True
            elif select_val.isdigit() and 1 <= int(select_val) <= len(suggestions):
                selected_idx = int(select_val) - 1
            else:
                logger.error(
                    "Invalid --select option '%s' (must be between 1 and %d, or '%d'/'all').",
                    args.select,
                    len(suggestions),
                    all_option_idx,
                )
                sys.exit(1)
        elif sys.stdin.isatty():
            try:
                choice = input(
                    f"Enter location number [1-{len(suggestions)}] or [{all_option_idx}/all] to scrape ALL locations (or Enter to exit): "
                ).strip()
                choice_lower = choice.lower()
                if choice_lower in ("all", "a") or choice == str(all_option_idx):
                    scrape_all = True
                elif choice.isdigit() and 1 <= int(choice) <= len(suggestions):
                    selected_idx = int(choice) - 1
                else:
                    print("Exiting. You can copy and run any of the commands printed above with the chosen location and latlums.\n")
                    return
            except (KeyboardInterrupt, EOFError):
                print("\nAborted.")
                return
        else:
            print(
                f"Tip: Run one of the above commands with --location, --city, --lat, and --lon, "
                f"or pass --select <1..{len(suggestions)}>, or pass --all to scrape all locations sequentially.\n"
            )
            return

        if scrape_all:
            # Batch all locations mode: Ask once for Step 3 details & reviews confirmation if not pre-specified
            scrape_details = True
            if args.no_details:
                scrape_details = False
            elif not (args.details or args.yes):
                if sys.stdin.isatty():
                    try:
                        ans = input(
                            f"\nDo you want to scrape hotel details and customer reviews for all {len(suggestions)} locations? [Y/n]: "
                        ).strip().lower()
                        if ans in ("n", "no"):
                            scrape_details = False
                    except (KeyboardInterrupt, EOFError):
                        print("\nAborted.")
                        return

            scraper.scrape_all_locations(
                city=target_city,
                suggestions=suggestions,
                max_pages=args.pages,
                start_page=args.start_page or 1,
                start_offset=args.start_offset or 0,
                scrape_details=scrape_details,
                details_limit=args.details_limit,
                reviews_max_pages=args.max_review_pages,
            )
            return

        if selected_idx is None:
            return

        selected = suggestions[selected_idx]
        target_lat = selected["latitude"]
        target_lon = selected["longitude"]
        target_location = selected["entity_title"]
        print(f"\nProceeding with [{selected_idx + 1}] Location: '{target_location}' (lat={target_lat}, lon={target_lon})...\n")

    # 1. First check the status to see if APIs get data or not
    status_ok = scraper.check_status(
        city=target_city,
        lat=target_lat,
        lon=target_lon,
        location=target_location,
    )
    if not status_ok:
        logger.error("API status check failed. Please check network/connectivity.")
        sys.exit(1)

    if args.check_status:
        logger.info("Status check completed successfully (--check-status mode). Exiting.")
        return

    # 2. Scrape full restaurant responses into output/{location}_{city}/{offset}_{page}.json
    target_folder, saved_files = scraper.scrape(
        city=target_city,
        lat=target_lat,
        lon=target_lon,
        location=target_location,
        max_pages=args.pages,
        start_page=args.start_page,
        start_offset=args.start_offset,
    )

    print("\n" + "=" * 60)
    print("                    SCRAPING SUMMARY")
    print("=" * 60)
    print(f"Target Location Folder: {target_folder}")
    print(f"Total Response Files Saved: {len(saved_files)}")
    for f in saved_files:
        print(f"  • {f.name} -> {f}")
    if scraper.mongo_manager and scraper.mongo_manager.is_connected():
        print(f"MongoDB Database:       {config.mongo_db}")
        print(f"MongoDB Collection:     {config.mongo_collection}")
        print(f"Total Unique in DB:     {scraper.mongo_manager.get_total_count():,} restaurant(s)")

    # 3. Optional parsing preview
    if args.parse and saved_files:
        print("\n" + "=" * 60)
        print("                PARSED RESTAURANTS PREVIEW")
        print("=" * 60)
        first_file = saved_files[0]
        parsed = parse_restaurant_data(first_file)
        print(f"Showing first 5 restaurants from {first_file.name}:")
        for idx, r in enumerate(parsed[:5], 1):
            print(f"  {idx}. {r['name']} | Rating: {r['rating']} | Cuisines: {', '.join(r['cuisines'][:3])}")
            if r['url']:
                print(f"     URL: {r['url']}")
        print("=" * 60 + "\n")

    # 4. Step 3: Hotel Details & Reviews Scraping (webroutes/getPage & webroutes/reviews/loadMore)
    extracted_restaurants = scraper.last_scraped_restaurants
    if not extracted_restaurants and saved_files:
        for sf in saved_files:
            try:
                with sf.open("r", encoding="utf-8") as fh:
                    d = json.load(fh)
                items = d.get("sections", {}).get("SECTION_SEARCH_RESULT", [])
                if isinstance(items, list):
                    extracted_restaurants.extend(items)
            except Exception:
                pass

    # Deduplicate extracted restaurants
    seen_ids = set()
    deduped_extracted = []
    for r in extracted_restaurants:
        rid = (
            r.get("info", {}).get("resId")
            if isinstance(r.get("info"), dict)
            else None
        ) or r.get("restaurant_id") or r.get("_id")
        if rid and rid not in seen_ids:
            seen_ids.add(rid)
            deduped_extracted.append(r)
    extracted_restaurants = deduped_extracted

    if extracted_restaurants and not args.no_details:
        total_extracted = len(extracted_restaurants)
        folder_slug = target_folder.name

        print("=" * 90)
        print("             STEP 3: HOTEL DETAILS & REVIEWS SCRAPING")
        print("=" * 90)
        print(f"Extracted {total_extracted} unique restaurant(s) from search results.")
        print(f"Next: Scrape full hotel details (contact phone, full address, timings, rating)")
        print(f"      via: {RESTAURANT_DETAILS_API_URL}")
        print(f"      and customer reviews across all pages via: {REVIEWS_API_URL}")
        print(f"Details Folder:      output_details/{folder_slug}/")
        print(f"Reviews Folder:      output_reviews/{folder_slug}/")
        print(f"MongoDB Collections: {config.mongo_db}.{config.mongo_details_collection}")
        print(f"                     {config.mongo_db}.{config.mongo_reviews_collection}")
        print("=" * 90 + "\n")

        should_continue_details = False

        if args.details or args.yes:
            should_continue_details = True
            logger.info("Continuing to Step 3 hotel details & reviews (--details/--yes flag supplied)...")
        else:
            prompt_msg = (
                f"Do you want to continue to scrape hotel details and customer reviews for these "
                f"{total_extracted} restaurant(s)? [y/N]: "
            )
            try:
                user_resp = input(prompt_msg).strip().lower()
                should_continue_details = user_resp in ("y", "yes")
            except (KeyboardInterrupt, EOFError):
                should_continue_details = False

        if should_continue_details:
            print("\n" + "-" * 90)
            print(f"Starting Step 3: Fetching hotel details & reviews for {total_extracted} restaurant(s)...")
            print("-" * 90 + "\n")
            scraper.scrape_all_restaurant_details(
                restaurants=extracted_restaurants,
                city=target_city or "",
                location=target_location or folder_slug,
                folder_name=folder_slug,
                limit=args.details_limit,
                reviews_max_pages=args.max_review_pages,
            )
        else:
            print(
                f"\nSkipping hotel details and reviews scraping.\n"
                f"You can scrape hotel details & reviews anytime using:\n"
                f"  python3 scraper.py --scrape-details --city \"{target_city}\"\n"
            )


if __name__ == "__main__":
    main()
