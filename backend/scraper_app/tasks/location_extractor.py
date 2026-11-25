"""
tasks/location_extractor.py
Extracts location suggestions and coordinates for any city or locality query,
stores them into the 'latlong_data' PostgreSQL database table, and provides
structured location choices for user selection in the frontend.
"""

import logging
import re
import uuid
from typing import Any, Dict, List, Optional
from config import Config
from scraper import ZomatoScraper, slugify
from scraper_app.models import LatLongData

logger = logging.getLogger("location_extractor")


def extract_locations_and_store(
    query: str,
    request_id: Optional[Any] = None,
    user: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    Extracts all location suggestions and coordinates for a user-entered location query.
    Generates ONE unified requestid (UUID) for the entire batch of locations entered by the user.
    All extracted locations in the batch are saved in 'latlong_data' with that exact requestid.
    This exact requestid is then used as the primary key for batch extraction in 'zomato_batches'.
    """
    clean_query = str(query or "").strip()
    if not clean_query:
        return []

    # Resolve ONE unified request UUID for this entire batch of locations
    req_uuid: uuid.UUID
    if isinstance(request_id, uuid.UUID):
        req_uuid = request_id
    elif isinstance(request_id, str) and request_id.strip():
        try:
            req_uuid = uuid.UUID(request_id.strip())
        except ValueError:
            req_uuid = uuid.uuid4()
    else:
        req_uuid = uuid.uuid4()
    req_id_str = str(req_uuid)

    logger.info("[LOCATION_EXTRACTOR] Extracting locations for query: '%s' (batch request_id=%s)", clean_query, req_id_str)
    scraper = ZomatoScraper(config=Config(enable_mongo=False))
    raw_suggestions = []
    try:
        raw_suggestions = scraper.get_location_suggestions(clean_query)
    except Exception as exc:
        logger.warning("[LOCATION_EXTRACTOR] Error fetching suggestions for '%s': %s", clean_query, exc)
    finally:
        scraper.close()

    formatted_locations: List[Dict[str, Any]] = []
    stored_records: List[LatLongData] = []

    clean_q_lower = clean_query.lower()
    for s in (raw_suggestions or []):
        entity_id = str(s.get("entity_id") or "0")
        entity_type = str(s.get("entity_type") or "subzone")
        entity_title = str(s.get("entity_title") or s.get("entity_name") or clean_query.title()).strip()
        entity_name = str(s.get("entity_name") or entity_title).strip()
        raw_sug = s.get("raw_suggestion") if isinstance(s.get("raw_suggestion"), dict) else {}
        city_name_from_sug = str(raw_sug.get("cityName") or "").strip()

        # Filter out foreign / out-of-state suggestions (e.g. "Hyderabad Gate in Varanasi, UP" when searching "Hyderabad")
        if len(clean_q_lower) >= 3:
            sug_text = f"{entity_name} {city_name_from_sug}".lower()
            if clean_q_lower not in sug_text:
                continue

        parent_city = city_name_from_sug or clean_query.title()

        lat_val = s.get("latitude")
        lon_val = s.get("longitude")
        if lat_val is None and raw_sug:
            lat_val = raw_sug.get("latitude") or raw_sug.get("userDefinedLatitude")
        if lon_val is None and raw_sug:
            lon_val = raw_sug.get("longitude") or raw_sug.get("userDefinedLongitude")

        lat = None
        lon = None
        try:
            if lat_val is not None:
                lat = float(lat_val)
        except (ValueError, TypeError):
            lat = None

        try:
            if lon_val is not None:
                lon = float(lon_val)
        except (ValueError, TypeError):
            lon = None

        place_id = str(raw_sug.get("placeId") or s.get("place_id") or "")
        cell_id = str(raw_sug.get("cellId") or s.get("cell_id") or "")
        subzone_id_val = raw_sug.get("deliverySubzoneId") or s.get("delivery_subzone_id") or 0
        try:
            subzone_id = int(subzone_id_val)
        except (ValueError, TypeError):
            subzone_id = 0

        # All locations in this user query share the SAME unified batch requestid
        record = LatLongData(
            requestid=req_uuid,
            user=user,
            search_query=clean_query,
            city=parent_city,
            entity_id=entity_id,
            entity_type=entity_type,
            entity_title=entity_title,
            entity_name=entity_name,
            latitude=lat,
            longitude=lon,
            place_id=place_id,
            cell_id=cell_id,
            delivery_subzone_id=subzone_id,
            raw_data=raw_sug,
        )
        stored_records.append(record)

        formatted_locations.append({
            "request_id": req_id_str,
            "entity_id": entity_id,
            "entity_type": entity_type,
            "entity_title": entity_title,
            "entity_name": entity_name,
            "latitude": lat,
            "longitude": lon,
            "place_id": place_id,
            "cell_id": cell_id,
            "delivery_subzone_id": subzone_id,
        })

    # Bulk insert into latlong_data in PostgreSQL
    if stored_records:
        try:
            LatLongData.objects.bulk_create(stored_records, ignore_conflicts=True)
            logger.info(
                "[LOCATION_EXTRACTOR] Successfully stored %d location suggestions with unified request_id '%s' in 'latlong_data' for query '%s'",
                len(stored_records),
                req_id_str,
                clean_query,
            )
        except Exception as db_err:
            logger.warning("[LOCATION_EXTRACTOR] Failed to bulk save latlong_data: %s", db_err)

    return formatted_locations


def get_stored_locations(query: Optional[str] = None, request_id: Optional[Any] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Retrieves stored location records from 'latlong_data' table by request_id or query."""
    qs = LatLongData.objects.all()
    if request_id:
        qs = qs.filter(requestid=request_id)
    if query:
        clean_query = str(query or "").strip()
        qs = qs.filter(search_query__iexact=clean_query)
    records = qs.order_by("-created_at")[:limit]
    return [
        {
            "id": r.id,
            "request_id": r.request_id,
            "entity_id": r.entity_id,
            "entity_type": r.entity_type,
            "entity_title": r.entity_title,
            "entity_name": r.entity_name,
            "latitude": float(r.latitude) if r.latitude else None,
            "longitude": float(r.longitude) if r.longitude else None,
            "place_id": r.place_id,
            "cell_id": r.cell_id,
            "delivery_subzone_id": r.delivery_subzone_id,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in records
    ]
