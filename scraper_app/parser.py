"""
Delivery Data Parser & Simultaneous Synchronization for Zomato Scraper.

Parses raw data from 'restaurants_data' (getPage) and 'reviews' (getPage reviews)
into the unified 'delivery_data' MongoDB collection.

Includes the 'total_reviews' count field computed from restaurant_data
as well as all rich fields from restaurant details (cuisines, cost for two,
popular dishes, highlights, known for, people liked, media, coordinates).
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("ZomatoParser")

DEFAULT_FIELDS = [
    "restaurant_id",
    "name",
    "city",
    "location",
    "address",
    "phone_numbers",
    "timing",
    "rating",
    "canonical_url",
    "aggregate_rating",
    "votes",
    "delivery_rating",
    "reviewcount",
    "total_reviews",
    "cuisines",
    "average_cost_for_two",
    "popular_dishes",
    "static_map_url",
]

ALL_FIELDS = DEFAULT_FIELDS + [
    "known_for",
    "people_liked",
    "highlights",
    "latitude",
    "longitude",
    "zipcode",
    "image_url",
    "status_text",
    "is_delivery_only",
    "reviews",
    "parsed_at",
]

EXAMPLE_SCHEMA_DOCUMENT = {
    "restaurant_id": 18438922,
    "name": "Bawarchi Restaurant",
    "city": "Hyderabad",
    "location": "RTC X Roads",
    "address": "Plot 66, Beside Sandhya Theatre, RTC X Roads, Hyderabad",
    "phone_numbers": "+914027634242, +914027634243",
    "timing": "11:30am – 11:30pm (Today)",
    "rating": "4.2",
    "canonical_url": "https://www.zomato.com/hyderabad/bawarchi-rtc-x-roads",
    "aggregate_rating": "4.1",
    "votes": 16200,
    "delivery_rating": "4.5",
    "reviewcount": 639800,
    "total_reviews": 656000,
    "cuisines": "Biryani, North Indian, Seafood, Mughlai, Desserts",
    "average_cost_for_two": "₹300 for two people (approx.)",
    "popular_dishes": "Tender Lamb, Bawarchi Biryani, Mutton Dum, Fry Prawns",
    "known_for": "Delicious hyderabadi biryani",
    "people_liked": "Service is Good, Fast Delivered, Best Delivery",
    "highlights": "Lunch, Dinner, Home delivery, Takeaway available, Indoor seating",
    "static_map_url": "https://maps.zomato.com/php/staticmap?center=17.402,78.498&zoom=16&size=500x250",
    "latitude": 17.4024,
    "longitude": 78.4981,
    "zipcode": "500020",
    "image_url": "https://b.zmtcdn.com/data/pictures/2/18438922/thumb.jpg",
    "status_text": "Open now",
    "is_delivery_only": False,
    "reviews": [
        {
            "user_name": "Rahul Sharma",
            "rating": "5",
            "review_text": "Authentic Hyderabadi biryani with great flavors and tender meat!",
            "like_count": 12,
        }
    ],
    "parsed_at": "2026-11-18T14:50:00Z",
}


def parse_numeric_metric(val: Any) -> int:
    """Converts metric numbers with K/M/B multipliers (e.g. '16.2K' -> 16200) to int."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip().upper().replace(",", "")
    if not s:
        return 0
    multiplier = 1
    if s.endswith("K"):
        multiplier = 1000
        s = s[:-1]
    elif s.endswith("M"):
        multiplier = 1000000
        s = s[:-1]
    elif s.endswith("B"):
        multiplier = 1000000000
        s = s[:-1]
    try:
        return int(float(s) * multiplier)
    except (ValueError, TypeError):
        return 0


def extract_total_reviews_from_restaurant_data(
    detail_data: Dict[str, Any],
    reviews_data: Optional[Dict[str, Any]] = None,
) -> int:
    """
    Computes total_reviews from restaurant_data:
    1. Sum of Dining reviewCount and Delivery reviewCount from SECTION_BASIC_INFO ratings.
    2. Fallback to basic_info votes or rating reviewCount.
    3. Fallback to SECTION_REVIEWS totalCount or numberOfPages.
    4. Fallback to reviews count in reviews_data.
    """
    if not isinstance(detail_data, dict):
        return 0

    page_data = detail_data.get("page_data", {}) if isinstance(detail_data.get("page_data"), dict) else detail_data
    sections = page_data.get("sections", {}) if isinstance(page_data.get("sections"), dict) else {}
    basic_info = sections.get("SECTION_BASIC_INFO", {}) if isinstance(sections.get("SECTION_BASIC_INFO"), dict) else {}

    # Check rating_new map
    dining_reviews = 0
    delivery_reviews = 0
    rating_new = basic_info.get("rating_new", {}) if isinstance(basic_info.get("rating_new"), dict) else {}
    ratings_map = rating_new.get("ratings", {}) if isinstance(rating_new.get("ratings"), dict) else {}

    dining_obj = ratings_map.get("DINING") or ratings_map.get("dining") or {}
    if isinstance(dining_obj, dict):
        dining_reviews = parse_numeric_metric(dining_obj.get("reviewCount") or dining_obj.get("votes"))

    delivery_obj = ratings_map.get("DELIVERY") or ratings_map.get("delivery") or {}
    if isinstance(delivery_obj, dict):
        delivery_reviews = parse_numeric_metric(delivery_obj.get("reviewCount") or delivery_obj.get("votes"))

    # Also check basic rating object
    basic_rating = basic_info.get("rating", {}) if isinstance(basic_info.get("rating"), dict) else {}
    if not dining_reviews and isinstance(basic_rating, dict):
        dining_reviews = parse_numeric_metric(basic_rating.get("votes"))

    combined_count = dining_reviews + delivery_reviews
    if combined_count > 0:
        return combined_count

    # Check SECTION_REVIEWS inside detail_data or page_data
    sec_reviews = sections.get("SECTION_REVIEWS", {}) if isinstance(sections.get("SECTION_REVIEWS"), dict) else {}
    if isinstance(sec_reviews, dict):
        if sec_reviews.get("totalCount"):
            t_cnt = parse_numeric_metric(sec_reviews.get("totalCount"))
            if t_cnt > 0:
                return t_cnt
        if sec_reviews.get("numberOfPages"):
            p_cnt = parse_numeric_metric(sec_reviews.get("numberOfPages"))
            if p_cnt > 0:
                return p_cnt * 5

    # Check reviews in reviews_data
    if reviews_data and isinstance(reviews_data, dict):
        revs = reviews_data.get("reviews") or []
        if isinstance(revs, list) and revs:
            return len(revs)

    return 0


def build_delivery_document(
    detail_data: Dict[str, Any],
    reviews_data: Optional[Dict[str, Any]] = None,
    restaurant_id: Optional[int] = None,
    request_id: Optional[str] = None,
    city: str = "",
    location: str = "",
    location_key: Optional[str] = None,
    selected_fields: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Builds a normalized document for 'delivery_data' MongoDB collection,
    incorporating all available fields from restaurant details and reviews.
    """
    if not isinstance(detail_data, dict):
        return None

    # Unwrap if raw_data is stored as a sub-key
    raw_source = detail_data.get("raw_data") if isinstance(detail_data.get("raw_data"), dict) else detail_data
    page_info = raw_source.get("page_info", {}) if isinstance(raw_source.get("page_info"), dict) else {}
    page_data = raw_source.get("page_data", {}) if isinstance(raw_source.get("page_data"), dict) else raw_source
    sections = page_data.get("sections", {}) if isinstance(page_data.get("sections"), dict) else {}
    basic_info = sections.get("SECTION_BASIC_INFO", {}) if isinstance(sections.get("SECTION_BASIC_INFO"), dict) else {}
    contact = sections.get("SECTION_RES_CONTACT", {}) if isinstance(sections.get("SECTION_RES_CONTACT"), dict) else {}
    res_details = sections.get("SECTION_RES_DETAILS", {}) if isinstance(sections.get("SECTION_RES_DETAILS"), dict) else {}

    # Resolve Restaurant ID
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

    resolved_city = city or detail_data.get("city") or contact.get("city_name") or ""
    resolved_location = location or detail_data.get("location") or contact.get("locality_verbose") or ""

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
    timing = ""
    if isinstance(raw_timing, dict):
        timing = raw_timing.get("timing_desc") or raw_timing.get("customTimingText") or raw_timing.get("timingText") or ""
    elif isinstance(raw_timing, str):
        timing = raw_timing

    raw_rating = detail_data.get("rating") or basic_info.get("rating", {}).get("aggregate_rating") or ""
    rating = str(raw_rating).strip()

    canonical_url = (
        detail_data.get("canonical_url")
        or page_info.get("canonicalUrl")
        or detail_data.get("page_url")
        or ""
    )

    # Ratings
    rating_new = basic_info.get("rating_new", {}).get("ratings", {}) if isinstance(basic_info.get("rating_new"), dict) else {}
    dining_obj = rating_new.get("DINING") or rating_new.get("dining") or {}
    delivery_obj = rating_new.get("DELIVERY") or rating_new.get("delivery") or {}
    basic_rating_obj = basic_info.get("rating", {}) if isinstance(basic_info.get("rating"), dict) else {}

    dining_agg_rating = str(dining_obj.get("rating") or dining_obj.get("ratingV2") or basic_rating_obj.get("aggregate_rating") or "").strip()
    dining_votes = parse_numeric_metric(dining_obj.get("reviewCount") or basic_rating_obj.get("votes"))

    delivery_rating = str(delivery_obj.get("rating") or delivery_obj.get("ratingV2") or "").strip()
    delivery_review_count = parse_numeric_metric(delivery_obj.get("reviewCount"))

    static_map_url = str(contact.get("static_map_url") or "").strip()

    # Rich Fields from Restaurant Data (SECTION_RES_DETAILS & BASIC_INFO)
    # 1. Cuisines
    cuisines_str = basic_info.get("cuisine_string") or ""
    if not cuisines_str and isinstance(res_details, dict):
        cuis_list = res_details.get("CUISINES", {}).get("cuisines", [])
        if isinstance(cuis_list, list):
            cuisines_str = ", ".join(c.get("name") for c in cuis_list if isinstance(c, dict) and c.get("name"))
    if not cuisines_str:
        cuis_list = basic_info.get("cuisine", [])
        if isinstance(cuis_list, list):
            cuisines_str = ", ".join(c.get("name") for c in cuis_list if isinstance(c, dict) and c.get("name"))

    # 2. Average Cost for Two
    cft_details = res_details.get("CFT_DETAILS", {}) if isinstance(res_details, dict) else {}
    cfts = cft_details.get("cfts", []) if isinstance(cft_details, dict) else []
    average_cost_for_two = (
        cfts[0].get("title", "") if cfts and isinstance(cfts[0], dict) else cft_details.get("cost_text_min_info", "")
    )
    if not average_cost_for_two:
        average_cost_for_two = (
            basic_info.get("costText", {}).get("text")
            or basic_info.get("cft", {}).get("text")
            or cft_details.get("cft", "")
            or ""
        )

    # 3. Popular Dishes
    top_dishes_obj = res_details.get("TOP_DISHES", {}) if isinstance(res_details, dict) else {}
    popular_dishes = top_dishes_obj.get("description", "") if isinstance(top_dishes_obj, dict) else ""

    # 4. Known For
    known_for_obj = res_details.get("KNOWN_FOR", {}) if isinstance(res_details, dict) else {}
    known_for = known_for_obj.get("knownFor", "") if isinstance(known_for_obj, dict) else ""

    # 5. People Liked
    people_liked_obj = res_details.get("PEOPLE_LIKED", {}) if isinstance(res_details, dict) else {}
    people_liked = people_liked_obj.get("description", "") if isinstance(people_liked_obj, dict) else ""

    # 6. Highlights / Amenities
    highlights_obj = res_details.get("HIGHLIGHTS", {}) if isinstance(res_details, dict) else {}
    hl_list = highlights_obj.get("highlights", []) if isinstance(highlights_obj, dict) else []
    highlights = ", ".join(h.get("text") for h in hl_list if isinstance(h, dict) and h.get("text"))

    # 7. Coordinates & Location Details
    latitude = contact.get("latitude")
    longitude = contact.get("longitude")
    zipcode = str(contact.get("zipcode") or "").strip()

    # 8. Media & Status
    image_url = basic_info.get("res_thumb") or page_info.get("resThumb") or ""
    status_text = basic_info.get("res_status_text") or ""
    is_delivery_only = bool(basic_info.get("is_delivery_only", False))

    # User reviews extraction
    reviews_list: List[Dict[str, Any]] = []
    seen_rev_ids = set()
    raw_reviews = []
    if isinstance(reviews_data, dict):
        raw_reviews = reviews_data.get("reviews", [])
    elif isinstance(reviews_data, list):
        raw_reviews = reviews_data

    for rev in raw_reviews:
        if not isinstance(rev, dict):
            continue

        # Handle case where rev is a raw page payload with entities
        if "entities" in rev and isinstance(rev.get("entities"), dict):
            ent = rev.get("entities", {})
            p_ratings = ent.get("RATING", {})
            p_revs = ent.get("REVIEWS", {})
            rev_items = list(p_revs.values()) if isinstance(p_revs, dict) else (p_revs if isinstance(p_revs, list) else [])
            for r in rev_items:
                if isinstance(r, dict):
                    rid = str(r.get("reviewId") or r.get("id") or "")
                    if rid and rid in seen_rev_ids:
                        continue
                    if rid:
                        seen_rev_ids.add(rid)

                    r_rating = r.get("ratingV2") or r.get("rating")
                    if isinstance(r_rating, dict):
                        for sub_ent in r_rating.get("entities", []):
                            for eid in sub_ent.get("entity_ids", []):
                                r_obj = p_ratings.get(str(eid)) or p_ratings.get(eid)
                                if isinstance(r_obj, dict) and "rating" in r_obj:
                                    r_rating = r_obj.get("rating")
                                    break
                    reviews_list.append({
                        "review_id": rid or f"rev_{len(reviews_list) + 1}",
                        "user_name": str(r.get("userName") or (r.get("user", {}).get("name") if isinstance(r.get("user"), dict) else "Anonymous")),
                        "rating": str(r_rating or "").strip(),
                        "review_text": str(r.get("reviewText") or "").strip(),
                        "like_count": int(r.get("likeCount", 0) or 0),
                        "timestamp": r.get("timestamp"),
                    })
            continue

        u_name = rev.get("user_name") or rev.get("userName") or (rev.get("user", {}).get("name") if isinstance(rev.get("user"), dict) else "") or "Anonymous"
        u_rating = str(rev.get("rating") or rev.get("rating_text") or "").strip()
        u_text = str(rev.get("review_text") or rev.get("reviewText") or "").strip()
        raw_likes = rev.get("like_count") if rev.get("like_count") is not None else rev.get("likeCount", 0)
        try:
            u_likes = int(raw_likes)
        except (ValueError, TypeError):
            u_likes = 0

        rid = str(rev.get("review_id") or rev.get("id") or "")
        if rid and rid in seen_rev_ids:
            continue
        if rid:
            seen_rev_ids.add(rid)

        reviews_list.append({
            "review_id": rid or f"rev_{len(reviews_list) + 1}",
            "user_name": str(u_name),
            "rating": u_rating,
            "review_text": u_text,
            "like_count": u_likes,
            "timestamp": rev.get("timestamp"),
        })

    # Count of reviews which we scraped
    total_reviews = len(reviews_list)

    all_extracted = {
        "_id": res_id,
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
        "cuisines": cuisines_str,
        "average_cost_for_two": average_cost_for_two,
        "popular_dishes": popular_dishes,
        "known_for": known_for,
        "people_liked": people_liked,
        "highlights": highlights,
        "static_map_url": static_map_url,
        "latitude": latitude,
        "longitude": longitude,
        "zipcode": zipcode,
        "image_url": image_url,
        "status_text": status_text,
        "is_delivery_only": is_delivery_only,
        "reviews": reviews_list,
        "request_id": request_id or "",
        "location_key": location_key or "",
        "parsed_at": datetime.now(timezone.utc).isoformat(),
    }

    # Filter to only the selected fields requested by user (preserving essential query keys)
    if selected_fields:
        doc: Dict[str, Any] = {
            "_id": res_id,
            "restaurant_id": res_id,
            "city": resolved_city,
            "location": resolved_location,
            "location_key": location_key or all_extracted.get("location_key", ""),
            "request_id": request_id or "",
            "parsed_at": all_extracted["parsed_at"],
        }
        for field in selected_fields:
            if field in all_extracted:
                doc[field] = all_extracted[field]
        return doc

    return all_extracted


def parse_and_sync_simultaneously(
    restaurant_id: int,
    request_id: Optional[str] = None,
    location_key: Optional[str] = None,
    selected_fields: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Simultaneously called when either restaurant details or reviews are updated.
    Retrieves the record from PostgreSQL model RestaurantRecord, builds the normalized
    delivery document with 'total_reviews', and updates the model.
    """
    try:
        res_id = int(restaurant_id)
    except (ValueError, TypeError):
        return None

    from scraper_app.models import RestaurantRecord

    rec = RestaurantRecord.objects.filter(restaurant_id=res_id).first()
    if not rec:
        return None

    delivery_doc = build_delivery_document(
        detail_data=rec.raw_data.get("details", {}) if rec.raw_data else {},
        reviews_data={"reviews": rec.reviews},
        restaurant_id=res_id,
        request_id=request_id or (str(rec.batch_id) if rec.batch_id else ""),
        city=rec.city,
        location=rec.location,
        location_key=location_key or rec.location_key,
        selected_fields=selected_fields,
    )

    if delivery_doc:
        rec.delivery_document = delivery_doc
        rec.save(update_fields=["delivery_document", "updated_at"])
        logger.info(
            "Simultaneously parsed & updated RestaurantRecord for ID %s (fields: %d, total_reviews=%s)",
            res_id,
            len(delivery_doc.keys()),
            delivery_doc.get("total_reviews", "N/A"),
        )
        return delivery_doc

    return None


def reparse_delivery_data_for_request(
    request_id: str,
    location_key: Optional[str] = None,
    city: str = "",
    location: Optional[str] = None,
    selected_fields: Optional[List[str]] = None,
) -> int:
    """
    Parses and syncs delivery documents in RestaurantRecord directly via Django models.
    """
    from scraper_app.models import RestaurantRecord

    qs = RestaurantRecord.objects.all()
    if location_key:
        qs = qs.filter(location_key=location_key)
    elif request_id:
        qs = qs.filter(batch__request_id=str(request_id))
    elif city:
        qs = qs.filter(city__iexact=city)

    reparsed_count = 0
    for rec in qs:
        d_doc = build_delivery_document(
            detail_data=rec.raw_data.get("details", {}) if rec.raw_data else {},
            reviews_data={"reviews": rec.reviews},
            restaurant_id=rec.restaurant_id,
            request_id=request_id,
            city=rec.city,
            location=rec.location,
            location_key=location_key or rec.location_key,
            selected_fields=selected_fields,
        )
        if d_doc:
            rec.delivery_document = d_doc
            rec.save(update_fields=["delivery_document", "updated_at"])
            reparsed_count += 1

    logger.info("Reparsed %d documents in RestaurantRecord for request %s", reparsed_count, request_id)
    return reparsed_count
