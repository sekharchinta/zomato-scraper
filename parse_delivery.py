#!/usr/bin/env python3
"""
Delivery Data Parser & MongoDB Synchronization Utility.

Parses data extracted from restaurant hotel details (restaurants_data / output_details)
and customer reviews (reviews / output_reviews), and stores the normalized records
into MongoDB collection 'delivery_data' and 'output_delivery/' JSON files.

Fields extracted:
  - restaurant_id (and primary key _id)
  - name
  - city
  - location
  - address
  - phone_numbers
  - timing
  - rating
  - canonical_url (and cannonical_url)
  - rating_dining: aggregate_rating, votes
  - rating_delivery: rating, reviewcount
  - static_map_url (and staticmapurl)
  - reviews: list of {user_name, rating, review_text, like_count} for all users
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from config import Config
from scraper import (
    MongoDBManager,
    build_delivery_data_document,
    sync_delivery_folder_to_mongo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("DeliveryParser")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse Zomato Hotel Details and Reviews into 'delivery_data' collection and output_delivery/"
    )
    parser.add_argument(
        "--city",
        type=str,
        help="Optional city filter to parse (e.g. 'Hyderabad', 'Palakollu', 'Bengaluru')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on number of restaurants to parse",
    )
    parser.add_argument(
        "--preview",
        "-p",
        action="store_true",
        help="Print a preview of the first parsed restaurant document",
    )
    parser.add_argument(
        "--no-mongo",
        action="store_true",
        help="Disable MongoDB operations (save JSON to output_delivery/ only)",
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        help="Custom MongoDB connection URI",
    )

    args = parser.parse_args()
    config = Config()

    if args.no_mongo:
        config.enable_mongo = False
    if args.mongo_uri:
        config.mongo_uri = args.mongo_uri

    mongo_manager = None
    if config.enable_mongo:
        mongo_manager = MongoDBManager(
            uri=config.mongo_uri,
            db_name=config.mongo_db,
            collection_name=config.mongo_collection,
            details_collection_name=config.mongo_details_collection,
            reviews_collection_name=config.mongo_reviews_collection,
            delivery_collection_name=config.mongo_delivery_collection,
        )
        if not mongo_manager.connect():
            logger.warning("MongoDB connection failed. Continuing in disk-only mode.")
            mongo_manager = None

    print("\n" + "=" * 80)
    print("             ZOMATO DELIVERY DATA PARSER")
    print("=" * 80)
    print(f"Details Source:     {config.details_dir}/ & MongoDB '{config.mongo_details_collection}'")
    print(f"Reviews Source:     {config.reviews_dir}/ & MongoDB '{config.mongo_reviews_collection}'")
    print(f"Target Output:      {config.delivery_dir}/")
    if mongo_manager and mongo_manager.is_connected():
        print(f"MongoDB Target:     Database '{config.mongo_db}', Collection '{mongo_manager.delivery_collection_name}'")
    else:
        print("MongoDB Target:     Disabled (File storage only)")
    if args.city:
        print(f"City Filter:        {args.city}")
    if args.limit:
        print(f"Record Limit:       {args.limit}")
    print("=" * 80 + "\n")

    res = sync_delivery_folder_to_mongo(
        details_dir=config.details_dir,
        reviews_dir=config.reviews_dir,
        delivery_dir=config.delivery_dir,
        mongo_manager=mongo_manager,
    )

    print("\n" + "=" * 80)
    print("             DELIVERY DATA PARSING SUMMARY")
    print("=" * 80)
    print(f"Total Processed:    {res.get('processed', 0)}")
    print(f"Newly Upserted:     {res.get('upserted', 0)}")
    print(f"Updated/Modified:   {res.get('modified', 0)}")
    if mongo_manager and mongo_manager.is_connected():
        print(f"Total in DB:        {res.get('total_in_db', 0):,} records in '{mongo_manager.delivery_collection_name}'")
    print(f"Output Directory:   {config.delivery_dir}/")
    print("=" * 80 + "\n")

    # Optional preview of parsed document
    if args.preview or True:
        sample_file = next(config.delivery_dir.glob("*/*.json"), None)
        if sample_file and sample_file.exists():
            try:
                with sample_file.open("r", encoding="utf-8") as sf:
                    sample_doc = json.load(sf)
                print("=" * 80)
                print(f"SAMPLE PARSED RECORD PREVIEW: {sample_doc.get('name')} (ID: {sample_doc.get('restaurant_id')})")
                print("=" * 80)
                print(f"• Restaurant ID:     {sample_doc.get('restaurant_id')}")
                print(f"• Name:              {sample_doc.get('name')}")
                print(f"• City / Location:   {sample_doc.get('city')} / {sample_doc.get('location')}")
                print(f"• Address:           {sample_doc.get('address')}")
                print(f"• Phone Numbers:     {sample_doc.get('phone_numbers')}")
                print(f"• Timing:            {sample_doc.get('timing')}")
                print(f"• General Rating:    {sample_doc.get('rating')}")
                print(f"• Canonical URL:     {sample_doc.get('canonical_url')}")
                print(f"• Dining Rating:     {sample_doc.get('aggregate_rating')} (Votes: {sample_doc.get('votes')})")
                print(f"• Delivery Rating:   {sample_doc.get('delivery_rating')} (ReviewCount: {sample_doc.get('reviewcount')})")
                print(f"• Static Map URL:    {sample_doc.get('static_map_url')}")
                print(f"• User Reviews:      {len(sample_doc.get('reviews', []))} review(s) extracted")
                if sample_doc.get("reviews"):
                    first_rev = sample_doc["reviews"][0]
                    print(f"    - First Reviewer: '{first_rev.get('user_name')}' | Rating: {first_rev.get('rating')} | Likes: {first_rev.get('like_count')}")
                    print(f"      Text: {first_rev.get('review_text')[:100]}...")
                print("=" * 80 + "\n")
            except Exception as exc:
                logger.warning("Could not display preview: %s", exc)


if __name__ == "__main__":
    main()
