"""
Unit tests for Zomato Scraper modules with keyword auto-discovery and location folders.
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from config import Config
from scraper import (
    MongoDBManager,
    ZomatoScraper,
    build_delivery_data_document,
    extract_latlums_from_suggestion,
    extract_restaurant_page_url,
    find_nearest_suggestion,
    format_restaurant_detail_document,
    format_restaurant_document,
    format_restaurant_reviews_document,
    get_location_city_folder,
    parse_restaurant_data,
    print_location_suggestions,
    slugify,
    sync_delivery_folder_to_mongo,
    sync_details_folder_to_mongo,
    sync_output_folder_to_mongo,
    sync_reviews_folder_to_mongo,
)


class FolderNamingTests(unittest.TestCase):
    def test_slugify(self):
        self.assertEqual(slugify("Prashanth Nagar"), "prashanth_nagar")
        self.assertEqual(slugify("Table Space UB City, 8, 9!"), "table_space_ub_city_8_9")
        self.assertEqual(slugify("Majestic"), "majestic")

    def test_get_location_city_folder(self):
        self.assertEqual(
            get_location_city_folder("prashanth nagar", "Hyderabad"),
            "prashanth_nagar_hyderabad",
        )
        self.assertEqual(
            get_location_city_folder("Majestic", "Bengaluru"),
            "majestic_bengaluru",
        )
        self.assertEqual(
            get_location_city_folder("Indiranagar", "Bengaluru"),
            "indiranagar_bengaluru",
        )


class SuggestionMatchingTests(unittest.TestCase):
    def test_extract_latlums_from_entity_fields(self):
        s = {"entity_latitude": 17.385, "entity_longitude": 78.486}
        lat, lon = extract_latlums_from_suggestion(s)
        self.assertEqual(lat, 17.385)
        self.assertEqual(lon, 78.486)

    def test_extract_latlums_from_nested_place(self):
        s = {
            "entity_latitude": 0,
            "entity_longitude": 0,
            "place": {"latitude": 17.406, "longitude": 78.477},
        }
        lat, lon = extract_latlums_from_suggestion(s)
        self.assertEqual(lat, 17.406)
        self.assertEqual(lon, 78.477)

    def test_find_nearest_suggestion_for_hydera(self):
        suggestions = [
            {
                "entity_title": "Hyderabad Gate",
                "entity_name": "Hyderabad Gate, Varanasi, Uttar Pradesh",
                "entity_type": "point_of_interest",
                "entity_latitude": 25.26,
                "entity_longitude": 82.98,
            },
            {
                "entity_title": "Hyderabad",
                "entity_name": "Hyderabad, Telangana, India",
                "entity_type": "city",
                "location_type": "geocode",
                "entity_latitude": 17.406,
                "entity_longitude": 78.477,
            },
            {
                "entity_title": "Hyderabad School of Business",
                "entity_name": "Hyderabad School of Business, Banjara Hills",
                "entity_type": "point_of_interest",
                "entity_latitude": 17.411,
                "entity_longitude": 78.437,
            },
        ]

        nearest = find_nearest_suggestion("hydera", suggestions)
        self.assertEqual(nearest["entity_title"], "Hyderabad")
        self.assertEqual(nearest["entity_name"], "Hyderabad, Telangana, India")
        lat, lon = extract_latlums_from_suggestion(nearest)
        self.assertEqual(lat, 17.406)
        self.assertEqual(lon, 78.477)


class ConfigTests(unittest.TestCase):
    def test_default_config_values(self):
        config = Config()
        self.assertEqual(config.page_increment, 1)
        self.assertEqual(config.offset_increment, 12)
        self.assertTrue(config.city)

    def test_config_from_custom_env(self):
        with tempfile.NamedTemporaryFile("w+", delete=False) as tf:
            tf.write("CITY=Bengaluru\nMAX_PAGES=2\n")
            tf_path = Path(tf.name)

        try:
            cfg = Config(env_path=tf_path)
            self.assertEqual(cfg.city, "Bengaluru")
            self.assertEqual(cfg.max_pages, 2)
        finally:
            tf_path.unlink()


class LocationAndPayloadTests(unittest.TestCase):
    def setUp(self):
        cfg = Config()
        cfg.enable_mongo = False
        self.scraper = ZomatoScraper(cfg)

    def tearDown(self):
        self.scraper.close()

    @patch("requests.Session.get")
    def test_search_latlums_by_keyword(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "locationSuggestions": [
                {
                    "entity_title": "Hyderabad",
                    "entity_name": "Hyderabad, Telangana, India",
                    "entity_type": "city",
                    "entity_latitude": 17.406498,
                    "entity_longitude": 78.4772439,
                }
            ]
        }
        mock_get.return_value = mock_resp

        lat, lon, suggestion = self.scraper.search_latlums_by_keyword("hydera")
        self.assertEqual(lat, 17.406498)
        self.assertEqual(lon, 78.4772439)
        self.assertEqual(suggestion["entity_title"], "Hyderabad")

    def test_build_restaurants_payload_increments(self):
        loc = {
            "entity_id": "7006",
            "entity_type": "subzone",
            "city_id": "6",
            "latitude": 17.406,
            "longitude": 78.477,
            "user_defined_latitude": 17.406,
            "user_defined_longitude": 78.477,
            "city_name": "Hyderabad",
            "country_id": "1",
            "country_name": "India",
            "place_id": "846",
            "delivery_subzone_id": 846,
            "place_type": "DSZ",
            "cell_id": "4300399000479072256",
        }

        # Page 1, Offset 0
        p1 = self.scraper.build_restaurants_payload(loc, page=1, offset=0)
        self.assertEqual(p1["page"], 1)
        self.assertEqual(p1["solr_offset"], 0)
        self.assertEqual(p1["total_restaurants_shown"], 0)

        # Page 2, Offset 12 (offset increases by 12, page increases by 1)
        p2 = self.scraper.build_restaurants_payload(loc, page=2, offset=12)
        self.assertEqual(p2["page"], 2)
        self.assertEqual(p2["solr_offset"], 12)
        self.assertEqual(p2["total_restaurants_shown"], 12)

    @patch("requests.Session.get")
    def test_get_location_suggestions(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "locationSuggestions": [
                {
                    "entity_title": "Bengaluru",
                    "entity_name": "Bengaluru, India",
                    "entity_type": "city",
                    "entity_latitude": 12.971606,
                    "entity_longitude": 77.594376,
                },
                {
                    "entity_title": "SMVT Bengaluru",
                    "entity_name": "SMVT Bengaluru Railway Station",
                    "entity_type": "point_of_interest",
                    "entity_latitude": 13.000274,
                    "entity_longitude": 77.641070,
                },
            ]
        }
        mock_get.return_value = mock_resp

        suggestions = self.scraper.get_location_suggestions("Bengaluru")
        self.assertEqual(len(suggestions), 2)
        self.assertEqual(suggestions[0]["entity_title"], "Bengaluru")
        self.assertEqual(suggestions[0]["latitude"], 12.971606)
        self.assertEqual(suggestions[1]["entity_title"], "SMVT Bengaluru")
        self.assertEqual(suggestions[1]["longitude"], 77.641070)

    def test_print_location_suggestions_output(self):
        suggestions = [
            {
                "entity_title": "Bengaluru",
                "entity_name": "Bengaluru, India",
                "latitude": 12.971606,
                "longitude": 77.594376,
            }
        ]
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            print_location_suggestions(suggestions, city_input="Bengaluru")
        out = buf.getvalue()
        self.assertIn("Location: Bengaluru", out)
        self.assertIn('python3 scraper.py --location "Bengaluru" --city "Bengaluru" --lat 12.971606 --lon 77.594376', out)
        # Option at the end to scrape all locations sequentially
        self.assertIn("[2] ALL LOCATIONS: Extract & Scrape All 1 Locations Sequentially", out)
        self.assertIn('python3 scraper.py --city "Bengaluru" --all', out)

    def test_scrape_all_locations_batch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.scraper.config.output_dir = Path(tmpdir)
            suggestions = [
                {
                    "entity_title": "Majestic",
                    "entity_name": "Majestic, Bengaluru",
                    "latitude": 12.9766,
                    "longitude": 77.5713,
                },
                {
                    "entity_title": "Indiranagar",
                    "entity_name": "Indiranagar, Bengaluru",
                    "latitude": 12.9784,
                    "longitude": 77.6408,
                },
            ]

            dummy_folder = Path(tmpdir) / "majestic_bengaluru"
            dummy_folder.mkdir(parents=True, exist_ok=True)
            dummy_file = dummy_folder / "0_1.json"
            dummy_file.write_text(json.dumps({"sections": {"SECTION_SEARCH_RESULT": []}}), encoding="utf-8")

            with patch.object(self.scraper, "scrape", return_value=(dummy_folder, [dummy_file])) as mock_scrape, \
                 patch.object(self.scraper, "scrape_all_restaurant_details", return_value=[]) as mock_details:
                res = self.scraper.scrape_all_locations(
                    city="Bengaluru",
                    suggestions=suggestions,
                    max_pages=2,
                    scrape_details=True,
                )

                self.assertEqual(res["status"], "success")
                self.assertEqual(res["locations_total"], 2)
                self.assertEqual(res["locations_completed"], 2)
                self.assertEqual(mock_scrape.call_count, 2)

                # Check that locations JSON file was created with latlums
                loc_file = Path(res["locations_file"])
                self.assertTrue(loc_file.exists())
                with loc_file.open("r", encoding="utf-8") as f:
                    saved_locs = json.load(f)
                self.assertEqual(len(saved_locs), 2)
                self.assertEqual(saved_locs[0]["entity_title"], "Majestic")
                self.assertEqual(saved_locs[1]["entity_title"], "Indiranagar")

    @patch("sys.argv", ["scraper.py", "--city", "Bengaluru", "--all", "--yes", "--no-mongo"])
    @patch("scraper.ZomatoScraper.get_location_suggestions")
    @patch("scraper.ZomatoScraper.scrape_all_locations")
    def test_main_cli_all_locations_flag(self, mock_scrape_all, mock_get_sugg):
        from scraper import main
        mock_get_sugg.return_value = [
            {"entity_title": "Koramangala", "latitude": 12.9352, "longitude": 77.6245},
            {"entity_title": "Whitefield", "latitude": 12.9698, "longitude": 77.7499},
        ]
        mock_scrape_all.return_value = {"status": "success"}

        main()
        mock_get_sugg.assert_called_once_with("Bengaluru")
        mock_scrape_all.assert_called_once()
        call_kwargs = mock_scrape_all.call_args[1]
        self.assertEqual(call_kwargs["city"], "Bengaluru")
        self.assertEqual(len(call_kwargs["suggestions"]), 2)
        self.assertTrue(call_kwargs["scrape_details"])

    @patch("sys.argv", ["scraper.py", "--city", "Bengaluru", "--select", "all", "--no-details", "--no-mongo"])
    @patch("scraper.ZomatoScraper.get_location_suggestions")
    @patch("scraper.ZomatoScraper.scrape_all_locations")
    def test_main_cli_select_all(self, mock_scrape_all, mock_get_sugg):
        from scraper import main
        mock_get_sugg.return_value = [
            {"entity_title": "HSR Layout", "latitude": 12.9121, "longitude": 77.6446},
        ]
        mock_scrape_all.return_value = {"status": "success"}

        main()
        mock_get_sugg.assert_called_once_with("Bengaluru")
        mock_scrape_all.assert_called_once()
        call_kwargs = mock_scrape_all.call_args[1]
        self.assertEqual(call_kwargs["city"], "Bengaluru")
        self.assertFalse(call_kwargs["scrape_details"])


class ScraperAndParserTests(unittest.TestCase):
    def test_parse_restaurant_data(self):
        sample_response = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {
                        "type": "restaurant",
                        "info": {
                            "resId": 2001,
                            "name": "Masala Republic By Dadu's",
                            "rating": {
                                "aggregate_rating": "4.4",
                                "rating_text": "Very Good",
                            },
                            "cft": {"text": "₹1,000 for two"},
                            "cuisine": [{"name": "Asian"}, {"name": "North Indian"}],
                            "locality": {"name": "Himayatnagar"},
                            "o2Url": "/hyderabad/masala-republic-by-dadus-himayath-nagar",
                        },
                    }
                ]
            }
        }

        parsed = parse_restaurant_data(sample_response)
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["restaurant_id"], 2001)
        self.assertEqual(parsed[0]["name"], "Masala Republic By Dadu's")
        self.assertEqual(parsed[0]["rating"], "4.4")
        self.assertEqual(parsed[0]["cuisines"], ["Asian", "North Indian"])
        self.assertEqual(parsed[0]["locality"], "Himayatnagar")
        self.assertEqual(
            parsed[0]["url"],
            "https://www.zomato.com/hyderabad/masala-republic-by-dadus-himayath-nagar",
        )

    @patch("scraper.ZomatoScraper.resolve_location")
    @patch("scraper.ZomatoScraper.ensure_csrf_token")
    @patch("requests.Session.post")
    def test_stops_when_section_search_result_is_empty_even_if_hasmore_is_true(
        self, mock_post, mock_csrf, mock_resolve
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = Config()
            cfg.output_dir = Path(tmp_dir)
            cfg.request_delay = 0
            cfg.enable_mongo = False
            scraper = ZomatoScraper(cfg)

            mock_resolve.return_value = {
                "entity_id": "7006",
                "entity_type": "subzone",
                "city_id": "6",
                "city_name": "Hyderabad",
                "location_name": "Nampally",
                "folder_name": "nampally_hyderabad",
                "latitude": 17.394,
                "longitude": 78.465,
                "user_defined_latitude": 17.394,
                "user_defined_longitude": 78.465,
                "country_id": "1",
                "country_name": "India",
                "place_id": "846",
                "cell_id": "0",
                "delivery_subzone_id": 846,
                "place_type": "DSZ",
                "exact_location_url": "https://www.zomato.com/hyderabad/restaurants",
            }
            mock_csrf.return_value = "dummy_csrf_token"

            # Page 1: has restaurants
            resp1 = MagicMock()
            resp1.status_code = 200
            resp1.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [{"type": "restaurant", "info": {"resId": 101, "name": "Rest 1"}}],
                    "SECTION_SEARCH_META_INFO": {"searchMetaData": {"hasMore": True, "totalResults": 500}},
                }
            }

            # Page 2: empty SECTION_SEARCH_RESULT: [], but hasMore is STILL True
            resp2 = MagicMock()
            resp2.status_code = 200
            resp2.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [],
                    "SECTION_SEARCH_META_INFO": {"searchMetaData": {"hasMore": True, "totalResults": 500}},
                }
            }

            mock_post.side_effect = [resp1, resp2]

            folder, files = scraper.scrape(city="Hyderabad", max_pages=10)

            # Must stop at page 2 where SECTION_SEARCH_RESULT is []
            self.assertEqual(len(files), 2)
            self.assertEqual(mock_post.call_count, 2)
            self.assertTrue((folder / "0_1.json").exists())
            self.assertTrue((folder / "12_2.json").exists())
            scraper.close()

    @patch("scraper.ZomatoScraper.resolve_location")
    @patch("scraper.ZomatoScraper.ensure_csrf_token")
    @patch("requests.Session.post")
    def test_stops_on_exact_zomato_501_cap_payload(
        self, mock_post, mock_csrf, mock_resolve
    ):
        """
        Tests pagination termination on the exact terminal response payload returned by Zomato
        when reaching 501 results cap: hasMore=True, totalResults=12741, SECTION_SEARCH_RESULT=[], entities=[].
        Ensures pagination stops immediately and does not query subsequent pages.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = Config()
            cfg.output_dir = Path(tmp_dir)
            cfg.request_delay = 0
            cfg.enable_mongo = False
            scraper = ZomatoScraper(cfg)

            mock_resolve.return_value = {
                "entity_id": "7006",
                "entity_type": "subzone",
                "city_id": "6",
                "city_name": "Hyderabad",
                "location_name": "Nampally",
                "folder_name": "nampally_hyderabad",
                "latitude": 17.394,
                "longitude": 78.465,
                "user_defined_latitude": 17.394,
                "user_defined_longitude": 78.465,
                "country_id": "1",
                "country_name": "India",
                "place_id": "846",
                "cell_id": "0",
                "delivery_subzone_id": 846,
                "place_type": "DSZ",
                "exact_location_url": "https://www.zomato.com/hyderabad/restaurants",
            }
            mock_csrf.return_value = "dummy_csrf_token"

            # Page 1: normal restaurant response
            resp1 = MagicMock()
            resp1.status_code = 200
            resp1.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [{"type": "restaurant", "info": {"resId": 999, "name": "Biryani House"}}],
                    "SECTION_SEARCH_META_INFO": {"searchMetaData": {"hasMore": True, "totalResults": 12741}},
                }
            }

            # Page 2 (e.g. solr_offset=585, page=49): exact payload from user request
            resp2 = MagicMock()
            resp2.status_code = 200
            resp2.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [],
                    "SECTION_SEARCH_META_INFO": {
                        "searchMetaData": {
                            "previousSearchParams": '{"PreviousSearchId":"73225126-c7b6-4e01-8348-407d9312b2b8","PreviousSearchFilter":["{\\"category_context\\":\\"go_out_home\\"}","","{\\"context\\":\\"dineout_home\\"}"]}',
                            "postbackParams": '{"search_id":"73225126-c7b6-4e01-8348-407d9312b2b8","solr_offset":585,"page":49,"total_restaurants_shown":501,"total_results_shown":501,"vg_set":true}',
                            "totalResults": 12741,
                            "hasMore": True,
                            "getInactive": False,
                        }
                    },
                },
                "entities": [],
            }

            # If pagination didn't stop, it would attempt to call resp3
            resp3 = MagicMock()
            resp3.status_code = 200
            resp3.json.return_value = {"sections": {"SECTION_SEARCH_RESULT": []}}

            mock_post.side_effect = [resp1, resp2, resp3]

            folder, files = scraper.scrape(city="Hyderabad", max_pages=100)

            # Assert that pagination made EXACTLY 2 POST calls and stopped immediately at resp2
            self.assertEqual(mock_post.call_count, 2)
            self.assertEqual(len(files), 2)
            self.assertTrue((folder / "0_1.json").exists())
            self.assertTrue((folder / "12_2.json").exists())
            scraper.close()

    @patch("scraper.ZomatoScraper.resolve_location")
    @patch("scraper.ZomatoScraper.ensure_csrf_token")
    @patch("requests.Session.post")
    def test_stops_when_results_start_repeating(
        self, mock_post, mock_csrf, mock_resolve
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            cfg = Config()
            cfg.output_dir = Path(tmp_dir)
            cfg.request_delay = 0
            cfg.enable_mongo = False
            scraper = ZomatoScraper(cfg)

            mock_resolve.return_value = {
                "entity_id": "7006",
                "entity_type": "subzone",
                "city_id": "6",
                "city_name": "Hyderabad",
                "location_name": "Nampally",
                "folder_name": "nampally_hyderabad",
                "latitude": 17.394,
                "longitude": 78.465,
                "user_defined_latitude": 17.394,
                "user_defined_longitude": 78.465,
                "country_id": "1",
                "country_name": "India",
                "place_id": "846",
                "cell_id": "0",
                "delivery_subzone_id": 846,
                "place_type": "DSZ",
                "exact_location_url": "https://www.zomato.com/hyderabad/restaurants",
            }
            mock_csrf.return_value = "dummy_csrf_token"

            # Page 1: returns restaurant 101, 102
            resp1 = MagicMock()
            resp1.status_code = 200
            resp1.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [
                        {"type": "restaurant", "info": {"resId": 101, "name": "Rest 1"}},
                        {"type": "restaurant", "info": {"resId": 102, "name": "Rest 2"}},
                    ],
                }
            }

            # Page 2: Solr wraps around and returns the exact same 101, 102 (0 new unique)
            resp2 = MagicMock()
            resp2.status_code = 200
            resp2.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [
                        {"type": "restaurant", "info": {"resId": 101, "name": "Rest 1"}},
                        {"type": "restaurant", "info": {"resId": 102, "name": "Rest 2"}},
                    ],
                }
            }

            mock_post.side_effect = [resp1, resp2]

            folder, files = scraper.scrape(city="Hyderabad", max_pages=10)

            # Must stop at page 2, keep only page 1, and not loop indefinitely
            self.assertEqual(len(files), 1)
            self.assertEqual(mock_post.call_count, 2)
            self.assertTrue((folder / "0_1.json").exists())
            self.assertFalse((folder / "12_2.json").exists())
            scraper.close()


class MongoDBIntegrationTests(unittest.TestCase):
    def test_format_restaurant_document(self):
        item = {
            "type": "restaurant",
            "info": {
                "resId": 12345,
                "name": "Paradise Biryani",
                "rating": {"aggregate_rating": "4.3", "rating_text": "Very Good"},
                "cft": {"text": "₹800 for two"},
                "cuisine": [{"name": "Biryani"}, {"name": "North Indian"}],
                "locality": {"name": "Secunderabad"},
                "o2Url": "/hyderabad/paradise-biryani-secunderabad",
            },
        }
        doc = format_restaurant_document(item, city="Hyderabad", location="Secunderabad")
        self.assertIsNotNone(doc)
        self.assertEqual(doc["_id"], 12345)
        self.assertEqual(doc["restaurant_id"], 12345)
        self.assertEqual(doc["name"], "Paradise Biryani")
        self.assertEqual(doc["city"], "Hyderabad")
        self.assertEqual(doc["location"], "Secunderabad")
        self.assertEqual(doc["locality"], "Secunderabad")
        self.assertEqual(doc["rating"], "4.3")
        self.assertEqual(doc["cost_for_two"], "₹800 for two")
        self.assertEqual(doc["cuisines"], ["Biryani", "North Indian"])
        self.assertEqual(doc["url"], "https://www.zomato.com/hyderabad/paradise-biryani-secunderabad")
        self.assertIn("raw_data", doc)

    def test_format_restaurant_document_invalid(self):
        # Missing resId
        item_no_id = {"info": {"name": "No ID Restaurant"}}
        self.assertIsNone(format_restaurant_document(item_no_id))

        # Non-dict
        self.assertIsNone(format_restaurant_document("invalid"))

    def test_mongo_uniqueness_and_upsert(self):
        # Tests that MongoDB strictly enforces unique primary key (_id) and updates rather than duplicates
        mgr = MongoDBManager(db_name="zomato_test", collection_name="restaurants_test")
        connected = mgr.connect()
        if not connected:
            self.skipTest("MongoDB server not reachable on localhost:27017")

        try:
            mgr.collection.delete_many({})

            test_restaurant = {
                "type": "restaurant",
                "info": {
                    "resId": 88888,
                    "name": "Test Unique Biryani",
                    "rating": {"aggregate_rating": "4.0"},
                },
            }

            # First upsert -> 1 inserted
            upserted, modified = mgr.upsert_restaurants([test_restaurant], city="Hyderabad", location="Banjara Hills")
            self.assertEqual(upserted, 1)
            self.assertEqual(modified, 0)
            self.assertEqual(mgr.collection.count_documents({}), 1)

            # Upsert the EXACT same restaurant again with modified rating -> modified=1, inserted=0, total remains 1
            test_restaurant["info"]["rating"]["aggregate_rating"] = "4.5"
            upserted2, modified2 = mgr.upsert_restaurants([test_restaurant], city="Hyderabad", location="Banjara Hills")
            self.assertEqual(upserted2, 0)
            self.assertEqual(modified2, 1)
            # Count must still be 1 (strictly unique, no duplicates!)
            self.assertEqual(mgr.collection.count_documents({}), 1)

            doc = mgr.collection.find_one({"_id": 88888})
            self.assertEqual(doc["_id"], 88888)
            self.assertEqual(doc["rating"], "4.5")
        finally:
            mgr.collection.delete_many({})
            mgr.client.drop_database("zomato_test")
            mgr.close()

    def test_sync_output_folder_to_mongo(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            output_path = Path(tmp_dir)
            loc_folder = output_path / "somajiguda_hyderabad"
            loc_folder.mkdir(parents=True)

            sample_page = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [
                        {
                            "type": "restaurant",
                            "info": {"resId": 77771, "name": "Restaurant A"},
                        },
                        {
                            "type": "restaurant",
                            "info": {"resId": 77772, "name": "Restaurant B"},
                        },
                    ]
                }
            }
            with (loc_folder / "0_1.json").open("w") as fh:
                json.dump(sample_page, fh)

            mgr = MongoDBManager(db_name="zomato_test_sync", collection_name="restaurants_test")
            if not mgr.connect():
                self.skipTest("MongoDB not reachable")

            try:
                result = sync_output_folder_to_mongo(output_path, mgr)
                self.assertEqual(result["status"], "success")
                self.assertEqual(result["files_processed"], 1)
                self.assertEqual(result["items_processed"], 2)
                self.assertEqual(result["upserted"], 2)
                self.assertEqual(mgr.collection.count_documents({}), 2)
            finally:
                mgr.client.drop_database("zomato_test_sync")
                mgr.close()

    def test_extract_restaurant_page_url(self):
        # 1. cardAction.clickUrl
        item1 = {"cardAction": {"clickUrl": "/hyderabad/exotica-banjara-hills/info"}}
        self.assertEqual(extract_restaurant_page_url(item1), "/hyderabad/exotica-banjara-hills/info")

        # 2. full URL with domain
        item2 = {"cardAction": {"clickUrl": "https://www.zomato.com/bangalore/punjab-grill/info"}}
        self.assertEqual(extract_restaurant_page_url(item2), "/bangalore/punjab-grill/info")

        # 3. order.actionInfo.clickUrl
        item3 = {"order": {"actionInfo": {"clickUrl": "/hyderabad/paradise-biryani/info"}}}
        self.assertEqual(extract_restaurant_page_url(item3), "/hyderabad/paradise-biryani/info")

        # 4. info.o2Url
        item4 = {"info": {"o2Url": "/hyderabad/bawarchi-rtc-x-roads"}}
        self.assertEqual(extract_restaurant_page_url(item4), "/hyderabad/bawarchi-rtc-x-roads")

        # 5. MongoDB document with url
        doc = {"url": "https://www.zomato.com/hyderabad/chutneys-banjara-hills/info"}
        self.assertEqual(extract_restaurant_page_url(doc), "/hyderabad/chutneys-banjara-hills/info")

    def test_format_restaurant_detail_document(self):
        detail_payload = {
            "page_info": {
                "resId": 90744,
                "pageTitle": "Exotica, Banjara Hills, Hyderabad | Zomato ",
                "pageUrl": "/hyderabad/exotica-banjara-hills",
                "canonicalUrl": "https://www.zomato.com/hyderabad/exotica-banjara-hills",
            },
            "page_data": {
                "sections": {
                    "SECTION_BASIC_INFO": {
                        "res_id": 90744,
                        "name": "Exotica",
                        "cuisine_string": "North Indian, Biryani, Chinese",
                        "rating": {"aggregate_rating": "4.6"},
                        "timing": {"timing_desc": "12noon – 3:30pm, 7pm – 11pm"},
                    },
                    "SECTION_RES_CONTACT": {
                        "address": "5th Floor, 12th Square Building, Road 12, Banjara Hills, Hyderabad",
                        "city_name": "Hyderabad",
                        "locality_verbose": "Road 12, Banjara Hills, Hyderabad",
                        "phoneDetails": {"title": "Phone Numbers", "phoneStr": "+919550515500, +918121585500"},
                    },
                }
            },
        }

        doc = format_restaurant_detail_document(detail_payload, city="Hyderabad", location="Banjara Hills")
        self.assertIsNotNone(doc)
        self.assertEqual(doc["_id"], 90744)
        self.assertEqual(doc["restaurant_id"], 90744)
        self.assertEqual(doc["name"], "Exotica")
        self.assertEqual(doc["city"], "Hyderabad")
        self.assertEqual(doc["address"], "5th Floor, 12th Square Building, Road 12, Banjara Hills, Hyderabad")
        self.assertEqual(doc["phone_numbers"], "+919550515500, +918121585500")
        self.assertEqual(doc["cuisines"], "North Indian, Biryani, Chinese")
        self.assertEqual(doc["rating"], "4.6")
        self.assertIn("raw_data", doc)

    def test_mongo_detail_uniqueness_and_upsert(self):
        mgr = MongoDBManager(
            db_name="zomato_test_detail",
            collection_name="restaurants_test",
            details_collection_name="restaurants_data_test",
        )
        if not mgr.connect():
            self.skipTest("MongoDB not reachable")

        try:
            mgr.details_collection.delete_many({})

            sample_detail = {
                "page_info": {"resId": 99001, "name": "Detail Test Hotel"},
                "page_data": {
                    "sections": {
                        "SECTION_BASIC_INFO": {"res_id": 99001, "name": "Detail Test Hotel", "cuisine_string": "Mughlai"},
                        "SECTION_RES_CONTACT": {"address": "Test Road 1", "phoneDetails": {"phoneStr": "+911234567890"}},
                    }
                },
            }

            # 1. First upsert -> 1 inserted
            up1, mod1 = mgr.upsert_restaurant_detail(sample_detail, restaurant_id=99001)
            self.assertEqual(up1, 1)
            self.assertEqual(mod1, 0)
            self.assertEqual(mgr.get_details_count(), 1)

            # 2. Update with modified address -> 0 inserted, 1 modified, count stays 1 (strictly unique!)
            sample_detail["page_data"]["sections"]["SECTION_RES_CONTACT"]["address"] = "Updated Test Road 2"
            up2, mod2 = mgr.upsert_restaurant_detail(sample_detail, restaurant_id=99001)
            self.assertEqual(up2, 0)
            self.assertEqual(mod2, 1)
            self.assertEqual(mgr.get_details_count(), 1)

            doc = mgr.details_collection.find_one({"_id": 99001})
            self.assertEqual(doc["_id"], 99001)
            self.assertEqual(doc["address"], "Updated Test Road 2")
        finally:
            mgr.client.drop_database("zomato_test_detail")
            mgr.close()

    def test_scrape_restaurant_detail_and_sync(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = Config()
            cfg.output_dir = tmp_path / "output"
            cfg.details_dir = tmp_path / "output_details"
            cfg.enable_mongo = False
            cfg.request_delay = 0

            scraper = ZomatoScraper(cfg)

            fake_getPage_response = {
                "page_info": {"resId": 55555, "pageTitle": "Spicy Biryani Point, Hyderabad | Zomato"},
                "page_data": {
                    "sections": {
                        "SECTION_BASIC_INFO": {"res_id": 55555, "name": "Spicy Biryani Point", "cuisine_string": "Biryani"},
                        "SECTION_RES_CONTACT": {"address": "Near Charminar", "phoneDetails": {"phoneStr": "040-123456"}},
                    }
                },
            }

            with patch("requests.Session.get") as mock_get:
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = fake_getPage_response
                mock_get.return_value = mock_resp

                res = scraper.scrape_restaurant_detail(
                    page_url="/hyderabad/spicy-biryani-point/info",
                    restaurant_id=55555,
                    city="Hyderabad",
                    location="Charminar",
                )

                self.assertIsNotNone(res)
                saved_file = cfg.details_dir / "charminar_hyderabad" / "55555.json"
                self.assertTrue(saved_file.exists())

                with saved_file.open("r", encoding="utf-8") as fh:
                    saved_data = json.load(fh)
                self.assertEqual(saved_data["page_info"]["resId"], 55555)

            scraper.close()


class ReviewsScrapingTests(unittest.TestCase):
    """Unit tests for restaurant customer review scraping across all pages and MongoDB storage."""

    def test_format_restaurant_reviews_document(self):
        sample_reviews = [
            {
                "review_id": 1001,
                "user_name": "Foodie Person",
                "user_id": 501,
                "rating": "4.5",
                "review_text": "Great ambiance and biryani!",
                "timestamp": "2 days ago",
            },
            {
                "review_id": 1002,
                "user_name": "Curry Lover",
                "user_id": 502,
                "rating": "5.0",
                "review_text": "Authentic taste.",
                "timestamp": "1 week ago",
            },
        ]
        doc = format_restaurant_reviews_document(
            restaurant_id=19698425,
            name="Shah Ghouse",
            city="Hyderabad",
            location="Lakdikapul",
            page_url="/hyderabad/shah-ghouse-lakdikapul/reviews",
            number_of_pages=15,
            pages_scraped=2,
            reviews=sample_reviews,
        )
        self.assertEqual(doc["_id"], 19698425)
        self.assertEqual(doc["restaurant_id"], 19698425)
        self.assertEqual(doc["name"], "Shah Ghouse")
        self.assertEqual(doc["city"], "Hyderabad")
        self.assertEqual(doc["total_reviews"], 2)
        self.assertEqual(doc["number_of_pages"], 15)
        self.assertEqual(doc["pages_scraped"], 2)
        self.assertEqual(len(doc["reviews"]), 2)
        self.assertIn("scraped_at", doc)

    def test_mongo_reviews_uniqueness_and_upsert(self):
        mgr = MongoDBManager(
            db_name="zomato_test_reviews",
            collection_name="restaurants_test",
            details_collection_name="restaurants_data_test",
            reviews_collection_name="reviews_test",
        )
        if not mgr.connect():
            self.skipTest("MongoDB not reachable")

        try:
            mgr.reviews_collection.delete_many({})

            sample_doc = {
                "_id": 88888,
                "restaurant_id": 88888,
                "name": "Reviews Test Cafe",
                "city": "Hyderabad",
                "total_reviews": 1,
                "number_of_pages": 1,
                "pages_scraped": 1,
                "reviews": [{"review_id": 1, "review_text": "Nice coffee"}],
            }

            # 1. First upsert -> 1 inserted
            up1, mod1 = mgr.upsert_restaurant_reviews(sample_doc)
            self.assertEqual(up1, 1)
            self.assertEqual(mod1, 0)
            self.assertEqual(mgr.get_reviews_count(), 1)

            # 2. Update with additional reviews -> 0 inserted, 1 modified, count stays 1 (strictly unique!)
            sample_doc["reviews"].append({"review_id": 2, "review_text": "Good snacks"})
            sample_doc["total_reviews"] = 2
            up2, mod2 = mgr.upsert_restaurant_reviews(sample_doc)
            self.assertEqual(up2, 0)
            self.assertEqual(mod2, 1)
            self.assertEqual(mgr.get_reviews_count(), 1)

            stored = mgr.reviews_collection.find_one({"_id": 88888})
            self.assertEqual(stored["_id"], 88888)
            self.assertEqual(stored["total_reviews"], 2)
            self.assertEqual(len(stored["reviews"]), 2)
        finally:
            mgr.client.drop_database("zomato_test_reviews")
            mgr.close()

    def test_scrape_restaurant_reviews_pagination(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = Config()
            cfg.reviews_dir = tmp_path / "output_reviews"
            cfg.enable_mongo = False
            cfg.request_delay = 0

            scraper = ZomatoScraper(cfg)

            p1_response = {
                "page_data": {
                    "sections": {
                        "SECTION_REVIEWS": {
                            "numberOfPages": 2,
                            "currentPage": 1,
                        }
                    }
                },
                "entities": {
                    "REVIEWS": {
                        "101": {
                            "reviewId": 101,
                            "userName": "Alice",
                            "reviewText": "Superb mutton biryani",
                            "rating": "5.0",
                        },
                        "102": {
                            "reviewId": 102,
                            "userName": "Bob",
                            "reviewText": "Good quantity",
                            "rating": "4.0",
                        },
                    },
                    "RATING": {"r1": {"rating": 5}},
                    "REVIEW_COMMENTS": {},
                },
            }

            p2_response = {
                "page_data": {
                    "sections": {
                        "SECTION_REVIEWS": {
                            "numberOfPages": 2,
                            "currentPage": 2,
                        }
                    }
                },
                "entities": {
                    "REVIEWS": {
                        "103": {
                            "reviewId": 103,
                            "userName": "Charlie",
                            "reviewText": "Prompt service",
                            "rating": "4.5",
                        }
                    },
                    "RATING": {"r2": {"rating": 4.5}},
                    "REVIEW_COMMENTS": {},
                },
            }

            with patch("requests.Session.get") as mock_get:
                def fake_get_side_effect(url, *args, **kwargs):
                    params = kwargs.get("params", {})
                    resp = MagicMock()
                    resp.status_code = 200
                    if params.get("page") == 1:
                        resp.json.return_value = p1_response
                    elif params.get("page") == 2:
                        resp.json.return_value = p2_response
                    return resp

                mock_get.side_effect = fake_get_side_effect

                doc = scraper.scrape_restaurant_reviews(
                    restaurant_id=12345,
                    name="Bawarchi",
                    city="Hyderabad",
                    location="Nallakunta",
                    page_url="/hyderabad/bawarchi-nallakunta",
                )

                self.assertIsNotNone(doc)
                self.assertEqual(doc["_id"], 12345)
                self.assertEqual(doc["number_of_pages"], 2)
                self.assertEqual(doc["pages_scraped"], 2)
                self.assertEqual(doc["total_reviews"], 3)
                self.assertEqual(len(doc["reviews"]), 3)
                self.assertIn("101", doc["raw_reviews_by_id"])
                self.assertIn("102", doc["raw_reviews_by_id"])
                self.assertIn("103", doc["raw_reviews_by_id"])

                # Check file persisted to disk
                file_path = cfg.reviews_dir / "nallakunta_hyderabad" / "12345.json"
                self.assertTrue(file_path.exists())

                with file_path.open("r", encoding="utf-8") as fh:
                    saved = json.load(fh)
                self.assertEqual(saved["_id"], 12345)
                self.assertEqual(saved["total_reviews"], 3)

            scraper.close()

    def test_scrape_restaurant_reviews_zero_pages(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = Config()
            cfg.reviews_dir = tmp_path / "output_reviews"
            cfg.enable_mongo = False
            cfg.request_delay = 0

            scraper = ZomatoScraper(cfg)

            zero_response = {
                "page_data": {
                    "sections": {
                        "SECTION_REVIEWS": {
                            "numberOfPages": 0,
                            "currentPage": 1,
                        }
                    }
                },
                "entities": {
                    "REVIEWS": [],
                },
            }

            with patch("requests.Session.get") as mock_get:
                mock_resp = MagicMock()
                mock_resp.status_code = 200
                mock_resp.json.return_value = zero_response
                mock_get.return_value = mock_resp

                doc = scraper.scrape_restaurant_reviews(
                    restaurant_id=20405347,
                    name="Brand New Place",
                    city="Hyderabad",
                    location="Gachibowli",
                )

                self.assertIsNotNone(doc)
                self.assertEqual(doc["_id"], 20405347)
                self.assertEqual(doc["number_of_pages"], 0)
                self.assertEqual(doc["pages_scraped"], 0)
                self.assertEqual(doc["total_reviews"], 0)
                self.assertEqual(len(doc["reviews"]), 0)

                file_path = cfg.reviews_dir / "gachibowli_hyderabad" / "20405347.json"
                self.assertTrue(file_path.exists())

            scraper.close()

    def test_sync_reviews_folder_to_mongo(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            reviews_dir = tmp_path / "output_reviews"
            loc_dir = reviews_dir / "kondapur_hyderabad"
            loc_dir.mkdir(parents=True, exist_ok=True)

            r1_doc = {
                "_id": 3001,
                "restaurant_id": 3001,
                "name": "Sync Rest 1",
                "city": "Hyderabad",
                "total_reviews": 1,
                "reviews": [{"review_id": 101, "review_text": "Tasty"}],
            }
            r2_doc = {
                "_id": 3002,
                "restaurant_id": 3002,
                "name": "Sync Rest 2",
                "city": "Hyderabad",
                "total_reviews": 1,
                "reviews": [{"review_id": 102, "review_text": "Good"}],
            }

            with (loc_dir / "3001.json").open("w", encoding="utf-8") as f:
                json.dump(r1_doc, f)
            with (loc_dir / "3002.json").open("w", encoding="utf-8") as f:
                json.dump(r2_doc, f)

            mgr = MongoDBManager(
                db_name="zomato_test_sync_rev",
                reviews_collection_name="reviews_test",
            )
            if not mgr.connect():
                self.skipTest("MongoDB not reachable")

            try:
                mgr.reviews_collection.delete_many({})
                res = sync_reviews_folder_to_mongo(reviews_dir, mgr)
                self.assertEqual(res["status"], "success")
                self.assertEqual(res["files_processed"], 2)
                self.assertEqual(res["upserted"], 2)
                self.assertEqual(mgr.get_reviews_count(), 2)
            finally:
                mgr.client.drop_database("zomato_test_sync_rev")
                mgr.close()

    def test_step3_simultaneous_details_and_reviews(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            cfg = Config()
            cfg.details_dir = tmp_path / "output_details"
            cfg.reviews_dir = tmp_path / "output_reviews"
            cfg.enable_mongo = False
            cfg.request_delay = 0

            scraper = ZomatoScraper(cfg)

            fake_detail = {
                "page_info": {"resId": 77777, "pageTitle": "Paradise, Secunderabad | Zomato"},
                "page_data": {
                    "sections": {
                        "SECTION_BASIC_INFO": {"res_id": 77777, "name": "Paradise", "cuisine_string": "Biryani"},
                        "SECTION_RES_CONTACT": {"address": "MG Road", "phoneDetails": {"phoneStr": "040-27843115"}},
                    }
                },
            }

            fake_reviews = {
                "page_data": {
                    "sections": {
                        "SECTION_REVIEWS": {
                            "numberOfPages": 1,
                            "currentPage": 1,
                        }
                    }
                },
                "entities": {
                    "REVIEWS": {
                        "9901": {
                            "reviewId": 9901,
                            "userName": "Ramesh",
                            "reviewText": "Classic world famous biryani",
                            "rating": "4.8",
                        }
                    }
                },
            }

            with patch("requests.Session.get") as mock_get:
                def fake_get_switch(url, *args, **kwargs):
                    resp = MagicMock()
                    resp.status_code = 200
                    if "getPage" in url:
                        resp.json.return_value = fake_detail
                    elif "reviews/loadMore" in url:
                        resp.json.return_value = fake_reviews
                    return resp

                mock_get.side_effect = fake_get_switch

                # Simultaneous Step 3: Fetch detail AND reviews
                res = scraper.scrape_restaurant_detail(
                    page_url="/hyderabad/paradise-secunderabad/info",
                    restaurant_id=77777,
                    city="Hyderabad",
                    location="Secunderabad",
                    fetch_reviews=True,
                )

                self.assertIsNotNone(res)

                # 1. Verify hotel detail file exists
                detail_file = cfg.details_dir / "secunderabad_hyderabad" / "77777.json"
                self.assertTrue(detail_file.exists())

                # 2. Verify reviews file was simultaneously created
                review_file = cfg.reviews_dir / "secunderabad_hyderabad" / "77777.json"
                self.assertTrue(review_file.exists())

                with review_file.open("r", encoding="utf-8") as rf:
                    r_data = json.load(rf)
                self.assertEqual(r_data["_id"], 77777)
                self.assertEqual(r_data["total_reviews"], 1)
                self.assertEqual(r_data["reviews"][0]["review_text"], "Classic world famous biryani")

            scraper.close()


class DeliveryDataParsingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.tmpdir.name)
        self.details_dir = self.base_dir / "output_details"
        self.reviews_dir = self.base_dir / "output_reviews"
        self.delivery_dir = self.base_dir / "output_delivery"
        self.details_dir.mkdir(parents=True, exist_ok=True)
        self.reviews_dir.mkdir(parents=True, exist_ok=True)
        self.delivery_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_build_delivery_data_document_all_fields(self):
        detail_data = {
            "page_info": {
                "resId": 90744,
                "name": "Exotica",
                "canonicalUrl": "https://www.zomato.com/hyderabad/exotica-banjara-hills",
            },
            "page_data": {
                "sections": {
                    "SECTION_BASIC_INFO": {
                        "res_id": 90744,
                        "name": "Exotica",
                        "timing": {"timing_desc": "Opens at 12:30pm"},
                        "rating": {
                            "aggregate_rating": "4.6",
                            "votes": 11519,
                        },
                        "rating_new": {
                            "ratings": {
                                "DINING": {
                                    "rating": "4.6",
                                    "reviewCount": "11.5K",
                                },
                                "DELIVERY": {
                                    "rating": "4.5",
                                    "reviewCount": "1,775",
                                },
                            }
                        },
                    },
                    "SECTION_RES_CONTACT": {
                        "city_name": "Hyderabad",
                        "locality_verbose": "Banjara Hills, Hyderabad",
                        "address": "5th Floor, 12th Square Building, Road 12, Banjara Hills, Hyderabad",
                        "phoneDetails": {"phoneStr": "+919550515500, +918121585500"},
                        "static_map_url": "https://maps.zomato.com/php/staticmap?center=17.4103,78.4372",
                    },
                }
            },
        }

        reviews_data = {
            "reviews": [
                {
                    "user_name": "Tejaswi",
                    "rating": "5",
                    "review_text": "Shivam and binica were excellent",
                    "like_count": 2,
                },
                {
                    "user_name": "Saurav",
                    "rating": "4.5",
                    "review_text": "Awesome food and ambience",
                    "like_count": 0,
                },
            ]
        }

        doc = build_delivery_data_document(
            detail_data=detail_data,
            reviews_data=reviews_data,
            restaurant_id=90744,
        )

        self.assertIsNotNone(doc)
        # Verify core fields
        self.assertEqual(doc["_id"], 90744)
        self.assertEqual(doc["restaurant_id"], 90744)
        self.assertEqual(doc["name"], "Exotica")
        self.assertEqual(doc["city"], "Hyderabad")
        self.assertEqual(doc["location"], "Banjara Hills, Hyderabad")
        self.assertEqual(doc["address"], "5th Floor, 12th Square Building, Road 12, Banjara Hills, Hyderabad")
        self.assertEqual(doc["phone_numbers"], "+919550515500, +918121585500")
        self.assertEqual(doc["timing"], "Opens at 12:30pm")
        self.assertEqual(doc["rating"], "4.6")
        self.assertEqual(doc["canonical_url"], "https://www.zomato.com/hyderabad/exotica-banjara-hills")

        # Verify dining & delivery ratings (clean flat fields)
        self.assertEqual(doc["aggregate_rating"], "4.6")
        self.assertEqual(doc["votes"], "11.5K")
        self.assertEqual(doc["delivery_rating"], "4.5")
        self.assertEqual(doc["reviewcount"], "1,775")

        # Verify static map url
        self.assertEqual(doc["static_map_url"], "https://maps.zomato.com/php/staticmap?center=17.4103,78.4372")

        # Verify reviews
        self.assertEqual(len(doc["reviews"]), 2)
        self.assertEqual(doc["reviews"][0]["user_name"], "Tejaswi")
        self.assertEqual(doc["reviews"][0]["rating"], "5")
        self.assertEqual(doc["reviews"][0]["review_text"], "Shivam and binica were excellent")
        self.assertEqual(doc["reviews"][0]["like_count"], 2)

        # Verify duplicate fields and objects are removed
        self.assertNotIn("rating_dining", doc)
        self.assertNotIn("rating_delivery", doc)
        self.assertNotIn("ratings", doc)
        self.assertNotIn("cannonical_url", doc)
        self.assertNotIn("staticmapurl", doc)
        self.assertNotIn("review_count", doc)
        self.assertNotIn("total_reviews", doc)

    def test_sync_delivery_folder_to_mongo(self):
        # Create a sample detail file
        subfolder_d = self.details_dir / "banjara_hills_hyderabad"
        subfolder_d.mkdir(parents=True, exist_ok=True)
        detail_file = subfolder_d / "90744.json"
        detail_file.write_text(
            json.dumps({
                "page_info": {"resId": 90744, "name": "Exotica"},
                "page_data": {
                    "sections": {
                        "SECTION_BASIC_INFO": {"name": "Exotica"},
                        "SECTION_RES_CONTACT": {"address": "Road 12, Banjara Hills", "city_name": "Hyderabad"},
                    }
                },
            }),
            encoding="utf-8",
        )

        # Create a sample review file
        subfolder_r = self.reviews_dir / "banjara_hills_hyderabad"
        subfolder_r.mkdir(parents=True, exist_ok=True)
        review_file = subfolder_r / "90744.json"
        review_file.write_text(
            json.dumps({
                "restaurant_id": 90744,
                "reviews": [
                    {"user_name": "Priya", "rating": "5", "review_text": "Great place", "like_count": 1}
                ],
            }),
            encoding="utf-8",
        )

        mock_mongo = MagicMock()
        mock_mongo.is_connected.return_value = True
        mock_mongo.delivery_collection = MagicMock()
        mock_mongo.get_delivery_count.return_value = 1
        bulk_res = MagicMock()
        bulk_res.upserted_count = 1
        bulk_res.modified_count = 0
        mock_mongo.delivery_collection.bulk_write.return_value = bulk_res

        res = sync_delivery_folder_to_mongo(
            details_dir=self.details_dir,
            reviews_dir=self.reviews_dir,
            delivery_dir=self.delivery_dir,
            mongo_manager=mock_mongo,
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["processed"], 1)

        # Check delivery JSON file written to output_delivery/
        deliv_out = self.delivery_dir / "banjara_hills_hyderabad" / "90744.json"
        self.assertTrue(deliv_out.exists())
        with deliv_out.open("r", encoding="utf-8") as df:
            d_data = json.load(df)
        self.assertEqual(d_data["restaurant_id"], 90744)
        self.assertEqual(d_data["name"], "Exotica")
        self.assertEqual(len(d_data["reviews"]), 1)
        self.assertEqual(d_data["reviews"][0]["user_name"], "Priya")


if __name__ == "__main__":
    unittest.main()
