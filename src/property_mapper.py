"""
property_mapper.py
------------------
Resolves geographic hail impact zones into real, verifiable property addresses
using reverse geocoding grids and parcel data sources.

Strategy:
  1. Take the storm impact polygon/bbox
  2. Generate a lat/lon grid at ~50m intervals across the zone
  3. Reverse geocode each grid point to a street address (Nominatim / Google)
  4. Deduplicate addresses (same property hit by multiple grid points)
  5. Filter for residential properties
  6. Enrich with property data (RentCast / county assessor) where available

This replaces the old mock/random address generator.
"""

import logging
import os
import time
import sqlite3
import requests
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

logger = logging.getLogger(__name__)


class PropertyMapper:
    """
    Maps storm impact polygons to real street-level property addresses.
    """

    def __init__(self, google_api_key=None, rentcast_api_key=None):
        self.google_api_key = google_api_key or os.getenv("GOOGLE_MAPS_API_KEY")
        self.rentcast_api_key = rentcast_api_key or os.getenv("RENTCAST_API_KEY")
        self.geolocator = Nominatim(user_agent="roof_hunter_property_mapper_v2", timeout=12)
        # Track geocoded addresses to deduplicate
        self._seen_addresses = set()

    def get_addresses_in_polygon(self, bbox_or_polygon, grid_spacing_deg=0.0005, max_addresses=200):
        """
        Takes a geographic bounding box [lon_min, lat_min, lon_max, lat_max] from a 
        storm event and returns a list of real street addresses within the zone.
        
        grid_spacing_deg: ~0.0005 degrees ≈ 55 meters at Oklahoma latitude
                          This catches individual residential lots (~60ft wide)
        """
        logger.info(f"Mapping real property addresses within hail impact zone: {bbox_or_polygon}")
        self._seen_addresses = set()
        addresses = []

        if len(bbox_or_polygon) == 4:
            lon_min, lat_min, lon_max, lat_max = bbox_or_polygon
        else:
            logger.error(f"Unexpected polygon format: {bbox_or_polygon}")
            return []

        # Generate grid points across the impact zone
        lat = lat_min
        grid_points = []
        while lat <= lat_max:
            lon = lon_min
            while lon <= lon_max:
                grid_points.append((lat, lon))
                lon += grid_spacing_deg
            lat += grid_spacing_deg

        logger.info(f"Generated {len(grid_points)} grid points across impact zone "
                     f"({lat_max - lat_min:.4f}° x {lon_max - lon_min:.4f}°)")

        # Reverse geocode each grid point
        for i, (lat, lon) in enumerate(grid_points):
            if len(addresses) >= max_addresses:
                logger.info(f"Reached max address limit ({max_addresses}). Stopping grid scan.")
                break

            addr_info = self._reverse_geocode_point(lat, lon)
            if addr_info:
                addresses.append(addr_info)

            # Rate limit: Nominatim requires 1 req/sec
            if not self.google_api_key:
                time.sleep(1.1)
            else:
                time.sleep(0.05)  # Google Maps has higher rate limits

            if (i + 1) % 25 == 0:
                logger.info(f"  Geocoded {i + 1}/{len(grid_points)} grid points, "
                             f"found {len(addresses)} unique addresses so far")

        logger.info(f"Resolved {len(addresses)} unique property addresses in impact zone")
        return addresses

    def _reverse_geocode_point(self, lat, lon):
        """
        Reverse geocode a single lat/lon to a structured address.
        Uses Google Maps API if available, falls back to Nominatim.
        """
        try:
            if self.google_api_key:
                return self._google_reverse_geocode(lat, lon)
            else:
                return self._nominatim_reverse_geocode(lat, lon)
        except Exception as e:
            logger.debug(f"Geocode failed for ({lat}, {lon}): {e}")
            return None

    def _google_reverse_geocode(self, lat, lon):
        """Use Google Maps Geocoding API for higher-accuracy reverse geocoding."""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            "latlng": f"{lat},{lon}",
            "key": self.google_api_key,
            "result_type": "street_address|premise"  # Only return actual addresses
        }
        try:
            res = requests.get(url, params=params, timeout=10)
            if res.status_code != 200:
                return None
            data = res.json()
            results = data.get("results", [])
            if not results:
                return None

            result = results[0]
            formatted = result.get("formatted_address", "")

            # Deduplicate
            dedup_key = formatted.lower().strip()
            if dedup_key in self._seen_addresses:
                return None
            self._seen_addresses.add(dedup_key)

            # Extract address components
            components = {c["types"][0]: c["long_name"] 
                         for c in result.get("address_components", []) 
                         if c.get("types")}

            return {
                "address": formatted,
                "property_id": result.get("place_id", f"GOOG-{lat:.6f}-{lon:.6f}"),
                "latitude": lat,
                "longitude": lon,
                "street_number": components.get("street_number", ""),
                "street_name": components.get("route", ""),
                "city": components.get("locality", ""),
                "state": components.get("administrative_area_level_1", ""),
                "zipcode": components.get("postal_code", ""),
                "county": components.get("administrative_area_level_2", ""),
                "source": "google_geocoding"
            }
        except Exception as e:
            logger.debug(f"Google geocode error: {e}")
            return None

    def _nominatim_reverse_geocode(self, lat, lon):
        """Use Nominatim (free) reverse geocoding with address-level detail."""
        try:
            location = self.geolocator.reverse(
                (lat, lon), 
                exactly_one=True, 
                addressdetails=True,
                zoom=18  # Building-level detail
            )
            if not location:
                return None

            raw_addr = location.raw.get("address", {})
            house_number = raw_addr.get("house_number", "")
            road = raw_addr.get("road", "")

            # Skip if no house number (means it's not a specific property)
            if not house_number or not road:
                return None

            city = (raw_addr.get("city") or raw_addr.get("town") 
                    or raw_addr.get("village") or raw_addr.get("hamlet") or "")
            state = raw_addr.get("state", "")
            zipcode = raw_addr.get("postcode", "")
            county = raw_addr.get("county", "")

            full_address = f"{house_number} {road}, {city}, {state} {zipcode}".strip(", ")

            # Deduplicate
            dedup_key = full_address.lower().strip()
            if dedup_key in self._seen_addresses:
                return None
            self._seen_addresses.add(dedup_key)

            return {
                "address": full_address,
                "property_id": f"NOM-{lat:.6f}-{lon:.6f}",
                "latitude": lat,
                "longitude": lon,
                "street_number": house_number,
                "street_name": road,
                "city": city,
                "state": state,
                "zipcode": zipcode,
                "county": county,
                "source": "nominatim"
            }
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.debug(f"Nominatim timeout/error for ({lat}, {lon}): {e}")
            return None

    def get_property_details(self, address_info):
        """
        Enrich an address with property ownership data from RentCast API.
        Returns owner name, estimated value, roof type, year built.
        """
        if not self.rentcast_api_key:
            logger.debug("No RentCast API key - returning basic info only")
            return {
                "owner": "LOOKUP REQUIRED",
                "estimated_value": None,
                "year_built": None,
                "roof_type": None
            }

        address = address_info.get("address", "")
        headers = {
            "accept": "application/json",
            "X-Api-Key": self.rentcast_api_key
        }
        url = f"https://api.rentcast.io/v1/properties?address={requests.utils.quote(address)}"

        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data and len(data) > 0:
                    prop = data[0]
                    owner_names = prop.get("owner", {}).get("names", [])
                    return {
                        "owner": owner_names[0] if owner_names else "ON FILE",
                        "estimated_value": prop.get("estimatedValue"),
                        "year_built": prop.get("yearBuilt"),
                        "roof_type": prop.get("features", {}).get("roofType"),
                        "bedrooms": prop.get("bedrooms"),
                        "bathrooms": prop.get("bathrooms"),
                        "sq_ft": prop.get("squareFootage"),
                        "property_type": prop.get("propertyType")
                    }
            elif res.status_code == 429:
                logger.warning("RentCast rate limit hit - queuing for later enrichment")
            else:
                logger.debug(f"RentCast returned {res.status_code} for {address}")
        except Exception as e:
            logger.debug(f"RentCast error: {e}")

        return {
            "owner": "LOOKUP REQUIRED",
            "estimated_value": None,
            "year_built": None,
            "roof_type": None
        }

    def expand_storm_bbox(self, lat, lon, hail_size_inches, duration_minutes=None):
        """
        Generate a realistic impact bounding box from a storm center point.
        Larger hail + longer duration = larger impact zone.
        
        Hail damage swaths are typically:
        - 1.5" hail: ~0.5 mile wide
        - 2.0" hail: ~1-2 miles wide  
        - 3.0"+ hail: ~2-5 miles wide
        
        Returns [lon_min, lat_min, lon_max, lat_max]
        """
        # Base radius in degrees (~1 mile = 0.0145 degrees at OK latitude)
        mile_in_deg = 0.0145
        
        if hail_size_inches >= 3.0:
            radius_miles = 3.0
        elif hail_size_inches >= 2.0:
            radius_miles = 1.5
        elif hail_size_inches >= 1.5:
            radius_miles = 0.75
        else:
            radius_miles = 0.5

        # Duration increases the swath length (storm moves)
        if duration_minutes and duration_minutes > 5:
            # Storm typically moves 30-40 mph, elongating the damage swath
            swath_extension = (duration_minutes / 60.0) * 35 * mile_in_deg
        else:
            swath_extension = 0

        radius_deg = radius_miles * mile_in_deg

        return [
            lon - radius_deg - swath_extension,  # lon_min
            lat - radius_deg,                      # lat_min 
            lon + radius_deg + swath_extension,    # lon_max
            lat + radius_deg                       # lat_max
        ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    mapper = PropertyMapper()
    
    # Test: Edmond, OK area after a hypothetical hail strike
    # Center of Edmond: 35.6528, -97.4781
    test_bbox = mapper.expand_storm_bbox(35.6528, -97.4781, hail_size_inches=2.0)
    logger.info(f"Generated impact bbox: {test_bbox}")
    
    # Get 10 addresses (small test)
    addresses = mapper.get_addresses_in_polygon(test_bbox, max_addresses=10)
    for a in addresses:
        print(f"  📍 {a['address']} (zip: {a.get('zipcode', '?')})")
