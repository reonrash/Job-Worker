import re

# --- US State and Territory Standardization ---
# This comprehensive list helps map abbreviations (e.g., 'ca') to full names ('california')
# and ensures consistency when processing raw location strings.
US_STATE_MAP = {
    'alabama': 'al', 'alaska': 'ak', 'arizona': 'az', 'arkansas': 'ar', 'california': 'ca',
    'colorado': 'co', 'connecticut': 'ct', 'delaware': 'de', 'florida': 'fl', 'georgia': 'ga',
    'hawaii': 'hi', 'idaho': 'id', 'illinois': 'il', 'indiana': 'in', 'iowa': 'ia',
    'kansas': 'ks', 'kentucky': 'ky', 'louisiana': 'la', 'maine': 'me', 'maryland': 'md',
    'massachusetts': 'ma', 'michigan': 'mi', 'minnesota': 'mn', 'mississippi': 'ms',
    'missouri': 'mo', 'montana': 'mt', 'nebraska': 'ne', 'nevada': 'nv', 'new hampshire': 'nh',
    'new jersey': 'nj', 'new mexico': 'nm', 'new york': 'ny', 'north carolina': 'nc',
    'north dakota': 'nd', 'ohio': 'oh', 'oklahoma': 'ok', 'oregon': 'or', 'pennsylvania': 'pa',
    'rhode island': 'ri', 'south carolina': 'sc', 'south dakota': 'sd', 'tennessee': 'tn',
    'texas': 'tx', 'utah': 'ut', 'vermont': 'vt', 'virginia': 'va', 'washington': 'wa',
    'west virginia': 'wv', 'wisconsin': 'wi', 'wyoming': 'wy',
    # Territories/Districts (Now fully exhaustive)
    'district of columbia': 'dc', 'puerto rico': 'pr', 'us virgin islands': 'vi',
    'guam': 'gu', 'american samoa': 'as', 'northern mariana islands': 'mp'
}

# Reverse map for checking if an abbreviation is present
STATE_ABBR_MAP = {v: k for k, v in US_STATE_MAP.items()}

# Common city and regional nicknames/abbreviations
CITY_NICKNAMES = {
    'nyc': 'new york city',
    'sf': 'san francisco',
    'la': 'los angeles',
    'boston metro': 'boston',
    'bay area': 'san francisco bay area',
    'philly': 'philadelphia',
    'atl': 'atlanta',
    'dallas ft worth': 'dallas',
}

# --- Generic Terms Standardization and Removal ---
# Terms to replace with 'remote'
REMOTE_TERMS = {
    'remote': 'remote', 'home': 'remote', 'virtual': 'remote', 'fully remote': 'remote',
    'work from home': 'remote', 'wfh': 'remote', 'anywhere': 'remote'
}

# Terms to remove as geographical noise
NOISE_TERMS = [
    'area', 'metro', 'county', 'district', 'city', 'region', 'state', 'us', 'usa', 'united states',
    'global', 'worldwide', 'office', 'headquarters', 'hq', 'hubs', 'inc', 'llc', 'corp', 'company',
]
# Add state abbreviations to noise terms for cleanup after normalization
NOISE_TERMS.extend(list(US_STATE_MAP.values()))


def normalize_job_data(job_data: dict) -> dict:
    """
    Normalizes job data by adding normalized fields.

    Args:
        job_data: Dictionary containing job fields including 'location'

    Returns:
        Updated job_data dictionary with normalized_location field added
    """
    normalized_data = job_data.copy()

    # Normalize location if present
    raw_location = job_data.get('location', '')
    normalized_data['normalized_location'] = normalize_location(raw_location)

    return normalized_data


def normalize_location(raw_location: str) -> str:
    """
    Cleans a raw location string into a standardized, searchable format for matching.

    The process: Lowercase -> Clean Punctuation -> Map Abbreviations -> Remove Noise.

    Args:
        raw_location: The location string scraped from the job site.

    Returns:
        The normalized location string (e.g., 'san francisco california' or 'remote').
    """
    if not raw_location:
        return ""

    # 1. Base Cleanup (Lowercase and replace non-alphanumeric with space)
    normalized = raw_location.lower().strip()
    # Replace common separators (comma, slash, hyphen) with a space
    normalized = re.sub(r'[\/\-,]', ' ', normalized)
    # Remove parentheses and other non-standard characters
    normalized = re.sub(r'[^\w\s]', '', normalized)

    # 2. Tokenize and Standardize
    words = normalized.split()
    standardized_words = []

    for word in words:
        # Check for remote terms first
        if word in REMOTE_TERMS:
            standardized_words.append('remote')
            continue

        # Check for city nicknames/abbreviations
        if word in CITY_NICKNAMES:
            standardized_words.extend(CITY_NICKNAMES[word].split())
            continue

        # Check for state abbreviations (e.g., 'ca')
        if word in STATE_ABBR_MAP:
            # We map 'ca' to 'california' for consistent searching
            standardized_words.append(STATE_ABBR_MAP[word])
            continue

        # Check for full state names and append the abbreviation as well (helpful for ILIKE matching)
        if word in US_STATE_MAP:
            standardized_words.append(word)
            standardized_words.append(US_STATE_MAP[word])
            continue

        # If it's none of the above, keep the word
        standardized_words.append(word)

    # 3. Final Cleaning (Remove duplicates and noise)

    # Create a set to handle duplicates and remove noise terms efficiently
    cleaned_set = set()
    for word in standardized_words:
        # Filter out common noise words
        if word not in NOISE_TERMS and len(word) > 1: # Ignore single letters (like 'a', 'i')
            cleaned_set.add(word)

    # The result should contain unique, standardized terms, sorted for consistency
    final_normalized = ' '.join(sorted(list(cleaned_set))).strip()

    # If the result is empty after cleaning, return the original raw cleaned string
    return final_normalized if final_normalized else normalized


if __name__ == "__main__":
    # --- Example Usage ---
    test_locations = [
        "San Francisco, CA (Bay Area Office)", # Complex city/state/noise
        "NYC / NY State Hybrid",               # Abbreviation/State/Remote qualifier
        "Remote-US Only",                      # Remote and country noise
        "Philly Metro",                        # Nickname and noise
        "tx",                                  # State abbreviation
        "Washington, D.C.",                    # District of Columbia
        "New York, NY",                        # Simple City, State
        "Seattle, WA",                         # Standard city/state
        "Job located in Guam"                  # New territory test
    ]

    print("--- Extensive US Location Normalization Test ---")
    for loc in test_locations:
        normalized = normalize_location(loc)
        print(f"Raw: '{loc}' -> Normalized: '{normalized}'")
        print("-" * 30)