# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Cruise Data Generator - Initial Load
# MAGIC
# MAGIC Generates synthetic ship, sailing, booking, and survey data for either:
# MAGIC - **River Cruises** -> based on Viking River Cruises fleet
# MAGIC - **Ocean Cruises** -> based on Royal Caribbean International fleet (excludes Celebrity / Silversea)
# MAGIC
# MAGIC Reference source: `data_notes.txt` in this git folder.
# MAGIC
# MAGIC ### Output tables
# MAGIC Each run writes four Delta tables into `serverless_stable_yrljz5_catalog.survey_demo`
# MAGIC with a prefix of `river_` or `ocean_` depending on the `cruise_type` parameter:
# MAGIC
# MAGIC | Table | Purpose |
# MAGIC |-------|---------|
# MAGIC | `<prefix>_ship_dim`    | Ship dimension (ship_name without "Viking" prefix for River, berths) |
# MAGIC | `<prefix>_sailing_dim` | Sailing dimension (ship ref, itinerary, sailing date, duration) |
# MAGIC | `<prefix>_booking`     | One row per passenger booking (pax_id, sailing ref, amount paid) |
# MAGIC | `<prefix>_survey`      | 45% response rate; scores 0-10 and optional text comments |
# MAGIC
# MAGIC ### Injected quality signal
# MAGIC Two sailings at least 2 months apart are flagged as "bad experience":
# MAGIC - `likelihood_to_recommend` (NPS proxy) lowered by 4 points
# MAGIC - `likelihood_to_book_another` lowered by 4 points
# MAGIC - All amenity-comment respondents on those sailings cite problems with the
# MAGIC   same specific amenity as the cause.

# COMMAND ----------

# MAGIC %md ## 1. Configuration

# COMMAND ----------

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd

dbutils.widgets.dropdown("cruise_type", "River", ["River", "Ocean"], "Cruise type")
cruise_type = dbutils.widgets.get("cruise_type")

CATALOG = "serverless_stable_yrljz5_catalog"
SCHEMA = "survey_demo"
PREFIX = cruise_type.lower()  # 'river' or 'ocean'

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

WINDOW_START = date(2026, 1, 1)
WINDOW_END = date(2026, 12, 31)

print(f"cruise_type       = {cruise_type}")
print(f"target namespace  = {CATALOG}.{SCHEMA}")
print(f"table prefix      = {PREFIX}_")
print(f"sailing window    = {WINDOW_START} to {WINDOW_END}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## 2. Reference data

# COMMAND ----------

# ------- VIKING (River) -------
_VIKING_LONGSHIP_NAMES = [
    "Aegir", "Alruna", "Alsvin", "Annar", "Atla", "Baldur", "Bestla", "Beyla",
    "Bragi", "Buri", "Dagur", "Delling", "Egdir", "Egil", "Einar", "Eir",
    "Eistla", "Eldir", "Embla", "Fjolvar", "Forseti", "Freya", "Fulla",
    "Gefjon", "Gersemi", "Gullveig", "Gyda", "Gymir", "Haki", "Halogi",
    "Heimdal", "Herja", "Hermod", "Hervor", "Hild", "Hlin", "Hnoss", "Honir",
    "Idi", "Idun", "Ingvi", "Jarl", "Kadlin", "Kara", "Kvasir", "Laga", "Lif",
    "Lofn", "Magni", "Mani", "Mimir", "Modi", "Njord", "Odin", "Ran", "Rinda",
    "Rolf", "Rota", "Saga", "Sigrun", "Sigyn", "Sjofn", "Skadi", "Skirnir",
    "Sol", "Tialfi", "Tir", "Tor", "Ullur", "Vali", "Var", "Ve", "Vidar",
    "Vilhjalm", "Vili",
]
_VIKING_SEINE_NAMES = ["Fjorgyn", "Kari", "Nerthus", "Radgrid", "Skaga"]
_VIKING_DOURO_NAMES = ["Helgrim", "Hemming", "Osfrid", "Torgil"]
_VIKING_NILE_NAMES = [
    "Amun", "Antares", "Anubis", "Aton", "Geb", "Hathor",
    "Osiris", "Ptah", "Ra", "Sekhmet", "Sobek", "Thoth",
]
_VIKING_MEKONG_NAMES = ["Saigon", "Tonle"]

VIKING_SHIPS = (
    [(f"Viking {n}", "Longship", 190)       for n in _VIKING_LONGSHIP_NAMES] +
    [(f"Viking {n}", "Longship-Seine", 190) for n in _VIKING_SEINE_NAMES] +
    [(f"Viking {n}", "Douro", 106)          for n in _VIKING_DOURO_NAMES] +
    [(f"Viking {n}", "Nile", 82)            for n in _VIKING_NILE_NAMES] +
    [(f"Viking {n}", "Mekong", 80)          for n in _VIKING_MEKONG_NAMES] +
    [("Viking Mississippi", "Mississippi", 386)]
)

# (name, days, [compatible ship classes])
VIKING_ITINERARIES = [
    ("Rhine Getaway",                     8,  ["Longship"]),
    ("Grand European Tour",               15, ["Longship"]),
    ("Romantic Danube",                   8,  ["Longship"]),
    ("Danube Waltz",                      8,  ["Longship"]),
    ("Danube Explorer",                   8,  ["Longship"]),
    ("Passage to Eastern Europe",         11, ["Longship"]),
    ("European Sojourn",                  23, ["Longship"]),
    ("Rhine, Main & Danube Odyssey",      18, ["Longship"]),
    ("Cities of Light",                   12, ["Longship"]),
    ("Christmas on the Rhine",            8,  ["Longship"]),
    ("Danube Christmas Delights",         8,  ["Longship"]),
    ("Lyon & Provence",                   8,  ["Longship"]),
    ("Chateaux, Rivers & Wine",           8,  ["Longship"]),
    ("Tulips & Windmills",                8,  ["Longship"]),
    ("Elegant Elbe",                      10, ["Longship"]),
    ("Paris & the Heart of Normandy",     8,  ["Longship-Seine"]),
    ("France's Finest",                   15, ["Longship-Seine"]),
    ("Paris to the Swiss Alps",           12, ["Longship-Seine"]),
    ("Portugal's River of Gold",          10, ["Douro"]),
    ("Portugal's River of Gold & Spain",  11, ["Douro"]),
    ("Pharaohs & Pyramids",               12, ["Nile"]),
    ("Magnificent Mekong",                15, ["Mekong"]),
    ("America's Great River",             15, ["Mississippi"]),
]

# ------- ROYAL CARIBBEAN (Ocean) -------
ROYAL_SHIPS = [
    # Icon class
    ("Icon of the Seas",         "Icon",     5610),
    ("Star of the Seas",         "Icon",     5610),
    ("Legend of the Seas",       "Icon",     5610),
    # Oasis class
    ("Oasis of the Seas",        "Oasis",    5602),
    ("Allure of the Seas",       "Oasis",    5718),
    ("Harmony of the Seas",      "Oasis",    5479),
    ("Symphony of the Seas",     "Oasis",    5518),
    ("Wonder of the Seas",       "Oasis",    5734),
    ("Utopia of the Seas",       "Oasis",    5668),
    # Quantum class (Quantum + Quantum Ultra)
    ("Quantum of the Seas",      "Quantum",  4180),
    ("Anthem of the Seas",       "Quantum",  4180),
    ("Ovation of the Seas",      "Quantum",  4180),
    ("Spectrum of the Seas",     "Quantum",  4246),
    ("Odyssey of the Seas",      "Quantum",  4198),
    # Freedom class
    ("Freedom of the Seas",      "Freedom",  3926),
    ("Liberty of the Seas",      "Freedom",  3798),
    ("Independence of the Seas", "Freedom",  3858),
    # Voyager class
    ("Voyager of the Seas",      "Voyager",  3602),
    ("Explorer of the Seas",     "Voyager",  3286),
    ("Adventure of the Seas",    "Voyager",  3114),
    ("Navigator of the Seas",    "Voyager",  3388),
    ("Mariner of the Seas",      "Voyager",  3344),
    # Radiance class
    ("Radiance of the Seas",     "Radiance", 2143),
    ("Brilliance of the Seas",   "Radiance", 2142),
    ("Serenade of the Seas",     "Radiance", 2143),
    ("Jewel of the Seas",        "Radiance", 2191),
    # Vision class
    ("Grandeur of the Seas",     "Vision",   1992),
    ("Enchantment of the Seas",  "Vision",   2252),
    ("Rhapsody of the Seas",     "Vision",   2040),
    ("Vision of the Seas",       "Vision",   2050),
]

ROYAL_ITINERARIES = [
    ("Eastern Caribbean 7-Night", 7,  ["Icon", "Oasis", "Freedom", "Voyager", "Radiance", "Vision"]),
    ("Western Caribbean 7-Night", 7,  ["Icon", "Oasis", "Freedom", "Voyager", "Radiance", "Vision"]),
    ("Southern Caribbean 7-Night", 7, ["Radiance", "Voyager", "Vision"]),
    ("Caribbean 3-Night",         3,  ["Oasis", "Freedom", "Voyager", "Vision"]),
    ("Caribbean 4-Night",         4,  ["Oasis", "Freedom", "Voyager", "Vision"]),
    ("Caribbean 5-Night",         5,  ["Oasis", "Freedom", "Voyager", "Vision"]),
    ("Bahamas 3-Night",           3,  ["Icon", "Oasis", "Freedom", "Voyager", "Radiance", "Vision"]),
    ("Bahamas 4-Night",           4,  ["Icon", "Oasis", "Freedom", "Voyager", "Radiance", "Vision"]),
    ("Bermuda 7-Night",           7,  ["Freedom", "Voyager"]),
    ("Alaska 7-Night",            7,  ["Quantum", "Voyager", "Radiance"]),
    ("Mediterranean 7-Night",     7,  ["Icon", "Oasis", "Quantum", "Freedom", "Voyager", "Radiance"]),
    ("Greek Isles 7-Night",       7,  ["Oasis", "Quantum", "Vision"]),
    ("Northern Europe 7-Night",   7,  ["Freedom", "Voyager", "Vision"]),
    ("Norwegian Fjords 7-Night",  7,  ["Freedom"]),
    ("Mexican Riviera 4-Night",   4,  ["Quantum", "Voyager"]),
    ("Mexican Riviera 7-Night",   7,  ["Quantum", "Voyager"]),
    ("Australia 7-Night",         7,  ["Quantum", "Voyager"]),
    ("Southeast Asia 4-Night",    4,  ["Quantum", "Voyager"]),
    ("Japan 5-Night",             5,  ["Quantum"]),
    ("Transatlantic 14-Night",    14, ["Oasis", "Voyager"]),
    ("Panama Canal 10-Night",     10, ["Radiance"]),
    ("Canada & New England 7-Night", 7, ["Radiance"]),
]

if cruise_type == "River":
    ships_raw            = VIKING_SHIPS
    itineraries          = VIKING_ITINERARIES
    base_price_per_night = 400.0        # Viking is premium
    name_strip_prefix    = "Viking "
else:
    ships_raw            = ROYAL_SHIPS
    itineraries          = ROYAL_ITINERARIES
    base_price_per_night = 150.0        # Royal Caribbean mid-market
    name_strip_prefix    = ""

print(f"Ships loaded: {len(ships_raw)}")
print(f"Itineraries loaded: {len(itineraries)}")

# COMMAND ----------

# MAGIC %md ## 3. Build Ship DIM

# COMMAND ----------

ship_rows = []
for i, (full_name, ship_class, berths) in enumerate(ships_raw, start=1):
    short_name = full_name.replace(name_strip_prefix, "", 1) if name_strip_prefix else full_name
    ship_rows.append({
        "ship_id":    i,
        "ship_name":  short_name,
        "ship_class": ship_class,
        "berths":     berths,
    })

ship_df_pd = pd.DataFrame(ship_rows)
print(f"Ships: {len(ship_df_pd)}  |  Total berths: {ship_df_pd['berths'].sum():,}")
display(ship_df_pd.head(15))

# COMMAND ----------

# MAGIC %md ## 4. Build Sailing DIM
# MAGIC
# MAGIC Each ship cycles through itineraries compatible with its class, with a
# MAGIC 1-day turnaround between sailings so ships are nearly always under sail.
# MAGIC
# MAGIC Approximately 2.5% of ships are flagged for a 14-21 day dry-dock window
# MAGIC somewhere during the year (keeps idle ratio under 3% at any snapshot).

# COMMAND ----------

class_to_itins = {}
for itin_name, days, classes in itineraries:
    for c in classes:
        class_to_itins.setdefault(c, []).append((itin_name, days))

num_ships = len(ship_rows)
dry_dock_count = max(1, int(round(num_ships * 0.025)))
dry_dock_ids = set(random.sample([s["ship_id"] for s in ship_rows], dry_dock_count))
print(f"Dry-dock ships: {dry_dock_count} / {num_ships} ({100*dry_dock_count/num_ships:.1f}%)")

sailing_rows = []
sailing_id = 1
for ship in ship_rows:
    ship_itins = class_to_itins.get(ship["ship_class"], [])
    if not ship_itins:
        continue

    has_dd = ship["ship_id"] in dry_dock_ids
    dd_start = dd_end = None
    if has_dd:
        offset = random.randint(60, 300)
        dd_start = WINDOW_START + timedelta(days=offset)
        dd_end = dd_start + timedelta(days=random.randint(14, 21))

    cur = WINDOW_START
    while cur < WINDOW_END:
        # Skip if currently in dry-dock
        if dd_start is not None and dd_start <= cur < dd_end:
            cur = dd_end + timedelta(days=1)
            continue

        itin_name, days = random.choice(ship_itins)
        end = cur + timedelta(days=days)

        # Skip itineraries that overlap into the dry-dock window
        if dd_start is not None and cur < dd_start and end > dd_start:
            cur = dd_end + timedelta(days=1)
            continue

        if end > WINDOW_END:
            break

        sailing_rows.append({
            "sailing_id":     sailing_id,
            "ship_id":        ship["ship_id"],
            "itinerary_name": itin_name,
            "sailing_date":   cur,
            "sailing_days":   days,
            "end_date":       end,
        })
        sailing_id += 1
        cur = end + timedelta(days=1)   # one-day turnaround

sailing_df_pd = pd.DataFrame(sailing_rows)
print(f"Sailings: {len(sailing_df_pd):,}")
display(sailing_df_pd.head(10))

# COMMAND ----------

# MAGIC %md ## 5. Build Booking table
# MAGIC
# MAGIC Each sailing is filled to 95-110% of its ship's berth count (so some
# MAGIC sailings are over-booked, mirroring real cruise occupancy). Price is a
# MAGIC function of per-night base rate x duration x cabin class multiplier x noise.

# COMMAND ----------

berths_by_id = {s["ship_id"]: s["berths"] for s in ship_rows}

occ_factors    = np.random.uniform(0.95, 1.10, size=len(sailing_rows))
booking_counts = np.array([
    int(round(berths_by_id[s["ship_id"]] * occ_factors[i]))
    for i, s in enumerate(sailing_rows)
])
total_bookings = int(booking_counts.sum())
print(f"Total bookings: {total_bookings:,}")

sailing_ids_arr   = np.repeat([s["sailing_id"]   for s in sailing_rows], booking_counts)
sailing_days_arr  = np.repeat([s["sailing_days"] for s in sailing_rows], booking_counts)

# Cabin class multipliers (interior cheap -> suite expensive)
cabin_multipliers = np.random.choice(
    [0.8, 1.0, 1.2, 1.5, 2.0, 3.0],
    size=total_bookings,
    p=[0.28, 0.32, 0.20, 0.12, 0.06, 0.02],
)
noise = np.random.uniform(0.9, 1.1, size=total_bookings)

amounts = np.round(base_price_per_night * sailing_days_arr * cabin_multipliers * noise, 2)

booking_df_pd = pd.DataFrame({
    "booking_id":  np.arange(1, total_bookings + 1, dtype=np.int64),
    "pax_id":      np.arange(1, total_bookings + 1, dtype=np.int64),
    "sailing_id":  sailing_ids_arr.astype(np.int64),
    "amount_paid": amounts,
})
print(f"Booking rows: {len(booking_df_pd):,}")
display(booking_df_pd.head(10))

# COMMAND ----------

# MAGIC %md ## 6. Build Survey table (with issue injection)
# MAGIC
# MAGIC - 45% of bookings respond
# MAGIC - All scores 0-10 (int), from clipped normal distributions centered high
# MAGIC - 15% of respondents comment on stateroom (90% positive / 10% negative)
# MAGIC - 35% of respondents comment on amenities (70% / 20% / 10% pos/neu/neg)
# MAGIC - Two sailings (>=2 months apart) have `likelihood_to_recommend` and
# MAGIC   `likelihood_to_book_another` lowered by 4; their amenity-comment
# MAGIC   respondents all cite the same specific amenity problem.

# COMMAND ----------

POS_STATEROOM = [
    "Clean, comfortable, and quiet - a lovely retreat.",
    "The stateroom was spacious with plenty of storage.",
    "Loved the view from our balcony every morning.",
    "Beautiful decor and the bed was wonderfully comfortable.",
    "Housekeeping was excellent and the room was always spotless.",
    "Perfect size for a cruise, with every detail considered.",
    "Our verandah stateroom was the highlight of the trip.",
    "Great layout, lovely linens, and the bathroom was well-appointed.",
    "The room was immaculate and the amenities were top-notch.",
    "Modern, bright, and well-maintained - we felt right at home.",
]

NEG_STATEROOM = [
    "The room was smaller than we expected and felt cramped.",
    "The air conditioning was noisy and kept us up at night.",
    "Shower had very low water pressure throughout the trip.",
    "Our bed was uncomfortable and we didn't sleep well.",
    "The bathroom could really use a refresh - felt dated.",
    "We had a persistent odor in our stateroom that was never resolved.",
    "Thin walls - we heard everything from our neighbors.",
    "Storage was limited and we struggled to unpack properly.",
    "The TV did not work for most of the voyage.",
    "Housekeeping was inconsistent and we had to call multiple times.",
]

POS_AMENITY = [
    "Loved the pool deck - great way to unwind.",
    "The dining options were varied and consistently excellent.",
    "Entertainment was top-notch, especially the evening shows.",
    "The spa was pure bliss - already looking forward to the next trip.",
    "Fitness center was well-equipped and never too crowded.",
    "Lounge spaces were beautiful and comfortable.",
    "The bars had great cocktails and welcoming staff.",
    "Loved the quiet reading nooks and the library.",
    "The outdoor spaces made every sailing day memorable.",
    "Lots to do and great variety for all ages.",
]

NEUTRAL_AMENITY = [
    "Amenities were about what we expected - nothing stood out.",
    "Pool area is fine, but gets crowded on sea days.",
    "The dining venues are good; some are better than others.",
    "Entertainment schedule was decent but a bit repetitive.",
    "Gym has what you need, nothing fancy.",
    "The spa menu is standard - no surprises.",
    "Bars are OK; the drinks were as expected.",
    "Reasonable mix of amenities for a ship of this size.",
    "No complaints but also nothing that wowed us.",
    "Facilities are serviceable but could use some updating.",
]

NEG_AMENITY = [
    "The pool was overcrowded almost every day.",
    "Dining was slow and the quality was uneven.",
    "Entertainment was repetitive and the shows felt dated.",
    "Lines everywhere - we spent more time waiting than enjoying.",
    "Several amenities we booked for were unavailable.",
    "Bar service was consistently slow.",
    "The spa was overpriced for what it offered.",
    "Fitness center equipment was out of order most of the week.",
    "The lounge spaces were noisy and lacked seating.",
    "Many venues were closed or on reduced hours during our sailing.",
]

if cruise_type == "River":
    BAD_AMENITY_1 = "Aquavit Terrace"
    BAD_AMENITY_2 = "Sun Deck"
    BAD_COMMENTS = {
        "Aquavit Terrace": [
            "The Aquavit Terrace was closed for most of our trip - a huge letdown.",
            "We were really looking forward to al fresco dining on the Aquavit Terrace, but it was unavailable the entire cruise.",
            "Aquavit Terrace service was poor and the retractable glass doors were broken; could not use the space in bad weather.",
            "Half the Aquavit Terrace was roped off for repairs - it ruined the ambiance we booked for.",
            "The Aquavit Terrace, the main reason we chose Viking, was out of service - very disappointing.",
            "Problems with the Aquavit Terrace limited where we could eat; the backup options were overcrowded.",
            "The Aquavit Terrace had leaking windows and furniture was removed - a mess.",
            "Our Aquavit Terrace experience was ruined by ongoing maintenance work the whole week.",
        ],
        "Sun Deck": [
            "The Sun Deck was closed off for most of the cruise due to a safety issue.",
            "Sun Deck had broken loungers and the walking track was partially blocked all week.",
            "They had to shut down the Sun Deck for repairs - we lost one of the best features of the ship.",
            "The herb garden on the Sun Deck looked completely neglected and the putting green was closed.",
            "Sun Deck umbrellas were missing or broken; nowhere to escape the sun.",
            "Access to the Sun Deck was restricted all week - a major disappointment.",
            "The Sun Deck furniture was in disrepair and large sections were off-limits.",
            "Sun Deck was essentially unusable - no shade, broken chairs, and the bar was closed.",
        ],
    }
else:
    BAD_AMENITY_1 = "Category 6 Waterpark"
    BAD_AMENITY_2 = "Casino Royale"
    BAD_COMMENTS = {
        "Category 6 Waterpark": [
            "Category 6 Waterpark was closed almost the entire cruise - a huge letdown for the kids.",
            "Most of the Category 6 slides had technical issues; only 2 of 6 ever opened.",
            "The waterpark was the reason we booked this ship, and it was down most of the week.",
            "Category 6 Waterpark closures were poorly communicated - we lost many hours in line.",
            "Several Category 6 slides were out of service and the waits for working ones were brutal.",
            "The flagship Category 6 Waterpark was closed for maintenance - very frustrating.",
            "Category 6 was advertised heavily but was almost entirely non-functional during our sailing.",
            "Category 6 Waterpark issues ruined what should have been the highlight of the trip.",
        ],
        "Casino Royale": [
            "Casino Royale had machines down and tables closed - very limited gaming.",
            "The casino was understaffed and had major issues with the slot machines.",
            "Casino Royale closed early most nights - missed the whole experience.",
            "Casino smoke filtering was broken; the whole area was unpleasant to spend time in.",
            "Many of Casino Royale's tables were closed and the machines were malfunctioning.",
            "Casino Royale had serious technical problems - card shufflers down, slots offline.",
            "The casino experience was ruined by equipment failures that lasted the whole week.",
            "Casino Royale felt neglected - broken machines, no cocktail service, nothing worked right.",
        ],
    }

# Pick 2 bad sailings at least 2 months (60+ days) apart
candidates_early = [s for s in sailing_rows if s["sailing_date"].month in (3, 4)]
candidates_late  = [s for s in sailing_rows if s["sailing_date"].month in (9, 10)]
if not candidates_early or not candidates_late:
    # Fallback: just pick the first and last sailing that are >=60 days apart
    sorted_s = sorted(sailing_rows, key=lambda s: s["sailing_date"])
    candidates_early = [sorted_s[len(sorted_s)//4]]
    candidates_late  = [sorted_s[3*len(sorted_s)//4]]

bad_sailing_1 = random.choice(candidates_early)
bad_sailing_2 = random.choice(candidates_late)
days_apart = (bad_sailing_2["sailing_date"] - bad_sailing_1["sailing_date"]).days
assert abs(days_apart) >= 60, f"Bad sailings only {days_apart} days apart"

bad_sailing_map = {
    bad_sailing_1["sailing_id"]: BAD_AMENITY_1,
    bad_sailing_2["sailing_id"]: BAD_AMENITY_2,
}

print(f"Bad sailing 1: sailing_id={bad_sailing_1['sailing_id']}  "
      f"date={bad_sailing_1['sailing_date']}  amenity={BAD_AMENITY_1}")
print(f"Bad sailing 2: sailing_id={bad_sailing_2['sailing_id']}  "
      f"date={bad_sailing_2['sailing_date']}  amenity={BAD_AMENITY_2}")
print(f"Days apart: {days_apart}")

# COMMAND ----------

# 45% respond
responder_mask = np.random.random(total_bookings) < 0.45
responder_count = int(responder_mask.sum())
print(f"Responders: {responder_count:,} / {total_bookings:,} "
      f"({100*responder_count/total_bookings:.2f}%)")

resp_booking_ids = booking_df_pd["booking_id"].values[responder_mask]
resp_sailing_ids = booking_df_pd["sailing_id"].values[responder_mask]

def _norm_int(mean, sigma, size):
    return np.clip(np.round(np.random.normal(mean, sigma, size)), 0, 10).astype(np.int32)

overall       = _norm_int(8.3, 1.3, responder_count)
recommend     = _norm_int(8.5, 1.5, responder_count)
book_another  = _norm_int(8.0, 1.8, responder_count)
stateroom_sat = _norm_int(8.2, 1.5, responder_count)
amenity_sat   = _norm_int(8.0, 1.6, responder_count)

# Issue injection: lower NPS and likelihood-to-book by 4 on bad sailings
is_bad = np.isin(resp_sailing_ids, [bad_sailing_1["sailing_id"], bad_sailing_2["sailing_id"]])
recommend    = np.where(is_bad, np.clip(recommend    - 4, 0, 10), recommend).astype(np.int32)
book_another = np.where(is_bad, np.clip(book_another - 4, 0, 10), book_another).astype(np.int32)

# 15% stateroom comments: 90% pos / 10% neg
state_mask    = np.random.random(responder_count) < 0.15
state_pos_mix = np.random.random(responder_count) < 0.90
state_comments = np.full(responder_count, None, dtype=object)
for i in np.where(state_mask)[0]:
    pool = POS_STATEROOM if state_pos_mix[i] else NEG_STATEROOM
    state_comments[i] = random.choice(pool)

# 35% amenity comments: 70% pos / 20% neu / 10% neg
amen_mask      = np.random.random(responder_count) < 0.35
amen_rolls     = np.random.random(responder_count)
amen_comments  = np.full(responder_count, None, dtype=object)
for i in np.where(amen_mask)[0]:
    if is_bad[i]:
        amenity_name = bad_sailing_map[int(resp_sailing_ids[i])]
        amen_comments[i] = random.choice(BAD_COMMENTS[amenity_name])
    else:
        r = amen_rolls[i]
        if r < 0.70:
            amen_comments[i] = random.choice(POS_AMENITY)
        elif r < 0.90:
            amen_comments[i] = random.choice(NEUTRAL_AMENITY)
        else:
            amen_comments[i] = random.choice(NEG_AMENITY)

survey_df_pd = pd.DataFrame({
    "survey_id":                 np.arange(1, responder_count + 1, dtype=np.int64),
    "booking_id":                resp_booking_ids.astype(np.int64),
    "overall_experience":        overall,
    "likelihood_to_recommend":   recommend,
    "likelihood_to_book_another": book_another,
    "stateroom_satisfaction":    stateroom_sat,
    "stateroom_comment":         state_comments,
    "amenities_satisfaction":    amenity_sat,
    "amenities_comment":         amen_comments,
})
print(f"Survey rows: {len(survey_df_pd):,}")
display(survey_df_pd.head(10))

# COMMAND ----------

# MAGIC %md ## 7. Write to Delta

# COMMAND ----------

def _save(pd_df: pd.DataFrame, table: str) -> None:
    fqn = f"{CATALOG}.{SCHEMA}.{table}"
    sdf = spark.createDataFrame(pd_df)
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(fqn))
    cnt = spark.table(fqn).count()
    print(f"wrote {fqn:<70s} rows={cnt:,}")

_save(ship_df_pd,    f"{PREFIX}_ship_dim")
_save(sailing_df_pd, f"{PREFIX}_sailing_dim")
_save(booking_df_pd, f"{PREFIX}_booking")
_save(survey_df_pd,  f"{PREFIX}_survey")

# COMMAND ----------

# MAGIC %md ## 8. Verification

# COMMAND ----------

# MAGIC %md ### Row counts

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_ship_dim)    AS ships,
  (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_sailing_dim) AS sailings,
  (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_booking)     AS bookings,
  (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_survey)      AS surveys
"""))

# COMMAND ----------

# MAGIC %md ### Capacity utilisation (should fall between 95% and 110%)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  ROUND(100 * MIN(occupancy), 2) AS min_occupancy_pct,
  ROUND(100 * AVG(occupancy), 2) AS avg_occupancy_pct,
  ROUND(100 * MAX(occupancy), 2) AS max_occupancy_pct
FROM (
  SELECT b.sailing_id, COUNT(*) / s.berths AS occupancy
  FROM {CATALOG}.{SCHEMA}.{PREFIX}_booking b
  JOIN {CATALOG}.{SCHEMA}.{PREFIX}_sailing_dim sa ON b.sailing_id = sa.sailing_id
  JOIN {CATALOG}.{SCHEMA}.{PREFIX}_ship_dim   s  ON sa.ship_id   = s.ship_id
  GROUP BY b.sailing_id, s.berths
)
"""))

# COMMAND ----------

# MAGIC %md ### Survey response rate (target ~45%)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  ROUND(100 * (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_survey) /
              (SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{PREFIX}_booking), 2) AS response_rate_pct
"""))

# COMMAND ----------

# MAGIC %md ### Comment population rates (targets: 15% stateroom, 35% amenity)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  ROUND(100 * SUM(CASE WHEN stateroom_comment IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
    AS stateroom_comment_pct,
  ROUND(100 * SUM(CASE WHEN amenities_comment IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2)
    AS amenities_comment_pct
FROM {CATALOG}.{SCHEMA}.{PREFIX}_survey
"""))

# COMMAND ----------

# MAGIC %md ### Bad sailings surface (lowest avg recommend scores)

# COMMAND ----------

display(spark.sql(f"""
SELECT
  sa.sailing_id,
  sa.sailing_date,
  sa.itinerary_name,
  COUNT(sv.survey_id)                           AS survey_count,
  ROUND(AVG(sv.likelihood_to_recommend), 2)     AS avg_recommend,
  ROUND(AVG(sv.likelihood_to_book_another), 2)  AS avg_book_another,
  ROUND(AVG(sv.overall_experience), 2)          AS avg_overall
FROM {CATALOG}.{SCHEMA}.{PREFIX}_sailing_dim sa
JOIN {CATALOG}.{SCHEMA}.{PREFIX}_booking     b  ON sa.sailing_id = b.sailing_id
JOIN {CATALOG}.{SCHEMA}.{PREFIX}_survey      sv ON b.booking_id  = sv.booking_id
GROUP BY sa.sailing_id, sa.sailing_date, sa.itinerary_name
ORDER BY avg_recommend ASC
LIMIT 5
"""))

# COMMAND ----------

print(f"Done. Wrote 4 tables under {CATALOG}.{SCHEMA}.{PREFIX}_*")
