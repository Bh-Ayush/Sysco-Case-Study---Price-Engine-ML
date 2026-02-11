"""
Sysco Revenue Management — Data Ingestion & Transaction Generation
Parses the real Sysco Arkansas price sheet and generates realistic
transactional history for portfolio-level pricing analysis.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import hashlib

np.random.seed(42)

# ── Real Sysco Price Sheet Data ──────────────────────────────────────────────
# Parsed from the Sysco Arkansas Price Sheet (Effective 4/1/23)
# Contract: S000000035 / 4600049774 — Statewide Groceries

RAW_PRODUCTS = [
    # (contract_item, sysco_item, brand, description, uom, cost)
    (1, 4184552, "SYS CLS", "APPLE BUTTER, #10CAN", "6/#10", 67.02),
    (2, 4015574, "SYS CLS", "APPLE, SLICES, IN WATER, #10CAN", "6/#10", 62.82),
    (3, 4062030, "SYS CLS", "APPLESAUCE, #10CAN", "6/#10", 42.81),
    (4, 1484106, "SYS CLS", "APPLESAUCE, INDIVIDUAL, 4OZ CUP", "72/4 OZ", 32.62),
    (7, 4462107, "SYS REL", "BACON BITS, IMITATION", "12/16 OZ", 32.23),
    (8, 2125421, "CLABGIR", "BAKING SODA, 24/16OZ", "24/16 OZ", 26.91),
    (10, 7599826, "FIELDST", "BAR, FIG, 1.5OZ", "192/1.5 OZ", 50.96),
    (11, 5056757, "NAT VLY", "BAR, GRANOLA, ALMOND, SWEET/SALTY", "128/1.2OZ", 58.95),
    (12, 5523808, "KELLOGG", "BAR, NUTRIGRAIN, APPLE, 1.3OZ", "48/1.3 OZ", 31.00),
    (13, 5523816, "KELLOGG", "BAR, NUTRIGRAIN, BLUEBERRY, 1.3OZ", "48/1.3 OZ", 31.00),
    (14, 5523949, "KELLOGG", "BAR, NUTRIGRAIN, STRAWBERRY, 1.3OZ", "48/1.3 OZ", 31.00),
    (15, 7558299, "KELLOGG", "BAR, RICE KRISPIE TREAT", "4/20 CT", 41.70),
    (18, 4000873, "BUSH", "BEANS, BAKED, #10CAN", "6/#10", 41.51),
    (20, 4062360, "SYS CLS", "BEANS, GREAT NORTHERN, #10CAN", "6/#10", 40.36),
    (22, 4062618, "SYS REL", "BEANS, GREEN, CUT, #10CAN", "6/#10", 34.65),
    (25, 4014973, "SYS CLS", "BEANS, KIDNEY, DARK RED, #10CAN", "6/#10", 39.74),
    (28, 3362274, "CASACLS", "BEANS, PINTO, #10CAN", "6/#10", 37.65),
    (34, 2596831, "SADLERS", "BEEF BBQ, BRISKET, CHOPPED W/SAUCE", "4/5 LB", 105.34),
    (35, 2949097, "ADVANCE", "BEEF PATTY, CHARBROILED, 3OZ", "81/3 OZ", 108.16),
    (38, 2415693, "SYS CLS", "BEEF, CUBED STK, UNBRDED, RAW, 4OZ", "40/4 OZ", 55.05),
    (43, 566838, "FIRECLS", "BEEF, GROUND, 80/20, 10LB", "4/10# AV", 117.20),
    (44, 2473742, "FIRECLS", "BEEF, GROUND, 90/10, 10LB", "2/10 LB", 72.01),
    (46, 1561364, "FIRECLS", "BEEF, PATTY PRECOOKED, 4OZ", "40/4 OZ", 54.70),
    (48, 2417410, "FIRECLS", "BEEF, PATTY, 4OZ", "80/4 OZ", 66.85),
    (50, 6010524, "SYS CLS", "BEEF, POT ROAST W/GRAVY", "2/5 LB", 88.36),
    (58, 4015343, "SYS CLS", "BEETS, SLICED, #10CAN", "6/#10", 38.69),
    (61, 5622873, "PILLSBY", "BREAKFAST, BISCUIT, BTRMILK", "120/2.25OZ", 29.63),
    (63, 2559128, "PILLSBY", "BREAKFAST, BISCUIT, SOUTHERN STYLE", "120/2 OZ", 30.32),
    (65, 2115630, "FRNANDO", "BREAKFAST, BURRITO, SAUS/EGG/CHEESE", "90/3.5 OZ", 64.43),
    (67, 1069822, "RICHS", "BREAKFAST, DONUT, YEAST, GLAZED", "108/1.2 OZ", 43.54),
    (77, 5397734, "BAKCRFT", "BREAKFAST, PANCAKE, HEAT & SERVE", "144/1.3 OZ", 22.83),
    (84, 1826254, "SYS CLS", "BREAKFAST, WAFFLE, BELGIAN, 4\"", "72/2.4OZ", 37.83),
    (86, 6988265, "SYS REL", "BROCCOLI, CUT, FRZ", "12/2.5 LB", 35.39),
    (87, 6743058, "SYS REL", "BROCCOLI, SPEAR, FRZ", "12/2 LB", 33.47),
    (88, 5568241, "SYS CLS", "BROTH, CHICKEN, 49OZ", "12/49 OZ", 32.03),
    (90, 1334911, "B/BOY", "BURRITO, BEEF&BEAN, PREFRIED", "60/5.5 OZ", 52.66),
    (94, 3030816, "WHLFIMP", "BUTTER, NOT OLEO MARGARINE, 1LB", "36/1 LB", 117.45),
    (101, 1913819, "GM", "CEREAL, APP/CIN CHEERIOS", "96/1 OZ", 22.79),
    (104, 2177584, "GM", "CEREAL, CHEERIOS", "96/1 OZ", 22.79),
    (107, 4044640, "KELLOGG", "CEREAL, CORN FLAKES, .75OZ", "96/3/4 OZ", 48.12),
    (117, 4044558, "GM", "CEREAL, HONEYNUT CHEERIOS", "96/1 OZ", 22.79),
    (120, 5007448, "HOSPTLY", "CEREAL, OATMEAL HOT, QUICK", "12/42 OZ", 51.44),
    (132, 6697890, "BBRLCLS", "CHEESE, AMER, PROCSSD, SLICE", "4/5 LB", 39.13),
    (137, 8315482, "GR LAKE", "CHEESE, MOZZARELLA, SHREDDED", "4/5 LB", 57.89),
    (141, 5103064, "BBRLCLS", "CHEESE, SWISS, PASTURZ, SLICED", "4/5 LB", 39.70),
    (145, 7680352, "TYSON", "CHICKEN, 8PC, CUT, 5OZ", "96/5 OZ", 79.55),
    (149, 8488829, "TYSONRL", "CHICKEN, BREAST, FILLET, BREADED", "2/5 LB", 31.87),
    (150, 9562877, "SYS CLS", "CHICKEN, BREAST, SKINLESS, RAW, 5OZ", "48/5 OZ", 46.48),
    (155, 2157315, "TYSON", "CHICKEN, DICED", "2/5 LB", 51.09),
    (164, 3962727, "SYS CLS", "CHICKEN, WING, BRD, PRECKD", "3/5 LB", 31.89),
    (167, 6381982, "TYSON", "CHICKEN, BREAST NUGGET", "250/.67 OZ", 27.87),
    (168, 4182150, "SYS CLS", "CHILI CON CARNE W/BEANS, #10CAN", "6/#10", 74.14),
    (170, 4360762, "CHEETOS", "CHIPS, CHEESE, CRUNCHY BAKED", "104/.875OZ", 44.77),
    (175, 4073847, "FRITOS", "CHIPS, CORN, ORIGINAL, 16OZ", "8/16 OZ", 20.15),
    (183, 2077345, "BBRLCLS", "CHIPS, POTATO, REGULAR, 1LB", "9/1 LB", 28.22),
    (187, 9550674, "CASACLS", "CHIPS, TORTILLA, CORN, WHT, RND", "6/2 LB", 36.42),
    (193, 4135380, "SYS CLS", "COATING, PAN, AEROSOL, 17OZ", "6/17 OZ", 18.71),
    (200, 7887041, "CITVCLS", "COFFEE, REG, 2LB", "6/2 LB", 86.51),
    (206, 4125852, "LORNA D", "COOKIES, LORNA DOONE", "120/1 OZ", 24.06),
    (210, 4341903, "NABISCO", "COOKIES, OREO SANDWICH, 1.14OZ", "120/4 CT", 35.48),
    (227, 4107520, "SYS CLS", "CORN, WHOLE KERNEL, GOLDEN, #10CAN", "6/#10", 34.65),
    (239, 6056105, "LANCE", "CRACKER, SALTINE", "500/2 PK", 14.49),
    (242, 5538590, "SUNSHIN", "CRACKERS, CHEEZITS", "60/1.5 OZ", 26.28),
    (249, 3412424, "WHLFCLS", "ICE CREAM, CHOCOLATE CHP", "48/4 OZ", 21.68),
    (251, 1972744, "SYS CLS", "PIE, APPLE, RTB, 10\"", "6/46 OZ", 34.83),
    (268, 1014786, "SARALEE", "CAKE, POUND", "12/16 OZ", 67.27),
    (275, 4523460, "PILLSBY", "DOUGH, BISCUIT, GARLIC/CHDR", "210/1.2 OZ", 26.67),
    (289, 7065584, "SYS CLS", "DOUGH, ROLL, PKRHOUSE", "288/1.2 OZ", 24.57),
    (294, 4537955, "SYS REL", "DRESSING, 1000 ISLAND, 1 GAL", "4/1 GAL", 60.64),
    (300, 4538874, "SYS CLS", "DRESSING, FRENCH, 1 GAL", "4/1 GAL", 49.91),
    (304, 4537607, "SYS REL", "DRESSING, RANCH BUTTRMLK, 1 GAL", "4/1 GAL", 43.01),
    (329, 2592012, "PAPETTI", "EGGS, COOKED, DICED", "4/5 LB", 60.61),
    (333, 2105823, "WHLFCLS", "EGGS, WHITE SHELL, GRD A LARGE", "1/15 DZ", 59.41),
    (343, 4646105, "FRSHWTR", "FISH, CATFISH, FILLETS", "1/10 LB", 59.89),
    (350, 7077655, "PORTCLS", "FISH, PANGASIUS, FILET", "1/15 LB", 35.28),
    (354, 1002643, "PORTSIM", "FISH, TILAPIA, FILLET, SKINLESS", "2/5 LB", 62.82),
    (355, 8379251, "SYS CLS", "FLOUR, H&R ALL PURPOSE, 25LBS", "1/25 LB", 11.04),
    (359, 4604120, "SYS CLS", "FRUIT COCKTAIL IN JUICE #10CAN", "6/#10", 58.31),
    (438, 4113361, "SYS REL", "KETCHUP, #10CAN", "6/#10", 32.45),
    (441, 8747859, "HSRCIMP", "KETCHUP, 9G PACKET", "1000/9 GM", 28.03),
    (449, 4002432, "SYS REL", "MAYONNAISE, HEAVY DUTY, 1GAL", "4/1 GAL", 52.30),
    (456, 4036380, "CARNATN", "MILK, EVAPORATED, #10", "6/#10", 83.61),
    (482, 4730552, "GILSTER", "MIX, PUDDING, BANANA", "12/24OZ", 28.72),
    (488, 5072137, "AREZCLS", "MUSHROOM, STEM&PIECES, #10CAN", "6/#10", 42.71),
    (493, 6571228, "RED BOY", "MUSTARD, YELLOW, 1 GAL", "4/1 GAL", 17.02),
    (495, 4119095, "SYS CLS", "OIL, CORN, 1 GALLON", "6/1 GAL", 95.97),
    (507, 5204544, "AREZCLS", "PASTA, ELBOW, MACARONI", "2/10 LB", 21.76),
    (513, 5204597, "AREZCLS", "PASTA, SPAGHETTI", "2/10 LB", 23.04),
    (515, 4113650, "SYS REL", "PEA, EARLY GREEN, #10CAN", "6/#10", 46.32),
    (524, 4009189, "SYS CLS", "PEANUT BUTTER, CREAMY, 5LB", "6/5 LB", 53.46),
    (559, 4087771, "SYS SUP", "PINEAPPLE, CHUNK, IN JUICE, #10CAN", "6/#10", 34.27),
    (575, 1265537, "SYS REL", "PORK, BACON BULK LAYFLAT, SLICED", "1/15 LB", 54.34),
    (580, 1044718, "RICHS", "PORK, BBQ W/SAUCE", "4/5 LB", 103.30),
    (586, 1247105, "HORMEL", "PORK, HAM, FULLY COOKED, 10LB", "4/10 LB", 134.76),
    (590, 1116532, "SYS CLS", "PORK, SAUSAGE PATTY, RAW, MILD", "80/2 OZ", 27.62),
    (594, 1440825, "SIMPLOT", "POTATO, FRY, CRINKLE CUT 1/2\"", "6/5 LB", 26.40),
    (595, 1995051, "SYS CLS", "POTATO, FRY, CRINKLE CUT 3/8\"", "6/5LB", 38.00),
    (601, 726127, "IDAHOAN", "POTATO, MASHED, INSTANT", "12/26 OZ", 74.74),
    (602, 3677739, "SYS CLS", "POTATO, MASHED, INSTANT, 5.5LB", "6/5.31LB", 80.33),
    (609, 1994563, "SYS CLS", "POTATO, STRAIGHT CUT, 3/8\"", "6/5LB", 38.28),
    (612, 5246846, "SIMPLOT", "POTATO, TATER TOT", "6/5 LB", 31.70),
    (649, 4295374, "SYS CLS", "RICE, LONG GRAIN, 25LBS", "1/25 LB", 12.33),
    (651, 4671350, "SYS IMP", "RICE, PARBOILED, 25LBS", "1/25 LB", 14.90),
    (662, 4540373, "SYS CLS", "SALT, GRANULATED, IODIZED, 25LBS", "1/25 LB", 6.28),
    (667, 4008355, "SYS REL", "SAUCE, BARBEQUE, SMOKY, 1 GALLON", "4/1 GAL", 45.72),
    (669, 389348, "SYS CLS", "SAUCE, CHEESE, CHEDDAR, #10CAN", "6/#10", 66.96),
    (672, 4005567, "KIKKOMAN", "SAUCE, SOY, 1 GAL", "4/ 1 gal", 52.20),
    (673, 4189361, "SYS CLS", "SAUCE, SPAGHETTI MARINRA, #10CAN", "6/10#", 33.70),
    (700, 4944567, "SYS CLS", "SOUP, BASE, CHICKEN, 16OZ", "6/1 LB", 17.83),
    (703, 4104402, "CAMPBEL", "SOUP, CHICKEN, NOODLE, 12/49.5OZ", "12/50 OZ", 53.36),
    (709, 4040390, "CAMPBEL", "SOUP, TOMATO, 51OZ", "12/50 OZ", 44.78),
    (662, 4540373, "SYS CLS", "SALT, GRANULATED, IODIZED, 25LBS", "1/25 LB", 6.28),
    (739, 4782694, "SYS CLS", "SUGAR GRANULATED XFINE", "1/50 LB", 39.00),
    (740, 5087572, "SYS CLS", "SUGAR, GRANULATED, EXTRA FINE, 25LBS", "1/25 LB", 19.88),
    (762, 5370952, "SYS CLS", "SYRUP, SORGHAM, 1 GALLON", "4/1 GAL", 61.13),
    (764, 6046643, "MISSION", "TACO SHELL, REGULAR, 5\"", "8/25 CT", 24.90),
    (772, 5096466, "SYS IMP", "TOMATO, DICED, IN JUICE, #10CAN", "6/10#", 28.61),
    (786, 8682692, "PORTCLS", "TUNA, CHUNK, SKIPJCK, LITE, IN WATR", "6/66.5OZ", 80.37),
    (788, 4897468, "JENNIEO", "TURKEY, BREAST STEAK, 4OZ", "40/4OZ", 57.12),
    (795, 7268279, "SYS CLS", "TURKEY, GROUND, MECH SEPARATED", "4/5 LB", 28.07),
    (800, 2578680, "SYS REL", "VEAL, FRITTER, BREADED, 4OZ", "40/4 OZ", 53.36),
    (804, 1474964, "SYS CLS", "VEGETABLES, FROZEN, CA BLEND", "12/2LB", 26.40),
    (812, 4112926, "SYS CLS", "VEGETABLES, MIXED, #10CAN", "6/#10", 40.58),
]

# ── Category Assignment ──────────────────────────────────────────────────────
CATEGORY_MAP = {
    "APPLE": "Canned Fruit", "APPLESAUCE": "Canned Fruit", "APRICOT": "Canned Fruit",
    "PEACH": "Canned Fruit", "PEAR": "Canned Fruit", "PINEAPPLE": "Canned Fruit",
    "FRUIT": "Canned Fruit", "STRAWBERRY": "Canned Fruit",
    "BACON": "Protein - Pork", "PORK": "Protein - Pork", "HAM": "Protein - Pork",
    "SAUSAGE": "Protein - Pork",
    "BEEF": "Protein - Beef", "MEATBALL": "Protein - Beef", "VEAL": "Protein - Beef",
    "SALISBURY": "Protein - Beef",
    "CHICKEN": "Protein - Poultry", "TURKEY": "Protein - Poultry",
    "FISH": "Protein - Seafood", "TUNA": "Protein - Seafood", "SHRIMP": "Protein - Seafood",
    "CHEESE": "Dairy", "BUTTER": "Dairy", "MILK": "Dairy", "EGGS": "Dairy",
    "CREAM": "Dairy", "YOGURT": "Dairy", "ICE CREAM": "Frozen Desserts",
    "CEREAL": "Breakfast", "BREAKFAST": "Breakfast", "PANCAKE": "Breakfast",
    "WAFFLE": "Breakfast", "POP-TART": "Breakfast", "OATMEAL": "Breakfast",
    "BEANS": "Canned Vegetables", "CORN": "Canned Vegetables", "PEA": "Canned Vegetables",
    "TOMATO": "Canned Vegetables", "BEETS": "Canned Vegetables",
    "VEGETABLE": "Frozen Vegetables", "BROCCOLI": "Frozen Vegetables",
    "SPINACH": "Frozen Vegetables", "SQUASH": "Frozen Vegetables",
    "POTATO": "Potatoes & Sides", "RICE": "Potatoes & Sides", "PASTA": "Potatoes & Sides",
    "CHIPS": "Snacks", "CRACKER": "Snacks", "COOKIES": "Snacks", "BAR": "Snacks",
    "GOLDFISH": "Snacks",
    "DRESSING": "Condiments", "KETCHUP": "Condiments", "MUSTARD": "Condiments",
    "SAUCE": "Condiments", "MAYONNAISE": "Condiments", "RELISH": "Condiments",
    "JELLY": "Condiments", "PEANUT BUTTER": "Condiments",
    "SOUP": "Soup & Broth", "BROTH": "Soup & Broth", "CHILI": "Soup & Broth",
    "PIZZA": "Prepared Entrees", "BURRITO": "Prepared Entrees", "RAVIOLI": "Prepared Entrees",
    "TACO": "Prepared Entrees", "ENTREE": "Prepared Entrees",
    "COFFEE": "Beverages", "JUICE": "Beverages", "DRINK": "Beverages",
    "SODA": "Beverages", "TEA": "Beverages", "WATER": "Beverages",
    "FLOUR": "Baking & Dry Goods", "SUGAR": "Baking & Dry Goods", "OIL": "Baking & Dry Goods",
    "SALT": "Baking & Dry Goods", "SPICE": "Baking & Dry Goods", "SEASONING": "Baking & Dry Goods",
    "MIX": "Baking & Dry Goods", "SHORTENING": "Baking & Dry Goods",
    "DOUGH": "Bakery", "ROLL": "Bakery", "LOAF": "Bakery", "BREADSTICK": "Bakery",
    "PIE": "Frozen Desserts", "CAKE": "Frozen Desserts", "DESSERT": "Frozen Desserts",
    "COBBLER": "Frozen Desserts", "PUDDING": "Frozen Desserts",
    "SUPPLEMENT": "Nutritional", "THICKENER": "Nutritional", "PUREE": "Nutritional",
}


def assign_category(desc):
    desc_upper = desc.upper()
    for keyword, cat in CATEGORY_MAP.items():
        if keyword in desc_upper:
            return cat
    return "Other"


# ── Customer Segment Definitions ─────────────────────────────────────────────
CUSTOMER_SEGMENTS = {
    "Healthcare": {
        "count": 18,
        "prefix": "HC",
        "names": [
            "Baptist Health System", "Mercy Hospital NW Arkansas",
            "CHI St. Vincent", "Arkansas Children's Hospital",
            "Washington Regional Medical", "UAMS Medical Center",
            "Sparks Health System", "NEA Baptist Memorial",
            "White River Medical Center", "North Arkansas Regional",
            "Baxter Regional Medical", "Ouachita County Medical",
            "DeWitt Hospital", "Piggott Community Hospital",
            "Crossridge Community Hospital", "Ozark Health Medical",
            "Delta Memorial Hospital", "Saline Memorial Hospital"
        ],
        "volume_multiplier": 1.4,
        "price_sensitivity": 0.15,
        "margin_target": 0.22,
        "basket_breadth": 0.85,
    },
    "K-12 Education": {
        "count": 22,
        "prefix": "ED",
        "names": [
            "Little Rock School District", "Bentonville Public Schools",
            "Fort Smith Public Schools", "Springdale School District",
            "Rogers School District", "Fayetteville Public Schools",
            "Conway School District", "Jonesboro Public Schools",
            "Cabot School District", "Bryant School District",
            "Jacksonville North Pulaski", "Van Buren School District",
            "Russellville School District", "Searcy School District",
            "Pine Bluff School District", "Paragould School District",
            "Siloam Springs Schools", "Mountain Home Schools",
            "Vilonia School District", "Greenwood School District",
            "Alma School District", "Beebe School District"
        ],
        "volume_multiplier": 1.0,
        "price_sensitivity": 0.35,
        "margin_target": 0.16,
        "basket_breadth": 0.70,
    },
    "Restaurant/FSR": {
        "count": 15,
        "prefix": "RS",
        "names": [
            "Whole Hog Cafe (Multi-Unit)", "Doe's Eat Place",
            "Tusk & Trotter", "Brave New Restaurant",
            "The Hive at 21C", "Pressroom Little Rock",
            "Petit & Keet", "Arthur's Prime Steakhouse",
            "AQ Chicken House", "Hammontree's Grilled Cheese",
            "The Venesian Inn", "Franke's Cafeteria Group",
            "Western Sizzlin' (Franchisee)", "CJ's Butcher Boy Burgers",
            "Catfish Hole (Multi-Unit)"
        ],
        "volume_multiplier": 0.7,
        "price_sensitivity": 0.25,
        "margin_target": 0.24,
        "basket_breadth": 0.50,
    },
    "Corrections/Government": {
        "count": 10,
        "prefix": "GV",
        "names": [
            "AR Dept of Correction - Cummins", "AR Dept of Correction - Tucker",
            "AR Dept of Correction - Varner", "AR Dept of Correction - Grimes",
            "Pulaski County Detention", "Benton County Jail",
            "Washington County Detention", "Craighead County Detention",
            "AR National Guard - Camp Robinson", "AR Veterans Home - Fayetteville"
        ],
        "volume_multiplier": 2.0,
        "price_sensitivity": 0.40,
        "margin_target": 0.14,
        "basket_breadth": 0.60,
    },
    "Senior Living": {
        "count": 12,
        "prefix": "SL",
        "names": [
            "Butterfield Trail Village", "Pleasant Valley Senior Living",
            "Legacy Village of Jacksonville", "Woodland Hills Senior",
            "The Gardens at Osage Terrace", "Brookdale Little Rock",
            "Cedar Lodge Nursing", "Green House Cottages Fayetteville",
            "AR Health Center", "Russellville Nursing & Rehab",
            "Briarwood Nursing", "Heritage Living Center"
        ],
        "volume_multiplier": 0.9,
        "price_sensitivity": 0.20,
        "margin_target": 0.20,
        "basket_breadth": 0.75,
    },
}


def build_product_catalog():
    """Build product catalog from real Sysco pricing data."""
    rows = []
    seen = set()
    for item in RAW_PRODUCTS:
        cid, sysco_id, brand, desc, uom, cost = item
        key = (sysco_id, desc)
        if key in seen:
            continue
        seen.add(key)
        category = assign_category(desc)
        # Determine if commodity vs non-commodity
        commodity_keywords = ["BEEF", "CHICKEN", "PORK", "TURKEY", "CHEESE",
                              "BUTTER", "EGGS", "OIL", "FLOUR", "SUGAR", "RICE",
                              "MILK", "POTATO"]
        is_commodity = any(k in desc.upper() for k in commodity_keywords)

        rows.append({
            "product_id": f"SKU-{sysco_id:07d}",
            "sysco_item": sysco_id,
            "contract_item": cid,
            "brand": brand,
            "description": desc,
            "unit_of_measure": uom,
            "base_cost": cost,
            "category": category,
            "is_commodity": is_commodity,
            "pricing_tier": np.random.choice(
                ["Tier 1 - Strategic", "Tier 2 - Preferred", "Tier 3 - Standard"],
                p=[0.25, 0.45, 0.30]
            ),
        })
    return pd.DataFrame(rows)


def generate_customers():
    """Generate realistic customer base across segments."""
    customers = []
    cust_id = 1000
    for seg_name, seg_config in CUSTOMER_SEGMENTS.items():
        for i, name in enumerate(seg_config["names"]):
            cust_id += 1
            customers.append({
                "customer_id": f"{seg_config['prefix']}-{cust_id}",
                "customer_name": name,
                "segment": seg_name,
                "volume_multiplier": seg_config["volume_multiplier"] * np.random.uniform(0.7, 1.3),
                "price_sensitivity": seg_config["price_sensitivity"] * np.random.uniform(0.8, 1.2),
                "gp_target": seg_config["margin_target"],
                "basket_breadth": seg_config["basket_breadth"],
                "account_tier": np.random.choice(
                    ["National", "Regional", "Local"],
                    p=[0.15, 0.35, 0.50]
                ),
                "credit_rating": np.random.choice(["A", "B", "C"], p=[0.5, 0.35, 0.15]),
                "annual_revenue_est": round(np.random.lognormal(11.5, 0.8), 2),
            })
    return pd.DataFrame(customers)


def generate_transactions(products_df, customers_df, weeks=16):
    """
    Generate 16 weeks of transactional data simulating real ordering patterns.
    Includes cost fluctuations, seasonal effects, and customer-level variation.
    """
    transactions = []
    start_date = datetime(2025, 10, 6)  # 16 weeks back from ~Feb 2026

    for week_num in range(weeks):
        week_start = start_date + timedelta(weeks=week_num)

        # Simulate cost changes (commodity volatility)
        cost_shock = 1.0
        if week_num == 6:
            cost_shock = 1.035   # 3.5% cost increase in week 7 (the "lever change")
        elif week_num == 7:
            cost_shock = 1.042
        elif week_num >= 8:
            cost_shock = 1.04 + np.random.uniform(-0.005, 0.005)

        for _, cust in customers_df.iterrows():
            # Each customer orders a subset of products each week
            n_products = int(len(products_df) * cust["basket_breadth"] * np.random.uniform(0.6, 1.0))
            ordered_products = products_df.sample(n=min(n_products, len(products_df)))

            for _, prod in ordered_products.iterrows():
                # Volume: cases per week
                base_vol = np.random.poisson(lam=3) * cust["volume_multiplier"]
                if base_vol == 0:
                    base_vol = 1

                # Seasonal adjustment
                seasonal = 1.0
                if prod["category"] in ["Soup & Broth", "Breakfast"] and week_num > 10:
                    seasonal = 1.15  # winter boost
                if prod["category"] in ["Beverages", "Frozen Desserts"] and week_num < 4:
                    seasonal = 1.10  # early fall warmth

                volume = max(1, int(base_vol * seasonal))

                # Cost with commodity shock applied to commodity items
                actual_cost = prod["base_cost"]
                if prod["is_commodity"] and week_num >= 6:
                    actual_cost = prod["base_cost"] * cost_shock

                # List price = cost + target margin
                target_margin = cust["gp_target"]
                list_price = actual_cost / (1 - target_margin - 0.05)

                # Net price after customer-specific adjustments
                # Some customers have negotiated discounts
                discount_pct = np.random.uniform(0, 0.08) * cust["price_sensitivity"]
                net_price = list_price * (1 - discount_pct)

                # Existing override? (random ~12% of transactions have overrides)
                has_override = np.random.random() < 0.12
                override_price = None
                if has_override:
                    override_direction = np.random.choice(["up", "down"], p=[0.3, 0.7])
                    if override_direction == "down":
                        override_price = net_price * np.random.uniform(0.92, 0.98)
                    else:
                        override_price = net_price * np.random.uniform(1.01, 1.06)
                    net_price = override_price

                # Calculate financials
                net_sales = round(net_price * volume, 2)
                total_cost = round(actual_cost * volume, 2)
                gross_profit = round(net_sales - total_cost, 2)
                gp_pct = round(gross_profit / net_sales, 4) if net_sales > 0 else 0

                transactions.append({
                    "transaction_id": hashlib.md5(
                        f"{week_num}-{cust['customer_id']}-{prod['product_id']}".encode()
                    ).hexdigest()[:12],
                    "week_number": week_num + 1,
                    "week_start": week_start.strftime("%Y-%m-%d"),
                    "customer_id": cust["customer_id"],
                    "customer_name": cust["customer_name"],
                    "segment": cust["segment"],
                    "product_id": prod["product_id"],
                    "description": prod["description"],
                    "category": prod["category"],
                    "brand": prod["brand"],
                    "is_commodity": prod["is_commodity"],
                    "cases_ordered": volume,
                    "unit_cost": round(actual_cost, 2),
                    "list_price": round(list_price, 2),
                    "net_price": round(net_price, 2),
                    "has_override": has_override,
                    "override_price": round(override_price, 2) if override_price else None,
                    "net_sales": net_sales,
                    "cogs": total_cost,
                    "gross_profit_dollars": gross_profit,
                    "gp_pct": gp_pct,
                    "pricing_tier": prod["pricing_tier"],
                })

    return pd.DataFrame(transactions)


if __name__ == "__main__":
    print("Building product catalog from Sysco price sheet...")
    products = build_product_catalog()
    print(f"  → {len(products)} unique products across {products['category'].nunique()} categories")

    print("Generating customer base...")
    customers = generate_customers()
    print(f"  → {len(customers)} customers across {customers['segment'].nunique()} segments")

    print("Generating 16 weeks of transaction history...")
    txns = generate_transactions(products, customers, weeks=16)
    print(f"  → {len(txns):,} transaction records generated")
    print(f"  → Total net sales: ${txns['net_sales'].sum():,.0f}")
    print(f"  → Average GP%: {txns['gp_pct'].mean():.1%}")

    # Save intermediate outputs
    products.to_csv("/home/claude/pricing_engine/products.csv", index=False)
    customers.to_csv("/home/claude/pricing_engine/customers.csv", index=False)
    txns.to_csv("/home/claude/pricing_engine/transactions.csv", index=False)
    print("\nData saved to /home/claude/pricing_engine/")
