from typing import Dict, List, Tuple, Any, Optional

class InternalCategoryMapper:
    INTERNAL_CATEGORIES: Dict[int, str] = {
        1:  "Groente & Fruit",
        2:  "Vlees & Vis",
        3:  "Vegan & Vegetarisch",
        4:  "Vleeswaren & Kaas",
        5:  "Zuivel",
        6:  "Brood & Bakkerij",
        7:  "Maaltijden",
        8:  "Wereldkeuken",
        9:  "Snacks & Snoep",
        10: "Diepvries",
        11: "Dranken",
        12: "Koffie & Thee",
        13: "Alcohol",
        14: "Verzorging",
        15: "Huishoudelijk",
        16: "Baby & Kind",
        17: "Huisdier",
        18: "Online aanbiedingen",
        19: "Overig",
    }

    RULES: List[Tuple[int, List[str]]] = [
        (1,  ["fruit", "groente", "vers sap", "sappen", "agf", "verse sappen"]),
        (2,  ["vlees", "vis", "gevogelte", "kip", "seafood", "meat", "fish"]),
        (3,  ["vegan", "vegetar", "plant-based"]),
        (4,  ["vleeswaren", "charcut", "kaas", "cheese", "borrelplank"]),
        (5,  ["zuivel", "yoghurt", "kwark", "melk", "dairy"]),
        (6,  ["brood", "bakkerij", "gebak", "bakery"]),
        (7,  ["maaltijd", "salade", "kant-en-klaar", "ready meal", "meal", "pasta", "rijst", "noedels"]),
        (8,  ["wereldkeuken", "aziatisch", "mexicaans", "italiaans", "wereld"]),
        (9,  ["snack", "chips", "snoep", "koek", "chocolade"]),
        (10, ["diepvries", "vries", "frozen"]),
        (11, ["drank", "frisdrank", "sap", "limonade"]),
        (12, ["koffie", "thee", "espresso", "capsules"]),
        (13, ["wijn", "bier", "alcohol", "gedistilleerd"]),
        (14, ["verzorging", "huid", "haar", "douche", "scheer", "personal care"]),
        (15, ["huishoud", "schoonmaak", "wasmiddel", "afwas", "wc", "papier"]),
        (16, ["baby", "kind", "luier", "babyvoeding"]),
        (17, ["huisdier", "kat", "hond", "pet"]),
        (18, ["aanbied", "bonus", "promo", "korting", "actie", "deals", "offers"]),
        (19, ["overig", "diversen", "other", "misc"]),
    ]

    SUPERMARKET_ALIASES: Dict[str, Dict[str, int]] = {
        "ah": {
            "Fruit, verse sappen": 1,
        }
    }

    def map(self, supermarket_id: str, source_name: Optional[str], source_slug: Optional[str] = None) -> Dict[str, Any]:
        text = f"{source_name or ''} {source_slug or ''}".strip().lower()

        if supermarket_id in self.SUPERMARKET_ALIASES and source_name in self.SUPERMARKET_ALIASES[supermarket_id]:
            cid = self.SUPERMARKET_ALIASES[supermarket_id][source_name]
            return {"id": cid, "name": self.INTERNAL_CATEGORIES[cid]}

        for cid, keys in self.RULES:
            for key in keys:
                if key in text:
                    return {"id": cid, "name": self.INTERNAL_CATEGORIES[cid]}

        return {"id": 19, "name": self.INTERNAL_CATEGORIES[19]}
