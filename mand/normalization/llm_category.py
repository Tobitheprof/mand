from __future__ import annotations
import logging, time
from functools import lru_cache
from typing import List, Optional

from mand.config.settings import settings

logger = logging.getLogger(__name__)

PROMPT_TEMPLATE = """
Classificeer dit supermarktproduct in precies één categorie uit de volgende lijst (gebruik exact de naam, geen extra tekst):

Supermarkt: {supermarket}
Product: {title} - {description}

Categorieën:
{category_text}

Algemene regels:
- Kies altijd de meest specifieke categorie.
- Als product eetbaar of drinkbaar is → nooit "Overig".
- Gebruik "Overig" uitsluitend voor non-food producten die niet passen in de categorieën Verzorging of Huishoudelijk.
- Merknamen (bv. Pepsi, Fanta, 7UP, Dr. Pepper, Jägermeister, Almhof) moeten altijd leiden tot de juiste productcategorie, niet tot "Overig".

Specifieke beslisregels:
1) Online aanbiedingen → producten met "OP=OP" in titel wanneer supermarkt alleen Albert Heijn is, "online BONUS" promotie, of expliciet online exclusieve aanbiedingen.
2) Diepvries → bevroren producten (ijs, diepvriespizza, diepvriesgroenten).
3) Baby & Kind → luiers, flesvoeding, babyvoeding, babyverzorging.
4) Huisdier → voer en verzorging voor huisdieren.
5) Alcohol → bier, wijn, sterke drank, likeuren, aperitieven (bijv. Jägermeister, Baileys).
6) Koffie & Thee → koffiebonen, pads, capsules, oploskoffie, thee.
7) Dranken → frisdrank, sap, water, sportdrank, siroop (bijv. Pepsi, Fanta, Crystal Clear, 7UP, Dr. Pepper).
8) Brood & Bakkerij → brood, bolletjes, croissants, gebak, cake, beschuit, crackers, ontbijtgranen (havermout, muesli, cornflakes).
9) Zuivel → melk, yoghurt, kwark, room, boter, slagroom, vla, plantaardige zuivel (havermelk, sojayoghurt).
10) Vleeswaren & Kaas → ham, salami, filet americain, kaas (jong/oud/geraspt), tapas en borrelhapjes, smeerkaas.
11) Vlees & Vis → rauw of bereid vlees en vis, inclusief conserven (niet beleg of kaas).
12) Vegan & Vegetarisch → vleesvervangers en plantaardige alternatieven (tofu, tempeh, falafel, hummus, vegan kaas, vegaburger).
13) Maaltijden → kant-en-klare maaltijden of maaltijdsalades (niet diepvries).
14) Wereldkeuken → internationale ingrediënten, kruiden en sauzen (sojasaus, curry, woksaus, taco's, nori, miso, aioli, pesto, mosterd, olijfolie, azijn).
15) Snacks & Snoep → chips, nootjes, snoep, chocolade, koekjes, repen.
16) Verzorging → shampoo, deodorant, scheerproducten, tandverzorging, vitamines, supplementen, zelfzorgmiddelen, maandverband (Always), oordoppen (Ohropax), kompressen.
17) Huishoudelijk → schoonmaakproducten, wasmiddel, keukenpapier, folie, vuilniszakken, huishoudelijke non-food artikelen (pannen, batterijen, textiel, sokken, beddengoed, pyjama's), planten, bloemen, plantenvoeding.
18) Groente & Fruit → verse, onbewerkte groenten en fruit (niet diepvries of conserven).
19) Overig → alleen non-food producten die nergens anders passen.

Output:
Geef exact één categorienaam uit de lijst, zonder extra tekst, zonder komma’s.
""".strip()


class LLMCategoryAssigner:
    """
    Thin wrapper to call an LLM to classify a product into exactly one category.
    Returns `None` on failure, never raises into callers.
    """

    def __init__(self):
        self.enabled = bool(settings.LLM_CATEGORY_ENABLED and settings.OPENROUTER_API_KEY)
        self.model = settings.LLM_CATEGORY_MODEL
        self.timeout = settings.LLM_CATEGORY_TIMEOUT_S
        self.max_retries = settings.LLM_CATEGORY_MAX_RETRIES

        # Lazy import to avoid hard dependency if feature is off
        self._client = None
        if self.enabled:
            try:
                from openai import OpenAI
                self._client = OpenAI(
                    base_url=settings.OPENROUTER_BASE_URL,
                    api_key=settings.OPENROUTER_API_KEY,
                )
            except Exception as e:
                logger.exception("LLMCategoryAssigner init failed; disabling LLM: %s", e)
                self.enabled = False

    @staticmethod
    def _prepare_categories_text(categories: List[str]) -> str:
        # One per line, exactly as names will be matched by the model.
        return "\n".join(categories)

    @lru_cache(maxsize=settings.LLM_CATEGORY_CACHE_SIZE)
    def classify_cached(self, supermarket: str, title: str, description: str, categories_key: str) -> Optional[str]:
        """
        Cache key is the tuple (supermarket, title, description, categories_key).
        categories_key should be a stable hash or joined string of category names to keep cache correct.
        """
        return self._classify(supermarket, title, description, categories_key)

    def classify(self, supermarket: str, title: str, description: str, categories: List[str]) -> Optional[str]:
        """Public entry: chooses cached path based on categories signature."""
        if not self.enabled:
            return None
        categories_key = "|".join(categories)
        return self.classify_cached(supermarket, title or "", description or "", categories_key)

    def _classify(self, supermarket: str, title: str, description: str, categories_key: str) -> Optional[str]:
        if not self.enabled or self._client is None:
            return None

        # Rebuild categories list from the key
        categories = categories_key.split("|")
        prompt = PROMPT_TEMPLATE.format(
            supermarket=supermarket.strip()[:64],
            title=(title or "").strip()[:256],
            description=(description or "").strip()[:600],
            category_text=self._prepare_categories_text(categories),
        )

        # Robust retries
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self._client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "Je bent een uiterst beknopte, nauwkeurige classificatie-assistent."},
                        {"role": "user", "content": prompt},
                    ],
                    timeout=self.timeout,
                )
                text = (resp.choices[0].message.content or "").strip()
                # Post-process: ensure we return exactly one of the given categories
                for cat in categories:
                    if text == cat:
                        return text
                # If model added extra punctuation/spaces, do simple normalization
                normalized = text.replace("’", "'").replace("`", "'").strip().strip(".").strip()
                for cat in categories:
                    if normalized == cat:
                        return cat
                # As a last-resort best effort, case-insensitive exact match
                lower_map = {c.lower(): c for c in categories}
                if normalized.lower() in lower_map:
                    return lower_map[normalized.lower()]
                logger.warning("LLM returned unmapped category: %r (attempt %d)", text, attempt)
                return None
            except Exception as e:
                logger.warning("LLM classify attempt %d/%d failed: %s", attempt, self.max_retries, e)
                time.sleep(0.6 * attempt)
        return None
