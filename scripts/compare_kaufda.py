"""Compare our search results against KaufDA live API for PLZ 53113."""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from collections import defaultdict
from app.connectors.kaufda import KaufdaOffersSeoConnector, KaufdaLocation

SEARCH_TERMS = [
    "Milch", "Butter", "Brot", "Käse", "Joghurt", "Eier",
    "Chips", "Huhn", "Hähnchen", "Wurst", "Schinken",
    "Nudeln", "Reis", "Tomaten", "Bananen", "Äpfel",
    "Cola", "Bier", "Wasser", "Schokolade",
    "Kaffee", "Zucker", "Mehl", "Kartoffeln",
    "Lachs", "Thunfisch", "Pizza", "Tiefkühlpizza",
    "Zahnpasta", "Toilettenpapier",
]

OUR_API = "http://127.0.0.1:8000/api/search"
PLZ = "53113"


async def search_ours(term: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(OUR_API, params={"q": term, "location": PLZ, "limit": 200})
        data = resp.json()
        return data


async def search_kaufda(connector: KaufdaOffersSeoConnector, term: str) -> list:
    try:
        offers = await connector.fetch_search_offers(keyword=term)
        return offers
    except Exception as e:
        print(f"  KaufDA error for '{term}': {e}")
        return []


async def main():
    connector = KaufdaOffersSeoConnector(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) Sparfuchs-Test/1.0",
        location=KaufdaLocation(lat=50.7206, lng=7.1187, city="Bonn", zip="53113"),
    )

    print(f"{'Query':<20} {'Ours':>6} {'KaufDA':>7} {'Chains_Ours':>12} {'Chains_KD':>10} {'Missing Chains'}")
    print("=" * 90)

    total_ours = 0
    total_kaufda = 0
    all_missing_chains = defaultdict(int)

    for term in SEARCH_TERMS:
        ours_data, kd_offers = await asyncio.gather(
            search_ours(term),
            search_kaufda(connector, term),
        )

        our_count = ours_data.get("total", 0)
        kd_count = len(kd_offers)

        our_chains = set(ours_data.get("available_chains", []))
        kd_chains = set(o.chain for o in kd_offers)

        missing = kd_chains - our_chains
        for c in missing:
            all_missing_chains[c] += 1

        missing_str = ", ".join(sorted(missing)) if missing else "-"

        marker = ""
        if our_count == 0 and kd_count > 0:
            marker = " !!!"
        elif kd_count > 0 and our_count < kd_count * 0.3:
            marker = " !"

        total_ours += our_count
        total_kaufda += kd_count

        print(f"{term:<20} {our_count:>6} {kd_count:>7} {len(our_chains):>12} {len(kd_chains):>10} {missing_str}{marker}")

        # Rate limit for KaufDA
        await asyncio.sleep(0.5)

    print("=" * 90)
    print(f"{'TOTAL':<20} {total_ours:>6} {total_kaufda:>7}")
    print(f"\nChains missing from our results (count of queries):")
    for chain, count in sorted(all_missing_chains.items(), key=lambda x: -x[1]):
        print(f"  {chain}: {count}/{len(SEARCH_TERMS)} queries")


if __name__ == "__main__":
    asyncio.run(main())
