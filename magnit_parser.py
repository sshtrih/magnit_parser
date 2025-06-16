import asyncio
import aiohttp
import csv
import os

from typing import List, Optional, Dict


class MagnitAPI:
    """
    Асинхронный клиент для взаимодействия с API приложения Магнит.
    Отвечает за получение списка товаров и брендов.
    """

    BASE_URL = "https://middle-api.magnit.ru"
    SEARCH_ENDPOINT = f"{BASE_URL}/v2/goods/search"
    PRODUCT_DETAIL_ENDPOINT = f"{BASE_URL}/api/v2/goods"

    HEADERS = {
        "X-Device-Platform": "iOS",
        "X-App-Version": "8.57.0",
    }

    def __init__(
        self, store_code: str, city_id: str = "1", max_concurrent_requests: int = 10
    ):
        self.store_code = store_code
        self.city_id = city_id
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)

    async def fetch_goods_by_category(self, category_id: int) -> List[Dict]:
        offset = 0
        limit = 50
        all_items: List[Dict] = []

        async with aiohttp.ClientSession(headers=self.HEADERS) as session:
            while True:
                payload = {
                    "categories": [category_id],
                    "includeAdultGoods": True,
                    "cityId": self.city_id,
                    "storeType": "1",
                    "pagination": {"limit": limit, "offset": offset},
                    "storeCode": self.store_code,
                    "sort": {"order": "desc", "type": "popularity"},
                    "catalogType": "1",
                }

                async with session.post(self.SEARCH_ENDPOINT, json=payload) as response:
                    response.raise_for_status()
                    data = await response.json()
                    all_items.extend(data.get("items", []))

                    if not data.get("pagination", {}).get("hasMore"):
                        break

                    offset += limit

            return await self._enrich_goods_with_brands(all_items, session)

    async def _enrich_goods_with_brands(
        self, items: List[Dict], session: aiohttp.ClientSession
    ) -> List[Dict]:
        tasks = [self._fetch_brand(item, session) for item in items]
        results = await asyncio.gather(*tasks)

        enriched = [item for item in results if item is not None]
        return enriched

    async def _fetch_brand(
        self, item: Dict, session: aiohttp.ClientSession
    ) -> Optional[Dict]:
        async with self.semaphore:
            product_id = item["id"]
            params = {"catalog-type": "1", "store-type": "1"}
            url = (
                f"{self.PRODUCT_DETAIL_ENDPOINT}/{product_id}/stores/{self.store_code}"
            )

            try:
                async with session.get(url, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    brand = ""
                    for detail in data.get("details", []):
                        if detail.get("name") == "Характеристики":
                            for param in detail.get("parameters", []):
                                if param.get("name") == "Бренд":
                                    brand = param.get("value")
                    return {
                        "id": item["id"],
                        "name": item["name"],
                        "regular_price": self._format_price(
                            item["promotion"].get("oldPrice") or item["price"]
                        ),
                        "promo_price": (
                            self._format_price(item["price"])
                            if item["promotion"].get("discountPercent")
                            else None
                        ),
                        "brand": brand or "",
                    }
            except Exception as e:
                return None

    def _format_price(self, value: Optional[int]) -> Optional[float]:
        return round(value / 100, 2) if value is not None else None


class CSVExporter:
    """
    Отвечает за сохранение списка товаров в CSV-файл.
    """

    def save(self, goods: List[Dict], filename: str) -> None:
        with open(filename, mode="w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["id", "name", "regular_price", "promo_price", "brand"])

            for item in goods:
                writer.writerow(
                    [
                        item["id"],
                        item["name"],
                        item["regular_price"],
                        item["promo_price"] or "",
                        item["brand"] or "",
                    ]
                )


class MagnitParser:
    """
    Основной контроллер парсинга, объединяющий API-клиент и экспортёр.
    """

    def __init__(self, api_client: MagnitAPI, exporter: CSVExporter):
        self.api_client = api_client
        self.exporter = exporter

    async def run(self, category_id: int, output_filename: str) -> None:
        goods = await self.api_client.fetch_goods_by_category(category_id)
        output_path = os.path.join(os.path.dirname(__file__), output_filename)
        self.exporter.save(goods, output_path)


async def main():
    MOSCOW_STORE_CODE = "777022"
    SPB_STORE_CODE = "618535"
    DAIRY_CATEGORY_ID = 4834

    exporter = CSVExporter()

    moscow_api = MagnitAPI(store_code=MOSCOW_STORE_CODE)
    moscow_parser = MagnitParser(api_client=moscow_api, exporter=exporter)
    await moscow_parser.run(
        category_id=DAIRY_CATEGORY_ID, output_filename="moscow_dairy_goods.csv"
    )

    spb_api = MagnitAPI(store_code=SPB_STORE_CODE)
    spb_parser = MagnitParser(api_client=spb_api, exporter=exporter)
    await spb_parser.run(category_id=DAIRY_CATEGORY_ID, output_filename="spb_dairy_goods.csv")


if __name__ == "__main__":
    asyncio.run(main())
