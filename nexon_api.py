import aiohttp
from urllib.parse import quote

BASE_URL = "https://open.api.nexon.com/maplestorytw/v1"

class NexonClient:
    def __init__(self, api_key: str, concurrency: int = 3):
        self.api_key = api_key
        self.concurrency = max(1, int(concurrency))

    def _headers(self):
        return {
            "x-nxopen-api-key": self.api_key
        }

    async def _get_json(self, session: aiohttp.ClientSession, url: str):
        try:
            async with session.get(url, headers=self._headers()) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    return None, {"status": resp.status, "body": text, "url": url}
                return await resp.json(), None
        except Exception as e:
            return None, {"status": "exception", "body": str(e), "url": url}

    async def get_ocid(self, session: aiohttp.ClientSession, character_name: str):
        url = f"{BASE_URL}/id?character_name={quote(character_name)}"
        data, err = await self._get_json(session, url)
        return data if data else None

    async def get_basic(self, session: aiohttp.ClientSession, ocid: str):
        url = f"{BASE_URL}/character/basic?ocid={quote(ocid)}"
        data, err = await self._get_json(session, url)
        return data if data else None

    # ✅ 修正：date_str 改成可選；None 表示不帶 date 取得最新快照
    async def get_stat(self, session: aiohttp.ClientSession, ocid: str, date_str: str | None = None):
        if date_str:
            url = f"{BASE_URL}/character/stat?ocid={quote(ocid)}&date={quote(date_str)}"
        else:
            url = f"{BASE_URL}/character/stat?ocid={quote(ocid)}"
        data, err = await self._get_json(session, url)
        return data if data else None

    async def get_guild_id(self, session: aiohttp.ClientSession, guild_name: str, world: str):
        url = f"{BASE_URL}/guild/id?guild_name={quote(guild_name)}&world_name={quote(world)}"
        data, err = await self._get_json(session, url)
        return data if data else None

    async def get_guild_basic(self, session: aiohttp.ClientSession, oguild_id: str):
        url = f"{BASE_URL}/guild/basic?oguild_id={quote(oguild_id)}"
        data, err = await self._get_json(session, url)
        return data if data else None
        
    async def get_item_equipment(
        self,
        session: aiohttp.ClientSession,
        ocid: str,
        date_str: str | None = None
    ):
        if date_str:
            url = f"{BASE_URL}/character/item-equipment?ocid={quote(ocid)}&date={quote(date_str)}"
        else:
            # 不帶 date → 取得最新快照
            url = f"{BASE_URL}/character/item-equipment?ocid={quote(ocid)}"

        data, err = await self._get_json(session, url)
        return data if data else None

