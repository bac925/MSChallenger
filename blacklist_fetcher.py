import math
import re
import time
import requests
from bs4 import BeautifulSoup

BASE = "https://maplestory.beanfun.com"


class BlacklistClient:
    """
    重要：必須使用同一個 requests.Session() 先 GET /blacklist 取得 cookie + token，
    再用同一個 session POST /blacklist?handler=BlockList，否則容易 400。
    """

    def __init__(self, server_name: str = "挑戰者", timeout: int = 20):
        self.server_name = server_name
        self.timeout = timeout
        self.session = requests.Session()
        self.csrf_token = None
        self.server_id = None

    def init(self) -> tuple[str, str]:
        """
        初始化：GET /blacklist 解析 __RequestVerificationToken 與 serverId
        回傳 (csrf_token, server_id)
        """
        html = self._get_blacklist_page_html()
        soup = BeautifulSoup(html, "html.parser")

        # token
        token_inp = soup.find("input", {"name": "__RequestVerificationToken"})
        if token_inp and token_inp.get("value"):
            self.csrf_token = token_inp["value"]
        else:
            m = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', html)
            if not m:
                raise RuntimeError("找不到 __RequestVerificationToken（官網頁面結構可能變更）")
            self.csrf_token = m.group(1)

        # serverId from ddlServer
        ddl = soup.find(id="ddlServer")
        if not ddl:
            raise RuntimeError("找不到 ddlServer（官網頁面結構可能變更）")

        sid = None
        for opt in ddl.find_all("option"):
            txt = (opt.text or "").strip()
            val = (opt.get("value") or "").strip()
            if txt == self.server_name and val:
                sid = val
                break
        if not sid:
            raise RuntimeError(f"找不到伺服器「{self.server_name}」對應的 serverId（官網下拉選單可能變更）")

        self.server_id = sid
        return self.csrf_token, self.server_id

    def _get_blacklist_page_html(self) -> str:
        r = self.session.get(
            f"{BASE}/blacklist",
            timeout=self.timeout,
            headers={"User-Agent": "Mozilla/5.0", "Referer": f"{BASE}/blacklist"},
        )
        r.raise_for_status()
        return r.text

    def fetch_all_for_date(self, block_date_ymd_slash: str, *, sleep_sec: float = 0.25) -> list[dict]:
        """
        抓某一天全部資料（跨頁）
        block_date 格式：YYYY/MM/DD
        """
        if not self.csrf_token or not self.server_id:
            self.init()

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": f"{BASE}/blacklist",
            "Origin": BASE,
            # JS 用的是 X-CSRF-TOKEN
            "X-CSRF-TOKEN": self.csrf_token,
        }

        def post(page: int) -> dict:
            # 有些站會同時驗 header + form field，所以一起帶
            data = {
                "blockDate": block_date_ymd_slash,
                "serverId": self.server_id,
                "page": page,
                "__RequestVerificationToken": self.csrf_token,
            }
            resp = self.session.post(
                f"{BASE}/blacklist?handler=BlockList",
                data=data,
                headers=headers,
                timeout=self.timeout,
            )
            # 若是 400，直接把回應文字帶出來方便你 debug（但不要拋出太長）
            if resp.status_code >= 400:
                txt = (resp.text or "")[:500]
                raise requests.HTTPError(
                    f"{resp.status_code} {resp.reason} for {resp.url} | body[:500]={txt}",
                    response=resp,
                )
            out = resp.json()
            time.sleep(max(0.0, float(sleep_sec)))
            return out

        first = post(1)
        data = first.get("listData") or []
        if not data:
            return []

        total_num = int(data[0].get("totalNum") or 0)
        total_pages = max(1, math.ceil(total_num / 100))

        out = list(data)
        for p in range(2, total_pages + 1):
            d = post(p)
            out.extend(d.get("listData") or [])
        return out


def _is_permanent_unblock_date(unblocked_date_text: str) -> bool:
    """
    永久停權判定：unBlockedDate 年份 >= 2079 視為永久
    """
    s = (unblocked_date_text or "").strip()
    if not s:
        return False
    m = re.match(r"^\s*(\d{4})/(\d{1,2})/(\d{1,2})\s*$", s)
    if not m:
        return False
    y = int(m.group(1))
    return y >= 2079


def extract_names(list_data: list[dict]) -> set[str]:
    names = set()
    for row in list_data:
        nm = (row.get("characterName") or "").strip()
        if nm:
            names.add(nm)
    return names


def extract_permanent_names(list_data: list[dict]) -> set[str]:
    names = set()
    for row in list_data:
        nm = (row.get("characterName") or "").strip()
        ub = (row.get("unBlockedDate") or "").strip()
        if nm and _is_permanent_unblock_date(ub):
            names.add(nm)
    return names
