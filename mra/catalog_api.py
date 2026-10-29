"""Direct, read-only catalog clients. Only the listed retrieval routes are allowed."""

from __future__ import annotations

import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request

from .audit import redact
from .extract import normalize_article
from .rate_limits import retry_seconds

HOSTS = {"wildberries": "https://content-api.wildberries.ru", "ozon": "https://api-seller.ozon.ru",
         "yandex_market": "https://api.partner.market.yandex.ru"}


class CatalogError(ValueError):
    pass


def access_issue(body):
    text = json.dumps(body, ensure_ascii=False).lower() if body else ""
    if "token withdrawn" in text or "token is revoked" in text:
        return "revoked"
    if "api-key is deactivated" in text:
        return "deactivated"
    return ""


def load_accounts(path):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.get("schema_version") != 1 or not isinstance(data.get("accounts"), list):
        raise CatalogError("ожидается accounts JSON schema_version=1")
    seen, result = set(), []
    for row in data["accounts"]:
        if not isinstance(row, dict) or row.get("marketplace") not in HOSTS:
            raise CatalogError("неизвестная площадка в настройках кабинета")
        if not all(isinstance(row.get(k), str) and row[k].strip() for k in ("account_id", "name")):
            raise CatalogError("кабинету нужны account_id и name")
        if row["account_id"] in seen:
            raise CatalogError("account_id повторяется")
        if any(key in row for key in ("api_key", "token", "authorization", "password")):
            raise CatalogError("настройки содержат секрет; используйте имя переменной окружения или путь к файлу")
        seen.add(row["account_id"])
        result.append(row)
    return result


def runtime_value(account, name):
    env = account.get(name + "_env")
    value = os.environ.get(env, "").strip() if env else ""
    if not value and account.get(name + "_file"):
        try:
            value = Path(account[name + "_file"]).expanduser().read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError):
            raise CatalogError("не удалось прочитать настройку " + name) from None
    if not value and name == "client_id" and str(account.get(name, "")).isdigit():
        value = str(account[name])
    return value


def listing(identifier, article, title="", *, aliases=(), status="listed"):
    if not isinstance(identifier, (str, int)) or isinstance(identifier, bool) or not str(identifier).strip():
        raise CatalogError("в карточке отсутствует идентификатор")
    if not isinstance(article, str) or not article.strip():
        raise CatalogError("в карточке отсутствует артикул продавца")
    if not normalize_article(article):
        raise CatalogError("после нормализации артикул пуст")
    return {"listing_id": str(identifier), "article": normalize_article(article), "seller_article": article,
            "title": str(title or ""), "status": status,
            "aliases": sorted(set((str(kind), str(value)) for kind, value in aliases if value is not None and str(value)))}


class CatalogClient:
    def __init__(self, account, *, transport=None, pause=time.sleep, max_pages=100, audit=None, limiter=None):
        self.account, self.transport, self.pause = account, transport, pause
        self.max_pages, self.calls, self.audit = max_pages, 0, audit
        self.limiter = limiter
        if type(max_pages) is not int or max_pages < 1:
            raise CatalogError("max_pages должен быть положительным")

    def request(self, method, path, payload=None, query=None):
        mp = self.account["marketplace"]
        allowed = ((mp == "wildberries" and method == "POST" and path == "/content/v2/get/cards/list") or
                   (mp == "ozon" and method == "POST" and path in {"/v3/product/list", "/v3/product/info/list"}) or
                   (mp == "yandex_market" and ((method == "GET" and path == "/v2/campaigns") or
                    (method == "POST" and path.startswith("/v2/businesses/") and path.endswith("/offer-mappings")
                     and path.split("/")[3].isdigit()))))
        if not allowed:
            raise CatalogError("маршрут не входит в список разрешённых чтений каталога")
        if self.calls >= self.max_pages:
            raise CatalogError("достигнут предел запросов; каталог не подтверждён полностью")
        if self.calls and (self.transport or self.limiter is None):
            self.pause(1.1)
        self.calls += 1
        if self.audit:
            self.audit("catalog.request.started", {"method": method, "path": path, "query": query, "body": payload, "call": self.calls})
        if self.transport:
            body = self.transport(method, path, payload, query)
        else:
            key = runtime_value(self.account, "api_key")
            if not key:
                raise CatalogError("не настроен ключ кабинета")
            headers = {"Content-Type": "application/json"}
            if mp == "wildberries":
                headers["Authorization"] = key
            elif mp == "ozon":
                client_id = runtime_value(self.account, "client_id")
                if not client_id:
                    raise CatalogError("не настроен Client-Id Ozon")
                headers.update({"Api-Key": key, "Client-Id": client_id})
            else:
                headers["Api-Key"] = key
            url = HOSTS[mp] + path + ("?" + urllib.parse.urlencode(query) if query else "")
            request = urllib.request.Request(url, headers=headers, method=method,
                data=json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None)
            if self.limiter is None:
                raise CatalogError("клиент каталога запущен без общего ограничителя кабинета")
            token, wait = self.limiter.acquire(method, path)
            cooldown, status, response_headers = 0, None, {}
            try:
                if wait:
                    if self.audit:
                        self.audit("catalog.rate_wait", {"seconds": wait})
                    self.pause(wait)
                with urllib.request.urlopen(request, timeout=45) as response:
                    raw = response.read(8_000_001)
                    status, response_headers = response.status, dict(response.headers)
                if len(raw) > 8_000_000:
                    raise CatalogError("страница каталога превышает 8 МБ")
                body = redact(json.loads(raw.decode("utf-8")), secrets=(key,))
            except urllib.error.HTTPError as exc:
                status, response_headers = exc.code, dict(exc.headers or {})
                if exc.code in {420, 429}:
                    cooldown = retry_seconds(exc.headers.get("Retry-After") if exc.headers else None,
                                             fallback=3600 if exc.code == 420 else 60)
                error_body = None
                try:
                    raw_error = exc.read(65536)
                    decoded = json.loads(raw_error.decode("utf-8"))
                    if isinstance(decoded, dict):
                        error_body = redact({k: decoded[k] for k in ("code", "message", "error", "errors", "detail", "title") if k in decoded}, secrets=(key,))
                except (OSError, ValueError, UnicodeError):
                    pass
                if self.audit:
                    self.audit("catalog.request.failed", {"http_status": exc.code, "path": path,
                        "response": error_body,
                        "request_id": exc.headers.get("x-request-id", "") if exc.headers else ""})
                raise CatalogError(f"каталог: HTTP {exc.code}; автоматического повтора нет") from None
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                if self.audit:
                    self.audit("catalog.request.failed", {"path": path, "error_type": type(exc).__name__})
                raise CatalogError("ошибка связи или формата каталога; автоматического повтора нет") from None
            finally:
                self.limiter.release(token, status=status, headers=response_headers, cooldown=cooldown)
        if not isinstance(body, dict) or body.get("error") or body.get("status") == "ERROR":
            raise CatalogError("площадка вернула ошибку каталога")
        return body

    def page(self, rows, cursor):
        if self.audit:
            # Only product identities, names and listing states: no commercial/credential fields.
            self.audit("catalog.page.received", {"listings": rows, "next_cursor": cursor, "call": self.calls})

    def fetch(self):
        return {"wildberries": self.wildberries, "ozon": self.ozon, "yandex_market": self.yandex}[self.account["marketplace"]]()

    def wildberries(self):
        cursor, seen, rows = {"limit": 100}, set(), []
        while True:
            body = self.request("POST", "/content/v2/get/cards/list", {"settings": {
                "sort": {"ascending": True}, "filter": {"withPhoto": -1}, "cursor": cursor}})
            cards, next_cursor = body.get("cards"), body.get("cursor")
            if not isinstance(cards, list) or not isinstance(next_cursor, dict) or type(next_cursor.get("total")) is not int:
                raise CatalogError("WB: неизвестная структура страницы")
            page = [listing(c.get("nmID"), c.get("vendorCode"), c.get("title"), aliases=[("nmID", c.get("nmID"))]) for c in cards]
            self.page(page, next_cursor)
            rows.extend(page)
            if next_cursor["total"] < 100:
                return rows
            cursor = {"limit": 100, "updatedAt": next_cursor.get("updatedAt"), "nmID": next_cursor.get("nmID")}
            fingerprint = json.dumps(cursor, sort_keys=True)
            if not cursor["updatedAt"] or not cursor["nmID"] or fingerprint in seen:
                raise CatalogError("WB: повторный или неполный курсор")
            seen.add(fingerprint)

    def ozon(self):
        cursor, seen, products = "", set(), {}
        while True:
            body = self.request("POST", "/v3/product/list", {"filter": {"visibility": "ALL"}, "limit": 1000, "last_id": cursor})
            result = body.get("result")
            if not isinstance(result, dict) or not isinstance(result.get("items"), list):
                raise CatalogError("Ozon: неизвестная структура списка товаров")
            items = result["items"]
            for item in items:
                pid = str(item.get("product_id") or "")
                if not pid:
                    raise CatalogError("Ozon: нет product_id")
                projected = {k: item.get(k) for k in ("product_id", "offer_id", "archived")}
                if pid in products and products[pid] != projected:
                    raise CatalogError("Ozon: противоречивые дубликаты товара")
                products[pid] = projected
            self.page([listing(i.get("product_id"), i.get("offer_id"), status="archived" if i.get("archived") else "listed") for i in items], result.get("last_id"))
            total = result.get("total")
            if not items and type(total) is int and len(products) < total:
                raise CatalogError("Ozon: список оборвался раньше указанного количества товаров")
            if not items or (type(total) is int and len(products) >= total) or (total is None and len(items) < 1000):
                break
            cursor = result.get("last_id")
            if not cursor or cursor in seen:
                raise CatalogError("Ozon: повторный или отсутствующий курсор")
            seen.add(cursor)
        rows = []
        ids = list(products)
        for offset in range(0, len(ids), 1000):
            wanted = ids[offset:offset + 1000]
            body = self.request("POST", "/v3/product/info/list", {"product_id": wanted})
            details = body.get("items")
            if not isinstance(details, list) or {str(i.get("id")) for i in details} != set(wanted):
                raise CatalogError("Ozon: не получена информация обо всех товарах")
            page = []
            for item in details:
                pid, sources = str(item["id"]), item.get("sources") or []
                aliases = [("product_id", pid)] + [("sku", s.get("sku")) for s in sources]
                if item.get("sku"):
                    aliases.append(("sku", item["sku"]))
                page.append(listing(pid, item.get("offer_id"), item.get("name"), aliases=aliases,
                    status="archived" if item.get("is_archived") or item.get("is_autoarchived") or products[pid]["archived"] else "listed"))
            self.page(page, None)
            rows.extend(page)
        return rows

    def yandex(self):
        business_id = self.account.get("business_id")
        if not str(business_id or "").isdigit():
            # Discovery is exact: never select the first of several accessible businesses.
            cursor, seen, businesses = "", set(), set()
            while True:
                body = self.request("GET", "/v2/campaigns", query={"limit": 100, **({"pageToken": cursor} if cursor else {})})
                campaigns = body.get("campaigns")
                if not isinstance(campaigns, list):
                    raise CatalogError("Яндекс: неизвестная структура кабинетов")
                businesses.update(str(c.get("business", {}).get("id")) for c in campaigns if c.get("business", {}).get("id"))
                cursor = (body.get("paging") or {}).get("nextPageToken")
                if not cursor:
                    break
                if cursor in seen:
                    raise CatalogError("Яндекс: повторный курсор кабинетов")
                seen.add(cursor)
            if len(businesses) != 1:
                raise CatalogError("Яндекс: укажите business_id; доступно ноль или несколько кабинетов")
            business_id = businesses.pop()
            if self.audit:
                self.audit("catalog.scope.resolved", {"business_id": business_id})
        cursor, seen, rows = "", set(), []
        while True:
            body = self.request("POST", f"/v2/businesses/{business_id}/offer-mappings", {"archived": False},
                {"limit": 100, **({"pageToken": cursor} if cursor else {})})
            result = body.get("result")
            if not isinstance(result, dict) or not isinstance(result.get("offerMappings"), list):
                raise CatalogError("Яндекс: неизвестная структура каталога")
            page = []
            for item in result["offerMappings"]:
                offer, mapping = item.get("offer") or {}, item.get("mapping") or {}
                page.append(listing(offer.get("offerId"), offer.get("offerId"), offer.get("name"),
                    aliases=[("offerId", offer.get("offerId")), ("marketSku", mapping.get("marketSku"))],
                    status="archived" if offer.get("archived") else "listed"))
            cursor = (result.get("paging") or {}).get("nextPageToken")
            self.page(page, cursor)
            rows.extend(page)
            if not cursor:
                return rows
            if cursor in seen:
                raise CatalogError("Яндекс: повторный курсор товаров")
            seen.add(cursor)
