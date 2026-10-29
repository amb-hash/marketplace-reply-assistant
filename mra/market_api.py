"""Bounded communication clients for the official WB, Ozon and Market APIs."""

from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from .audit import redact
from .catalog_api import runtime_value
from .rate_limits import retry_seconds

HOSTS = {"wildberries": "https://feedbacks-api.wildberries.ru", "ozon": "https://api-seller.ozon.ru",
         "yandex_market": "https://api.partner.market.yandex.ru"}


class MarketError(ValueError):
    def __init__(self, message, *, uncertain=False, status=None):
        super().__init__(message)
        self.uncertain, self.status = uncertain, status


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _id(value):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        raise MarketError("в обращении отсутствует идентификатор")
    return str(value)


def _rows(value, key):
    rows = value.get(key) if isinstance(value, dict) else None
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise MarketError("неизвестная структура ответа: " + key)
    return rows


def normalize(account, kind, raw, *, business_id=""):
    if kind not in {"question", "review"} or not isinstance(raw, dict):
        raise MarketError("неизвестный тип обращения")
    mp = account["marketplace"]
    hints, pros, cons, rating, created = {}, "", "", None, ""
    if mp == "wildberries":
        details = raw.get("productDetails") or {}
        hints = {"nmID": details.get("nmId"), "sellerArticle": details.get("supplierArticle")}
        text, native = raw.get("text", ""), raw.get("id")
        pros, cons = raw.get("pros") or "", raw.get("cons") or ""
        rating = raw.get("productValuation") if kind == "review" else None
        created = raw.get("createdDate") or ""
    elif mp == "ozon":
        hints, text, native = {"sku": _id(raw.get("sku"))}, raw.get("text", ""), raw.get("id")
        rating = raw.get("rating") if kind == "review" else None
        created = raw.get("published_at") or ""
    elif mp == "yandex_market":
        if not str(business_id).isdigit():
            raise MarketError("Яндекс: нужен точный business_id")
        if kind == "question":
            identifiers = raw.get("questionIdentifiers") or {}
            native, text = identifiers.get("id"), raw.get("text", "")
            if raw.get("businessId") is not None and str(raw["businessId"]) != str(business_id):
                raise MarketError("вопрос относится к другому business_id")
        else:
            identifiers, description = raw.get("identifiers") or {}, raw.get("description") or {}
            native, text = raw.get("feedbackId"), description.get("comment") or ""
            pros, cons = description.get("advantages") or "", description.get("disadvantages") or ""
            rating = (raw.get("statistics") or {}).get("rating")
        hints = {"offerId": identifiers.get("offerId")}
        created = raw.get("createdAt") or ""
    else:
        raise MarketError("неизвестная площадка")
    if any(not isinstance(value, str) for value in (text, pros, cons)):
        raise MarketError("текст обращения должен быть строкой")
    if rating is not None and (type(rating) is not int or not 1 <= rating <= 5):
        raise MarketError("неверная оценка отзыва")
    item = {"marketplace": mp, "account_id": account["account_id"], "external_id": _id(native),
            "kind": kind, "text": text, "pros": pros, "cons": cons, "rating": rating,
            "hints": {k: str(v) for k, v in hints.items() if v is not None and str(v)}, "created_at": created}
    target = {k: item[k] for k in ("marketplace", "account_id", "external_id", "kind")}
    if mp == "ozon":
        target["sku"] = item["hints"]["sku"]
    if mp == "yandex_market":
        target["business_id"] = str(business_id)
    fingerprint = digest({"target": target, **{k: item[k] for k in ("text", "pros", "cons", "rating", "hints")}})
    return {"message": item, "target": target, "fingerprint": fingerprint}


class CommunicationClient:
    def __init__(self, account, *, transport=None, audit=None, pause=time.sleep, max_requests=30, allow_write=False, limiter=None):
        if account.get("marketplace") not in HOSTS or type(max_requests) is not int or max_requests < 1:
            raise MarketError("неверные настройки клиента")
        self.account, self.transport, self.audit, self.pause = account, transport, audit, pause
        self.max_requests, self.calls, self.allow_write = max_requests, 0, allow_write
        self.limiter = limiter

    def record(self, event, payload):
        if self.audit:
            self.audit(event, {"account_id": self.account["account_id"], "marketplace": self.account["marketplace"], **payload})

    def request(self, method, path, payload=None, query=None, *, write=False):
        mp = self.account["marketplace"]
        read_routes = {
            "wildberries": {("GET", p) for p in ("/api/v1/questions", "/api/v1/question", "/api/v1/feedbacks", "/api/v1/feedback")},
            "ozon": {("POST", p) for p in ("/v1/question/list", "/v1/question/info", "/v1/question/answer/list",
                    "/v2/review/list", "/v2/review/info", "/v1/review/comment/list")},
        }
        write_routes = {"wildberries": {("PATCH", "/api/v1/questions"), ("POST", "/api/v1/feedbacks/answer")},
                        "ozon": {("POST", "/v1/question/answer/create"), ("POST", "/v1/review/comment/create")}}
        if mp == "yandex_market":
            allowed = (method == "GET" and path == "/v2/campaigns" and not write) or (method == "POST" and bool(re.fullmatch(
                r"(?:/v1/businesses/[1-9][0-9]*/goods-questions(?:/answers)?|/v2/businesses/[1-9][0-9]*/goods-feedback(?:/comments)?)", path)) and not write)
            if write:
                allowed = method == "POST" and bool(re.fullmatch(
                    r"(?:/v1/businesses/[1-9][0-9]*/goods-questions/update|/v2/businesses/[1-9][0-9]*/goods-feedback/comments/update)", path))
        else:
            allowed = (method, path) in (write_routes[mp] if write else read_routes[mp])
        if not allowed or (write and not self.allow_write):
            raise MarketError("маршрут или запись не разрешены")
        if write:
            if not isinstance(payload, dict):
                raise MarketError("ожидается объект запроса")
            if mp == "wildberries":
                valid = (set(payload) == {"id", "answer", "state"} and payload.get("state") == "wbRu"
                    and isinstance(payload.get("answer"), dict) and set(payload["answer"]) == {"text"}) if method == "PATCH" else set(payload) == {"id", "text"}
            elif mp == "ozon":
                valid = set(payload) == {"question_id", "sku", "text"} if path.endswith("/answer/create") else (
                    set(payload) == {"review_id", "text", "mark_review_as_processed"} and payload.get("mark_review_as_processed") is True)
            else:
                valid = (set(payload) == {"parentEntityId", "operationType", "text"} and payload.get("operationType") == "CREATE"
                    and (payload.get("parentEntityId") or {}).get("type") == "QUESTION") if "goods-questions" in path else (
                    set(payload) == {"feedbackId", "comment"} and isinstance(payload.get("comment"), dict) and set(payload["comment"]) == {"text"})
            if not valid:
                raise MarketError("разрешено только создание нового ответа на указанное обращение")
        if self.calls >= self.max_requests:
            raise MarketError("достигнут предел запросов; результат не подтверждён полностью")
        token, cooldown, status, response_headers = None, 0, None, {}
        if not self.transport:
            if self.limiter is None:
                raise MarketError("клиент API запущен без общего ограничителя кабинета")
            token, wait = self.limiter.acquire(method, path)
        elif self.calls:
            self.pause(1.1)
        started, key = time.monotonic(), ""
        try:
            if token is not None and wait:
                self.record("market.rate_wait", {"seconds": wait})
                self.pause(wait)
            self.calls += 1
            self.record("market.request.started", {"method": method, "path": path, "body": payload, "query": query,
                "write": write, "call": self.calls})
            if self.transport:
                body = self.transport(method, path, payload, query)
                status, request_id = 200, "fixture"
            else:
                key = runtime_value(self.account, "api_key")
                if not key:
                    raise MarketError("не настроен ключ кабинета")
                headers = {"Content-Type": "application/json"}
                if mp == "wildberries":
                    headers["Authorization"] = key
                elif mp == "ozon":
                    client_id = runtime_value(self.account, "client_id")
                    if not client_id:
                        raise MarketError("не настроен Client-Id Ozon")
                    headers.update({"Api-Key": key, "Client-Id": client_id})
                else:
                    headers["Api-Key"] = key
                request = urllib.request.Request(HOSTS[mp] + path + ("?" + urllib.parse.urlencode(query) if query else ""),
                    data=json.dumps(payload, ensure_ascii=False).encode() if payload is not None else None,
                    headers=headers, method=method)
                with urllib.request.build_opener(NoRedirect()).open(request, timeout=45) as response:
                    raw, status, request_id = response.read(8_000_001), response.status, response.headers.get("x-request-id", "")
                    response_headers = dict(response.headers)
                if len(raw) > 8_000_000:
                    raise MarketError("ответ API превышает 8 МБ", uncertain=write)
                body = json.loads(raw.decode()) if raw else {}
            body = redact(body, secrets=(key,))
            self.record("market.response.received", {"path": path, "http_status": status, "request_id": request_id,
                "write": write, "elapsed_ms": round((time.monotonic() - started) * 1000), "response": body})
            if not isinstance(body, dict) or body.get("error") or body.get("status") == "ERROR" or body.get("code"):
                raise MarketError("API вернул ошибку", uncertain=write)
            return body
        except urllib.error.HTTPError as exc:
            status, response_headers = exc.code, dict(exc.headers or {})
            if exc.code in {420, 429}:
                cooldown = retry_seconds(exc.headers.get("Retry-After") if exc.headers else None,
                                         fallback=3600 if exc.code == 420 else 60)
            try:
                body = redact(json.loads(exc.read(65536)), secrets=(key,))
            except (OSError, ValueError):
                body = None
            self.record("market.request.failed", {"path": path, "http_status": exc.code, "response": body, "write": write,
                "retry_after": exc.headers.get("Retry-After", "") if exc.headers else ""})
            raise MarketError(f"API: HTTP {exc.code}; автоматического повтора нет", uncertain=write and exc.code >= 500,
                              status=exc.code) from None
        except MarketError as exc:
            self.record("market.request.failed", {"path": path, "write": write, "reason": str(exc), "uncertain": exc.uncertain})
            raise
        except (OSError, ValueError, TypeError) as exc:
            self.record("market.request.failed", {"path": path, "error_type": type(exc).__name__, "write": write})
            raise MarketError("ошибка связи или формата API; автоматического повтора нет", uncertain=write) from None
        finally:
            if token is not None:
                self.limiter.release(token, status=status, headers=response_headers, cooldown=cooldown)

    def business_id(self):
        if getattr(self, "_business_id", ""):
            return self._business_id
        business = self.account.get("business_id")
        if str(business or "").isdigit() and int(business) > 0:
            return str(business)
        cursor, seen, businesses = "", set(), set()
        while True:
            body = self.request("GET", "/v2/campaigns", query={"limit": 100, **({"pageToken": cursor} if cursor else {})})
            businesses.update(str((row.get("business") or {}).get("id")) for row in _rows(body, "campaigns")
                              if (row.get("business") or {}).get("id"))
            cursor = (body.get("paging") or {}).get("nextPageToken")
            if not cursor:
                break
            if cursor in seen:
                raise MarketError("повторный курсор кабинетов")
            seen.add(cursor)
        if len(businesses) != 1:
            raise MarketError("укажите business_id: доступно ноль или несколько кабинетов")
        self._business_id = businesses.pop()
        return self._business_id

    def inbox(self, kind, *, limit=20):
        if kind not in {"question", "review"} or type(limit) is not int or not 1 <= limit <= 1000:
            raise MarketError("выберите question/review и limit от 1 до 1000")
        mp, cursor, seen, result = self.account["marketplace"], "", set(), []
        business = self.business_id() if mp == "yandex_market" else ""
        while True:
            if mp == "wildberries":
                size = min(100, limit - len(result))
                body = self.request("GET", "/api/v1/questions" if kind == "question" else "/api/v1/feedbacks", query={
                    "isAnswered": "false", "take": size, "skip": int(cursor or 0), "order": "dateDesc"})
                rows = _rows(body.get("data"), "questions" if kind == "question" else "feedbacks")
                next_cursor = str(int(cursor or 0) + len(rows)) if len(rows) >= size else ""
            elif mp == "ozon":
                payload = {"limit": 100, "last_id": cursor, "sort_dir": "DESC"}
                if kind == "question":
                    payload["filter"] = {"status": "UNPROCESSED"}
                body = self.request("POST", "/v1/question/list" if kind == "question" else "/v2/review/list", payload)
                rows = _rows(body, "questions" if kind == "question" else "reviews")
                if body.get("has_next") is True and not body.get("last_id"):
                    raise MarketError("объявлена следующая страница без курсора")
                next_cursor = body.get("last_id") if body.get("has_next", bool(rows)) else ""
            else:
                path = f"/v1/businesses/{business}/goods-questions" if kind == "question" else f"/v2/businesses/{business}/goods-feedback"
                body = self.request("POST", path, {"needAnswer": True} if kind == "question" else {"reactionStatus": "NEED_REACTION"},
                    {"limit": 50, **({"pageToken": cursor} if cursor else {})})
                rows = _rows(body.get("result"), "questions" if kind == "question" else "feedbacks")
                next_cursor = ((body.get("result") or {}).get("paging") or {}).get("nextPageToken")
            for raw in rows:
                if mp == "ozon" and kind == "review" and raw.get("status") == "PROCESSED":
                    continue
                if mp == "ozon" and kind == "question" and int(raw.get("answers_count") or 0) > 0:
                    continue
                result.append(normalize(self.account, kind, raw, business_id=business))
                if len(result) >= limit:
                    return result
            if not next_cursor:
                return result
            if not rows or next_cursor in seen:
                raise MarketError("неполный или повторный курсор обращений")
            seen.add(next_cursor)
            cursor = next_cursor

    def probe(self, kind):
        if kind not in {"question", "review"}:
            raise MarketError("выберите question или review")
        mp = self.account["marketplace"]
        if mp == "wildberries":
            body = self.request("GET", "/api/v1/questions" if kind == "question" else "/api/v1/feedbacks",
                query={"isAnswered": "false", "take": 1, "skip": 0, "order": "dateDesc"})
            rows = _rows(body.get("data"), "questions" if kind == "question" else "feedbacks")
        elif mp == "ozon":
            body = self.request("POST", "/v1/question/list" if kind == "question" else "/v2/review/list", {"limit": 20})
            rows = _rows(body, "questions" if kind == "question" else "reviews")
        else:
            business = self.business_id()
            body = self.request("POST", f"/v1/businesses/{business}/goods-questions" if kind == "question" else f"/v2/businesses/{business}/goods-feedback",
                {}, {"limit": 1})
            rows = _rows(body.get("result"), "questions" if kind == "question" else "feedbacks")
        return {"read_access": True, "returned": len(rows), "http_requests": self.calls}

    def check_scope(self, target):
        if any(str(target.get(k)) != str(self.account.get(k)) for k in ("marketplace", "account_id")):
            raise MarketError("попытка обратиться к другому кабинету")
        _id(target.get("external_id"))
        if target.get("kind") not in {"question", "review"}:
            raise MarketError("неизвестный тип обращения")
        if target["marketplace"] == "yandex_market" and self.business_id() != str(target.get("business_id")):
            raise MarketError("business_id изменился")

    def get(self, target):
        self.check_scope(target)
        mp, kind, external = target["marketplace"], target["kind"], target["external_id"]
        if mp == "wildberries":
            raw = self.request("GET", "/api/v1/question" if kind == "question" else "/api/v1/feedback", query={"id": external}).get("data")
        elif mp == "ozon":
            raw = self.request("POST", "/v1/question/info" if kind == "question" else "/v2/review/info",
                {"question_id" if kind == "question" else "review_id": external})
        else:
            business = target["business_id"]
            path = f"/v1/businesses/{business}/goods-questions" if kind == "question" else f"/v2/businesses/{business}/goods-feedback"
            body = self.request("POST", path, {"questionIds" if kind == "question" else "feedbackIds": [int(external)]}, {"limit": 50})
            rows = _rows(body.get("result"), "questions" if kind == "question" else "feedbacks")
            if len(rows) != 1:
                raise MarketError("обращение не найдено однозначно")
            raw = rows[0]
        current = normalize(self.account, kind, raw, business_id=target.get("business_id", ""))
        if current["target"] != target:
            raise MarketError("площадка вернула другое обращение или товар")
        return current

    def replies(self, target):
        self.check_scope(target)
        mp, kind, external = target["marketplace"], target["kind"], target["external_id"]
        if mp == "wildberries":
            body = self.request("GET", "/api/v1/question" if kind == "question" else "/api/v1/feedback", query={"id": external})
            raw = body.get("data") or {}
            if str(raw.get("id")) != external:
                raise MarketError("API вернул другой вопрос/отзыв")
            answer = raw.get("answer") or {}
            return [{"id": external, "text": answer["text"], "owner": True,
                     "status": "PUBLISHED" if kind == "question" and raw.get("state") == "wbRu" else "OBSERVED"}] if answer.get("text") else []
        cursor, seen, result = "", set(), []
        while True:
            if mp == "ozon":
                if kind == "question":
                    body = self.request("POST", "/v1/question/answer/list", {"question_id": external,
                        "sku": int(target["sku"]), "last_id": cursor})
                    rows = _rows(body, "answers")
                    next_cursor = body.get("last_id") if rows else ""
                else:
                    body = self.request("POST", "/v1/review/comment/list", {"review_id": external, "limit": 100,
                        "offset": int(cursor or 0), "sort_dir": "ASC"})
                    rows = _rows(body, "comments")
                    next_cursor = str(int(cursor or 0) + len(rows)) if len(rows) == 100 else ""
                for row in rows:
                    if kind == "question" and (str(row.get("question_id")) != external or str(row.get("sku")) != target["sku"]):
                        raise MarketError("ответ относится к другому вопросу или SKU")
                    status = row.get("status_publication", "OBSERVED") if kind == "question" else (
                        "REJECTED" if row.get("is_rejected") else "PUBLISHED" if row.get("is_published") else "UNMODERATED")
                    result.append({"id": _id(row.get("id")), "text": row.get("text", ""),
                        "owner": row.get("is_owner") is True if kind == "review" else None, "status": str(status).upper()})
            else:
                business = target["business_id"]
                path = f"/v1/businesses/{business}/goods-questions/answers" if kind == "question" else f"/v2/businesses/{business}/goods-feedback/comments"
                body = self.request("POST", path, {"questionId" if kind == "question" else "feedbackId": int(external)},
                    {"limit": 50, **({"pageToken": cursor} if cursor else {})})
                rows = _rows(body.get("result"), "answers" if kind == "question" else "comments")
                next_cursor = ((body.get("result") or {}).get("paging") or {}).get("nextPageToken")
                for row in rows:
                    if str(row.get("questionId" if kind == "question" else "feedbackId")) != external:
                        raise MarketError("ответ относится к другому обращению")
                    result.append({"id": _id(row.get("id")), "text": row.get("text", ""),
                        "owner": True if row.get("canModify") is True else False if (row.get("author") or {}).get("type") == "USER" else None,
                        "status": str(row.get("status", "OBSERVED")).upper()})
            if not next_cursor:
                return result
            if not rows or next_cursor in seen:
                raise MarketError("неполный или повторный курсор ответов")
            seen.add(next_cursor)
            cursor = next_cursor

    def write_plan(self, target, text):
        if target.get("marketplace") != self.account["marketplace"] or target.get("account_id") != self.account["account_id"]:
            raise MarketError("кабинет назначения не совпадает")
        mp, kind, external = target["marketplace"], target["kind"], _id(target.get("external_id"))
        maximum = 3000 if mp == "ozon" and kind == "question" else 4096 if mp == "yandex_market" and kind == "review" else 5000
        if not isinstance(text, str) or not 2 <= len(text.strip()) <= maximum:
            raise MarketError(f"ответ должен содержать от 2 до {maximum} символов")
        if kind not in {"question", "review"}:
            raise MarketError("неизвестный тип обращения")
        if mp == "wildberries":
            return {"method": "PATCH" if kind == "question" else "POST", "path": "/api/v1/questions" if kind == "question" else "/api/v1/feedbacks/answer",
                "body": {"id": external, "answer": {"text": text}, "state": "wbRu"} if kind == "question" else {"id": external, "text": text}}
        if mp == "ozon":
            return {"method": "POST", "path": "/v1/question/answer/create" if kind == "question" else "/v1/review/comment/create",
                "body": {"question_id": external, "sku": int(target["sku"]), "text": text} if kind == "question" else {
                    "review_id": external, "text": text, "mark_review_as_processed": True}}
        business = str(target.get("business_id", ""))
        if not business.isdigit() or int(business) < 1:
            raise MarketError("неверный business_id")
        return {"method": "POST", "path": f"/v1/businesses/{business}/goods-questions/update" if kind == "question" else f"/v2/businesses/{business}/goods-feedback/comments/update",
            "body": {"parentEntityId": {"id": int(external), "type": "QUESTION"}, "operationType": "CREATE", "text": text} if kind == "question" else {
                "feedbackId": int(external), "comment": {"text": text}}}

    def send(self, target, text):
        self.check_scope(target)
        plan = self.write_plan(target, text)
        return self.request(plan["method"], plan["path"], plan["body"], write=True)
