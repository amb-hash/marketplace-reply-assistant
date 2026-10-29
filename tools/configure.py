#!/usr/bin/env python3
"""Interactive local setup. No networking, no reading or copying credential contents."""

from datetime import datetime, timezone
import getpass
import json
from pathlib import Path
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from mra.catalog_api import load_accounts


def credential_path(value):
    path = Path(value).expanduser().resolve()
    if not path.is_absolute() or not path.is_file() or path.is_relative_to(ROOT.resolve()):
        raise ValueError("нужен существующий файл ключа вне папки установки MRA")
    return str(path)


def save(path, value):
    if path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%SZ")
        backup = path.with_name(path.stem + ".backup-" + stamp + "-" + uuid.uuid4().hex[:6] + path.suffix)
        shutil.copy2(path, backup)
        backup.chmod(0o600)
    temporary = path.with_name(".setup-" + uuid.uuid4().hex)
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def configure(read=input, write=print):
    data = ROOT / "data"
    data.mkdir(exist_ok=True)
    if not (data / "README.md").exists():
        (data / "README.md").write_text("# Локальные данные\n\nНастройки, SQLite и журналы этой установки. В Git не входят. Основная инструкция: [README](../README.md).\n")
    path = data / "accounts.json"
    accounts = load_accounts(path if path.exists() else ROOT / "examples/accounts.example.json")
    write("Настройка MRA: вводятся пути к файлам, сами ключи сюда не вставляйте. Сетевых запросов нет.")
    for i, account in enumerate(accounts, 1):
        write(f"{i}. {account['marketplace']} · {account['name']} · {account['account_id']}")
    selected = read("Номер кабинета (Enter — только настройка модели): ").strip()
    if selected:
        if not selected.isdigit() or not 1 <= int(selected) <= len(accounts):
            raise ValueError("выберите номер из списка")
        account = accounts[int(selected) - 1]
        value = read("Путь к файлу ключа кабинета (Enter — сохранить прежний): ").strip()
        if value:
            account["api_key_file"] = credential_path(value)
            account.pop("api_key_env", None)
        if account["marketplace"] == "ozon":
            client_id = read("Client-Id Ozon (Enter — сохранить прежний): ").strip()
            if client_id:
                if not client_id.isdigit() or int(client_id) < 1:
                    raise ValueError("Client-Id должен быть положительным числом")
                account["client_id"] = client_id
                for key in ("client_id_file", "client_id_env"):
                    account.pop(key, None)
        if account["marketplace"] == "yandex_market":
            business = read("business_id (Enter — сохранить прежний/определить при чтении): ").strip()
            if business:
                if not business.isdigit() or int(business) < 1:
                    raise ValueError("business_id должен быть положительным числом")
                account["business_id"] = business
        if account["marketplace"] == "wildberries":
            token_type = read("Тип WB: 1 — персональный, 2 — сервисный, 3 — базовый с секретом, 4 — базовый/неизвестно: ").strip()
            options = {"1": "personal", "2": "service", "3": "base_with_secret", "4": "base"}
            if token_type not in options:
                raise ValueError("укажите тип токена WB")
            account["token_type"] = options[token_type]
        save(path, {"schema_version": 1, "accounts": accounts})
        write("Настройка кабинета сохранена; действительность ключа проверяется отдельным чтением API.")
    elif not path.exists():
        save(path, {"schema_version": 1, "accounts": accounts})
    choice = read("Модель: 1 — OpenRouter/Luna, 2 — OpenRouter/Sol, 3 — OpenAI/Sol, Enter — сохранить прежнюю: ").strip()
    options = {"1": ("openrouter", "openai/gpt-6-luna"), "2": ("openrouter", "openai/gpt-6.1-sol"), "3": ("openai", "gpt-6.1-sol")}
    if choice:
        if choice not in options:
            raise ValueError("выберите модель из списка")
        provider, model = options[choice]
        key_file = credential_path(read("Путь к файлу API-ключа выбранного провайдера: ").strip())
        actor = read("Ваше имя для журнала (Enter — имя пользователя Mac/Linux): ").strip() or getpass.getuser()
        save(data / "runtime.json", {"schema_version": 1, "provider": provider, "model": model, "key_file": key_file, "operator": actor})
        write(f"Сохранено: {provider} / {model}. Оплата и доступность модели проверяются при отдельно разрешённом вызове.")
    write("Далее: ./install.sh, затем ./mra.command. Перед тестом проверьте доступ через access-check и обновите каталог.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(configure())
    except (OSError, ValueError) as exc:
        print("Настройка не завершена: " + str(exc), file=sys.stderr)
        raise SystemExit(1)
    except (EOFError, KeyboardInterrupt):
        print("\nНастройка прервана. Уже сохранённые шаги остаются; повторный запуск доступен.")
        raise SystemExit(0)
