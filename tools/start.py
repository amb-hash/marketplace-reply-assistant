#!/usr/bin/env python3
"""Guide a new operator from empty local key files to a human-reviewed session."""

from __future__ import annotations

import argparse
from contextlib import closing
import getpass
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.configure import save
from mra.catalog_api import load_accounts

FILES = {
    'acct_deppa_wb': ('wb-deppa-api-key.txt',),
    'acct_deppa_msk': ('wb-deppa-msk-api-key.txt',),
    'acct_deppa_ozon': ('ozon-deppa-api-key.txt', 'ozon-deppa-client-id.txt'),
    'acct_deppa_ozon_msk': ('ozon-deppa-msk-api-key.txt', 'ozon-deppa-msk-client-id.txt'),
    'acct_deppa_yandex': ('yandex-deppa-api-key.txt',),
    'acct_axxa_yandex': ('yandex-axxa-api-key.txt',),
}
PROFILES = {'1': ('openrouter', 'openai/gpt-6-luna', 'openrouter-api-key.txt'),
            '2': ('openrouter', 'openai/gpt-6.1-sol', 'openrouter-api-key.txt'),
            '3': ('openai', 'gpt-6.1-sol', 'openai-api-key.txt')}
CATEGORY = 'TWS наушники и моногарнитуры'


def prepare_keys(root):
    private = root / 'private'
    private.mkdir(mode=0o700, exist_ok=True)
    private.chmod(0o700)
    guide = private / 'README.md'
    if not guide.exists():
        guide.write_text('# Локальные секреты\n\nФайлы ключей этой установки. Git и сборщик передачи исключают private/. Начать: [START_HERE](../START_HERE.md).\n', encoding='utf-8')
    folder = private / 'credentials'
    folder.mkdir(mode=0o700, exist_ok=True)
    folder.chmod(0o700)
    names = sorted({name for row in FILES.values() for name in row} | {'openrouter-api-key.txt', 'openai-api-key.txt'})
    for name in names:
        path = folder / name
        if not path.exists():
            path.touch(mode=0o600, exist_ok=False)
        path.chmod(0o600)
    note = folder / 'README.md'
    if not note.exists():
        note.write_text('''# Куда вставлять ключи

Каждый TXT — одно значение без кавычек, названия поля и JSON. Сохраняйте обычный текст, не RTF. Для старта заполните выбранный кабинет и одного провайдера; остальные файлы можно заполнить позже.

- ozon-deppa-msk-api-key.txt — Api-Key Ozon Deppa MSK.
- ozon-deppa-msk-client-id.txt — Client-Id того же кабинета.
- ozon-deppa-api-key.txt и ozon-deppa-client-id.txt — отдельный кабинет Ozon Deppa.
- wb-deppa-api-key.txt и wb-deppa-msk-api-key.txt — ключи двух кабинетов WB.
- yandex-deppa-api-key.txt и yandex-axxa-api-key.txt — ключи двух кабинетов Яндекс Маркета.
- openrouter-api-key.txt — ключ OpenRouter; для выбранной модели нужен API-баланс.
- openai-api-key.txt — ключ прямого OpenAI API, если выбран этот провайдер.

Ozon: права вопросов/ответов и соответствующая подписка; для полного каталога — отдельное право чтения товаров. WB: категории «Вопросы и отзывы» и «Контент» с нужными правами. Яндекс: communication и доступ к каталогу. Подробности: [DEPLOYMENT](../../DEPLOYMENT.md).

Папка private/ игнорируется Git и не входит в ZIP исходников. После распаковки исходников start.command создаёт эти пустые файлы автоматически. Ключи в переписку и репозиторий не копируйте. Общая инструкция: [START_HERE](../../START_HERE.md).
''', encoding='utf-8')
    return folder


def validate_file(path, *, numeric=False):
    try:
        if path.stat().st_size > 16384:
            raise ValueError
        value = path.read_text(encoding='utf-8').strip()
        valid = bool(value) and not any(c.isspace() for c in value) and value.isascii() and not value.startswith(('{', '['))
        if numeric:
            valid = valid and value.isdigit() and int(value) > 0
        return valid
    except (OSError, UnicodeError, ValueError):
        return False


def configure(root, account_id, profile, *, token_type=None, business_id=None):
    folder = prepare_keys(root)
    provider, model, provider_file = PROFILES[profile]
    required = [folder / name for name in FILES[account_id]] + [folder / provider_file]
    missing = [path for path in required if not validate_file(path, numeric='client-id' in path.name)]
    if missing:
        return missing
    data = root / 'data'
    data.mkdir(mode=0o700, exist_ok=True)
    if not (data / 'README.md').exists():
        (data / 'README.md').write_text('# Данные установки\n\nSQLite, настройки и журналы этой установки. В Git не входят. [Инструкция](../START_HERE.md).\n', encoding='utf-8')
    path = data / 'accounts.json'
    accounts = load_accounts(path if path.exists() else root / 'examples/accounts.example.json')
    account = next(a for a in accounts if a['account_id'] == account_id)
    account['api_key_file'] = str(required[0].resolve())
    account.pop('api_key_env', None)
    if account['marketplace'] == 'ozon':
        account['client_id_file'] = str(required[1].resolve())
        for name in ('client_id', 'client_id_env'):
            account.pop(name, None)
    if token_type:
        account['token_type'] = token_type
    if business_id:
        account['business_id'] = business_id
    settings = [(path, {'schema_version': 1, 'accounts': accounts}),
        (data / 'runtime.json', {'schema_version': 1, 'provider': provider, 'model': model,
            'key_file': str((folder / provider_file).resolve()), 'operator': getpass.getuser()}),
        (data / 'start.json', {'schema_version': 1, 'account_id': account_id, 'profile': profile, 'category': CATEGORY})]
    for path, value in settings:
        if not path.exists() or json.loads(path.read_text(encoding='utf-8')) != value:
            save(path, value)
    return []


def main(argv=None):
    parser = argparse.ArgumentParser(description='Первый запуск MRA: кабинет → ключи → проверка → работа')
    parser.add_argument('--setup', action='store_true', help='изменить выбранный кабинет или провайдера')
    parser.add_argument('--prepare-only', action='store_true', help='создать файлы и проверить локальные настройки без API и запуска')
    parser.add_argument('--account', choices=tuple(FILES))
    parser.add_argument('--profile', choices=tuple(PROFILES))
    args = parser.parse_args(argv)
    root = ROOT
    folder = prepare_keys(root)
    accounts = load_accounts(root / 'examples/accounts.example.json')
    previous = {}
    if (root / 'data/start.json').exists():
        previous = json.loads((root / 'data/start.json').read_text(encoding='utf-8'))
    account_id = args.account or previous.get('account_id') or 'acct_deppa_ozon_msk'
    profile = args.profile or previous.get('profile') or '1'
    print('Marketplace Reply Assistant · запуск')
    interactive_setup = args.setup or (not previous and not args.prepare_only and not (args.account and args.profile))
    token_type, business_id = None, None
    if interactive_setup:
        print('\nШаг 1. Выберите кабинет.')
        for index, account in enumerate(accounts, 1):
            print(f"{index}. {account['marketplace']} · {account['name']}")
        value = input('Номер кабинета (Enter — Ozon Deppa MSK): ').strip()
        if value:
            if not value.isdigit() or not 1 <= int(value) <= len(accounts):
                raise ValueError('выберите кабинет из списка')
            account_id = accounts[int(value)-1]['account_id']
        print('\nШаг 2. Выберите провайдера и модель. Нужны API-ключ и баланс.')
        print('1. OpenRouter / Luna\n2. OpenRouter / Sol\n3. OpenAI / Sol')
        value = input('Вариант (Enter — OpenRouter / Luna): ').strip()
        if value:
            if value not in PROFILES:
                raise ValueError('выберите провайдера из списка')
            profile = value
        account = next(a for a in accounts if a['account_id'] == account_id)
        if account['marketplace'] == 'wildberries':
            value = input('Тип токена WB: 1 — персональный; 2 — сервисный; 3 — базовый с секретом; 4 — базовый: ').strip()
            token_type = {'1':'personal','2':'service','3':'base_with_secret','4':'base'}.get(value)
            if not token_type:
                raise ValueError('укажите тип токена WB')
        if account['marketplace'] == 'yandex_market':
            business_id = input('business_id Яндекс (Enter — определить при чтении, если кабинет один): ').strip() or None
            if business_id and (not business_id.isdigit() or int(business_id) < 1):
                raise ValueError('business_id должен быть положительным числом')
    if profile not in PROFILES or account_id not in FILES:
        raise ValueError('сохранённые настройки запуска некорректны; запустите с --setup')
    while True:
        missing = configure(root, account_id, profile, token_type=token_type, business_id=business_id)
        if not missing:
            break
        print('\nШаг 3. Вставьте значения и сохраните эти TXT-файлы:')
        for path in missing:
            print(str(path))
        print('В каждом файле — только значение одной строкой. Client-Id — число. Другие ключи пока не требуются.')
        if args.prepare_only:
            return 2
        if sys.platform == 'darwin':
            subprocess.run(['open', str(folder)], check=False)
            subprocess.run(['open', '-a', 'TextEdit', *map(str, missing)], check=False)
        if input('После сохранения нажмите Enter; 0 — завершить: ').strip() == '0':
            return 0
    if args.prepare_only:
        print('Локальные настройки готовы. Сетевых запросов нет.')
        return 0
    print('\nШаг 4. Подготавливаю локальную базу…')
    env = {name:value for name,value in os.environ.items() if not name.startswith('MRA_') and name not in {'OPENAI_API_KEY','OPENROUTER_API_KEY'}}
    env['MRA_PYTHON'] = sys.executable
    env.update(MRA_DATA_DIR=str(root / 'data'), MRA_DB_PATH=str(root / 'data/mra.sqlite3'),
        MRA_ACCOUNTS_FILE=str(root / 'data/accounts.json'), MRA_PROVIDER=PROFILES[profile][0],
        MRA_MODEL=PROFILES[profile][1], MRA_MAX_OUTPUT_TOKENS='2000')
    env['MRA_OPENROUTER_KEY_FILE' if PROFILES[profile][0] == 'openrouter' else 'MRA_OPENAI_KEY_FILE'] = str(folder / PROFILES[profile][2])
    if not (root / 'data/mra.sqlite3').is_file():
        log = root / 'data/startup-install.log'
        with log.open('w', encoding='utf-8') as stream:
            installed = subprocess.run(['sh', str(root / 'install.sh')], cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT)
        if installed.returncode:
            raise ValueError('установка не завершена; подробности в data/startup-install.log')
    checked = subprocess.run([sys.executable,'-B','-m','mra.cli','doctor'],cwd=root,env=env,capture_output=True,text=True)
    (root / 'data/startup-doctor.json').write_text(checked.stdout,encoding='utf-8')
    try:
        health = json.loads(checked.stdout)
    except ValueError:
        health = {}
    if checked.returncode or not all(health.get(name) is True for name in ('database_ok','rules_ok','canon_ok','bundled_rules_connected')):
        raise ValueError('локальная проверка не пройдена; подробности в data/startup-doctor.json')
    account = next(a for a in load_accounts(root / 'data/accounts.json') if a['account_id'] == account_id)
    print('Шаг 5. Проверяю доступ к вопросам выбранного кабинета. Это чтение API без вызова модели…')
    from mra.store import open_store
    from mra.market_api import CommunicationClient
    from mra.rate_limits import AccountLimiter
    with closing(open_store(root / 'data/mra.sqlite3')) as store:
        def audit(event, payload):
            store.audit_event(None, event, payload, actor=getpass.getuser())
        CommunicationClient(account, limiter=AccountLimiter(store, account), audit=audit, max_requests=3).probe('question')
        store.set_meta('operator.account_id', account_id)
        store.set_meta('operator.category', CATEGORY)
    print(f"\nГотово: {account['name']} · наушники с каноном · {PROFILES[profile][0]} / {PROFILES[profile][1]}.")
    print('Рабочая сессия: до 20 вопросов, по одному; до 2000 выходных токенов на ответ. Генерация платная.')
    print('Провайдер получает вопрос, правила и карточку товара. При переподготовке — также прежний ответ и ваше замечание.')
    print('Публикация — только после вашего Да. Нет переходит дальше. Переделать запрашивает замечание.')
    if input('Введите СТАРТ для начала работы (Enter — закончить настройку): ').strip().casefold() != 'старт':
        return 0
    return subprocess.run([sys.executable,'-B','-m','mra.cli','review-session','--account',account_id,
        '--category',CATEGORY,'--limit','20','--live'],cwd=root,env=env).returncode


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print('Запуск не завершён: ' + str(exc), file=sys.stderr)
        raise SystemExit(1)
    except (EOFError, KeyboardInterrupt):
        print('\nНастройка завершена. Вставленные значения и настройки сохранены.')
        raise SystemExit(0)
