#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
printf 'Имя оператора для журнала: '
read -r MRA_OPERATOR
[ -n "$MRA_OPERATOR" ] || exit 1
export MRA_OPERATOR
printf 'Модель: 1 — GPT-6 Luna, 2 — GPT-6.1 Sol: '
read -r mra_model_choice
case "$mra_model_choice" in
  1) MRA_MODEL=openai/gpt-6-luna ;;
  2) MRA_MODEL=openai/gpt-6.1-sol ;;
  *) printf 'Модель не выбрана.\n'; exit 1 ;;
esac
MRA_PROVIDER=openrouter
MRA_MAX_OUTPUT_TOKENS=2000
export MRA_PROVIDER MRA_MODEL MRA_MAX_OUTPUT_TOKENS
if [ -z "${OPENROUTER_API_KEY:-}${MRA_OPENROUTER_KEY_FILE:-}${MRA_OPENROUTER_AUTH_FILE:-}" ]; then
  MRA_OPENROUTER_KEY_FILE=$(python3 -B -c 'from mra.config import RUNTIME_KEY_FILE; print(RUNTIME_KEY_FILE)')
  if [ -z "$MRA_OPENROUTER_KEY_FILE" ]; then
    printf 'Путь к локальному файлу ключа OpenRouter: '
    read -r MRA_OPENROUTER_KEY_FILE
  fi
  export MRA_OPENROUTER_KEY_FILE
fi
exec ./mra-cli terminal
