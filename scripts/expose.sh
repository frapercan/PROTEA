#!/usr/bin/env bash
# scripts/expose.sh — Expone PROTEA a internet via ngrok (dominio estático gratuito)
#
# Usage:
#   bash scripts/expose.sh
#
# Requisitos:
#   - ngrok instalado y autenticado (ngrok config add-authtoken <token>)
#   - Stack PROTEA corriendo (bash scripts/manage.sh start)
#
# Arquitectura:
#   Un solo túnel ngrok apunta al frontend (:3000).
#   Las llamadas a /api-proxy/* son reescritas por Next.js a localhost:8000
#   (ver apps/web/next.config.ts), por lo que no hace falta exponer la API.
#
# Dominio estático configurado:
#   https://protea.ngrok.app

set -euo pipefail

NGROK_DOMAIN="protea.ngrok.app"
PUBLIC_URL="https://${NGROK_DOMAIN}"

GREEN="\033[32m"; YELLOW="\033[33m"; BOLD="\033[1m"; CYAN="\033[36m"; RESET="\033[0m"
step() { printf "\n${BOLD}==> %s${RESET}\n" "$*"; }
ok()   { printf "  ${GREEN}✓${RESET} %s\n" "$*"; }

# ── 1. Verificar que el stack corre ───────────────────────────────────────────
step "Verificando stack local"
curl -sf http://localhost:8000/jobs > /dev/null \
    || { printf "  ${YELLOW}API no está corriendo. Lanza primero: bash scripts/manage.sh start${RESET}\n"; exit 1; }
ok "API corriendo en :8000"
curl -sf http://localhost:3000 -o /dev/null \
    || { printf "  ${YELLOW}Frontend no está corriendo. Lanza primero: bash scripts/manage.sh start${RESET}\n"; exit 1; }
ok "Frontend corriendo en :3000"

# ── 2. Verificar ngrok ────────────────────────────────────────────────────────
step "Verificando ngrok"
if ! command -v ngrok &>/dev/null; then
    printf "  ${YELLOW}ngrok no encontrado en PATH.${RESET}\n"
    printf "  Instala ngrok: https://ngrok.com/download\n"
    printf "  Luego autentícate: ngrok config add-authtoken <TOKEN>\n"
    exit 1
fi
ok "ngrok disponible"

# ── 3. Abrir túnel ────────────────────────────────────────────────────────────
step "Abriendo túnel ngrok → :3000 (dominio estático)"

printf "\n${BOLD}╔══════════════════════════════════════════════════════════╗${RESET}\n"
printf "${BOLD}║              PROTEA accesible desde internet             ║${RESET}\n"
printf "${BOLD}╚══════════════════════════════════════════════════════════╝${RESET}\n\n"
printf "  ${GREEN}${BOLD}Frontend (para el profesor):${RESET}\n"
printf "  ${CYAN}  $PUBLIC_URL${RESET}\n\n"
printf "  ${YELLOW}Presiona Ctrl+C para cerrar el túnel.${RESET}\n\n"

ngrok http --domain="$NGROK_DOMAIN" 3000
