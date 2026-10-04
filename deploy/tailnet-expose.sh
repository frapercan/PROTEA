#!/usr/bin/env bash
# Reexpone a la tailnet los puertos que docker-compose liga solo a loopback.
#
# POR QUE NO SE LIGA DIRECTAMENTE A LA IP DE TAILSCALE EN EL COMPOSE.
# docker.service no declara ninguna dependencia de tailscaled.service, y las dos
# estan habilitadas al arranque. Un `ports: "100.124.245.1:5432:5432"` es por
# tanto una carrera: si docker gana, la interfaz tailscale0 todavia no tiene
# direccion, el bind falla y el contenedor no arranca. En esta maquina eso
# significa que el servidor que posee TODO el estado del proyecto se queda sin
# base de datos, en silencio, despues de un reinicio.
#
# Loopback esta siempre disponible, asi que el compose liga ahi y el reenvio lo
# hace tailscaled, que aplica esta configuracion cuando arranca. El modo de
# fallo se invierte: si tailscaled esta caido, el nodo de computo pierde acceso
# y el servidor conserva su base de datos. Esa es la direccion correcta.
#
# QUE SE EXPONE Y QUE NO. Solo lo que el nodo de computo necesita para unirse a
# las colas y publicar resultados. Las consolas de operador (15672 de RabbitMQ,
# 9001 de MinIO, 15692 de metricas) no se exponen: se alcanzan por tunel ssh
# cuando hace falta, que es poco y siempre con alguien delante.
#
#   ssh -N -L 15672:127.0.0.1:15672 bioxaxi2@100.124.245.1
#
# La configuracion de `tailscale serve` PERSISTE entre reinicios, asi que este
# script se ejecuta una vez, no en cada arranque.
set -euo pipefail

PUERTOS=(5432 5672 9000 8000)   # postgres, amqp, minio, api

uso() { echo "uso: $0 {on|off|status}" >&2; exit 2; }
[[ $# -eq 1 ]] || uso

case "$1" in
  on)
    for p in "${PUERTOS[@]}"; do
      echo "reenviando tailnet:${p} -> 127.0.0.1:${p}"
      tailscale serve --bg --tcp="${p}" "tcp://127.0.0.1:${p}"
    done
    echo
    tailscale serve status
    ;;
  off)
    for p in "${PUERTOS[@]}"; do
      tailscale serve --tcp="${p}" off || true
    done
    tailscale serve status
    ;;
  status)
    tailscale serve status
    ;;
  *) uso ;;
esac
