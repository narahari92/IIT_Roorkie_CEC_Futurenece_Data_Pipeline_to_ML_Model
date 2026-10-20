#!/usr/bin/env bash
# Start/restart/stop local Airflow on a laptop behind a TLS-inspecting proxy (Cato Networks).
# On a normal network just use `astro dev start` — the trainer's Dockerfile is unchanged.
#
#   scripts/astro_corp.sh start     # build images (with corp CA) + astro dev start
#   scripts/astro_corp.sh restart   # rebuild project image (e.g. after requirements.txt change) + restart
#   scripts/astro_corp.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
BASE_IMG=astro-runtime-corp-ca:3.3-2
PROJ_IMG=nammamart-airflow:latest

build() {
  if [ ! -s .certs/cato-ca.pem ]; then
    security find-certificate -a -c "Cato Networks" -p /Library/Keychains/System.keychain > .certs/cato-ca.pem
  fi
  docker image inspect "$BASE_IMG" >/dev/null 2>&1 || docker build -t "$BASE_IMG" -f .certs/Dockerfile.base .certs/
  docker build -t "$PROJ_IMG" -f .certs/Dockerfile.project .
}

case "${1:-start}" in
  start)   build; astro dev start --no-browser --wait 5m --image-name "$PROJ_IMG" ;;
  restart) build; astro dev restart --image-name "$PROJ_IMG" ;;
  stop)    astro dev stop ;;
  *) echo "usage: $0 {start|restart|stop}"; exit 1 ;;
esac
