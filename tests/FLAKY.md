# Flaky tests inventory

Snapshot 2026-05-05 tras T0.1.

Ningún test detectado como flaky en este pase. Esto significa que en una sola ejecución todos los que pasan, pasan deterministically y todos los que fallan, fallan deterministically (deuda documentada en `BROKEN.md`).

Política: si un test pasa unas veces y falla otras durante el desarrollo, registrarlo aquí con:

- Nombre completo
- Síntoma observado (timing, orden de ejecución, race condition sospechada)
- Reproducción mínima si conocida
- Plan: `time.sleep` a sustituir por `wait_until`, fixture a aislar, mock a estabilizar, etc.

Hard rule del master plan v3 §F6 T6.1: cero `time.sleep` en `tests/`. Si entra un test con sleep, va a este fichero hasta sustituirse por `wait_until(predicate, timeout)`.
