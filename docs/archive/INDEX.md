# Documentos archivados

Archivado el 2026-09-14. Nada borrado: `git mv` conserva el historial y el
movimiento se deshace con un comando.

**Los tres partes de incidente** (`2026-05-26-dlq-audit-baseline`,
`2026-05-26-gpu-resume-blocked`, `2026-05-29-login-persist-debug`) describen una
cola y una GPU de otra instalación. La máquina se formateó y el estado se
recreó, así que sus identificadores, rutas y recuentos no corresponden a nada
que exista ahora. Se conservan como procedencia de cómo se diagnosticaron esos
fallos, no como descripción del sistema.

**`quality_baseline.md` y `refactor_plan_lab_decoupling.md`** son de mayo de
2026 y el refactor que el segundo planifica **ya ocurrió**: el laboratorio vive
en `protea-method` y `protea-contracts`. Un plan cumplido leído como plan
pendiente es una instrucción para deshacer lo hecho.

**`tests-BROKEN.md`, `tests-FLAKY.md` y `tests-SLOW.md`** no los consulta ningún
ejecutor: no aparecen en `pyproject.toml`, ni en `conftest.py`, ni en los flujos
de CI. Llevan desde el 24 de mayo sin tocarse, de modo que lo que declaran roto,
inestable o lento es una foto de hace cuatro meses que nadie ha comprobado
desde entonces. Una lista de tests rotos que nada verifica envejece hacia la
mentira: o se marca en el propio test con `xfail` y `skip`, donde el ejecutor la
lee, o no dice nada.
