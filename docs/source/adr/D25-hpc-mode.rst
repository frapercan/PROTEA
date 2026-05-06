ADR-D25: HPC operation mode
=============================

:Status: Accepted (mode B primary, mode C deferred)
:Date: 2026-05-05
:Decided: 2026-05-06 (user confirmation)
:Phase: F-OPS
:Gate: opens at F-OPS entry

Context
-------
PROTEA must support HPC environments (BSC and similar). HPC sites
typically forbid privileged Docker, may restrict outbound network,
and schedule via SLURM. Two main modes are available:

- **Mode B**: stateless workers running on HPC nodes connect to a
  PostgreSQL and RabbitMQ hosted in the cloud (LifeWatch / EOSC).
- **Mode C**: fully airgapped batch bundle. ``.sif`` Apptainer image
  with snapshot DB precargado, default booster, single-node SLURM
  job, no outbound traffic.

Decision (recommended)
----------------------
Both. Mode B as primary (closer to the cloud architecture). Mode C as
fallback for sites without outbound network or strict data-sovereignty
constraints.

Consequences
------------
- Two SLURM templates (``deploy/hpc/slurm-mode-b.sh``,
  ``deploy/hpc/slurm-mode-c.sh``).
- Apptainer ``.sif`` produced from the OCI multi-stage builds (see
  D26).
- Airgap bundle (``protea-airgap-bundle-vX.Y.Z.tar.gz``) tested on a
  network-disconnected machine.

Resolution
----------
**Accepted with scope adjustment.** User confirmation 2026-05-06: mode
B primary, mode C deferred until **post-defensa** (when contact with
BSC or similar restricted sites becomes concrete). Mode B is sufficient
for the thesis defense scope: stateless PROTEA workers on HPC nodes
connecting to LifeWatch / EOSC-hosted Postgres + RabbitMQ. Mode C
(airgap ``.sif`` bundle) becomes a F9 post-defensa item.
