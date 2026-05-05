ADR-D25: HPC operation mode
=============================

:Status: Pending
:Date: 2026-05-05
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
Pending; gate opens with F-OPS (T-OPS.5, T-OPS.9).
