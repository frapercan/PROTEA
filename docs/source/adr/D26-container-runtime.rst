ADR-D26: Container runtime: OCI plus Apptainer
================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F-OPS

Context
-------
Cloud and developer environments expect OCI containers (Docker,
Podman, k8s). HPC sites mostly forbid privileged Docker but support
Apptainer (formerly Singularity) on rootless ``.sif`` images.

Decision
--------
Source of truth is OCI multi-stage Dockerfiles per repo. CI converts
each tagged image to an Apptainer ``.sif`` published as a release
artefact. No separate Apptainer Definition file maintained by hand.

Consequences
------------
- One Dockerfile per repo. ``.sif`` is a build artefact, not a source
  format.
- Image size budget per repo: <500 MB.
- ``protea-bundle`` repo orchestrates fat image construction for HPC.

Resolution
----------
Closed.
