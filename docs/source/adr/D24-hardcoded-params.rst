ADR-D24: Hardcoded parameters externalisation (T-CONF)
========================================================

:Status: Accepted
:Date: 2026-05-05
:Phase: F0 (closed)

Context
-------
Hardcoded chunk sizes, retries, batch sizes, timeouts, KNN K values,
score thresholds, pool sizes, reaper timeouts and similar magic
numbers were dispersed throughout ``protea-core``. Tuning per
deployment target (cloud, HPC-BSC, HPC-airgap, dev) is impossible
without externalisation; reproducibility suffers because the magic
numbers are not part of the run record.

Decision
--------
T-CONF: a three-step task in F0.

- **T-CONF.1**: inventory at ``docs/CONFIG_INVENTORY.md`` with 30-60
  entries minimum.
- **T-CONF.2**: ``protea_core.config.Settings`` (pydantic-settings)
  with hierarchy ``defaults < config/{env}.yaml < env vars < CLI
  flags``. Categories ``QueueTuning``, ``WorkerTuning``,
  ``OperationTuning``, ``IOTuning``, ``ObservabilityTuning``.
- **T-CONF.3**: living documentation appendix
  (``docs/source/appendix/configuration.rst``) auto-generated from
  the pydantic models.

Consequences
------------
- Magic numbers in operations code are forbidden post-T-CONF;
  ``# config-exempt: <reason>`` allowed only for semantic constants
  (``MD5_HASH_LEN``).
- Each ``ExperimentRun`` row records resolved hyperparameters as
  provenance.
- HPC and airgap deployments tune via ``config/hpc-bsc.yaml`` etc.

Resolution
----------
Closed (T-CONF.1-3 delivered in F0, 2026-05-05).
