# Changelog

## [0.7.1](https://github.com/frapercan/PROTEA/compare/v0.7.0...v0.7.1) (2026-05-07)


### Bug Fixes

* **lafa:** container syspath fallback no IndexError ([#25](https://github.com/frapercan/PROTEA/issues/25)) ([8b8951b](https://github.com/frapercan/PROTEA/commit/8b8951b03b91499330585cb5ee0b8dd09fb981e0))
* **lafa:** exporter HalfVector + anc2vec path ([#23](https://github.com/frapercan/PROTEA/issues/23)) ([6717790](https://github.com/frapercan/PROTEA/commit/67177900fa6f5c9b0623afd26049c6dbd134142c))

## [0.7.0](https://github.com/frapercan/PROTEA/compare/v0.6.0...v0.7.0) (2026-05-07)


### Features

* **lafa:** multi-booster bundle for v18/v19 selective reranker ([#21](https://github.com/frapercan/PROTEA/issues/21)) ([60b3ea9](https://github.com/frapercan/PROTEA/commit/60b3ea948bbad2f1241cd85fdda78e3d70dffb25))


### Documentation

* **lafa:** F-LAFA.5 method card draft ([#20](https://github.com/frapercan/PROTEA/issues/20)) ([a515429](https://github.com/frapercan/PROTEA/commit/a515429b5417d8cc36be71eabfc142e942a0d988))

## [0.6.0](https://github.com/frapercan/PROTEA/compare/v0.5.0...v0.6.0) (2026-05-07)


### Features

* **lafa:** F-LAFA minimal viable container (frozen-data + reranker) ([#16](https://github.com/frapercan/PROTEA/issues/16)) ([ad051bc](https://github.com/frapercan/PROTEA/commit/ad051bcaa6eae18ca9a7183e98ca06f5ed607ab0))
* **lafa:** F-LAFA.1 frozen-data bundle exporter ([#17](https://github.com/frapercan/PROTEA/issues/17)) ([eafb8e4](https://github.com/frapercan/PROTEA/commit/eafb8e40f2bef7ab82cb5bc45d24f0a0a957de8b))
* **lafa:** F-LAFA.4 Dockerfile + GHA workflow for ghcr.io image ([#18](https://github.com/frapercan/PROTEA/issues/18)) ([e852dc7](https://github.com/frapercan/PROTEA/commit/e852dc7236ae0b4764b1e686248541827c3a6bef))

## [0.5.0](https://github.com/frapercan/PROTEA/compare/v0.4.0...v0.5.0) (2026-05-06)


### Features

* **anc2vec:** F2C.5.a shim protea.core.anc2vec_embeddings → protea_method.anc2vec ([e88ff12](https://github.com/frapercan/PROTEA/commit/e88ff1264107546770df7fe802a23288f212452c))
* F2C.5.a expand shims (pca_cache, knn_search, reranker pure helpers) ([2e151f9](https://github.com/frapercan/PROTEA/commit/2e151f9966f0fa240856d823beab0f4c646fd683))
* F2C.5.a feature_enricher shim with DB-load adapter (-353 LOC) ([547f2c5](https://github.com/frapercan/PROTEA/commit/547f2c577fbc9def2b42b2b9284b51312e17ef94))
* **F2C.5:** wire PROTEA to protea-method (anc2vec shim first) ([e4633b1](https://github.com/frapercan/PROTEA/commit/e4633b15f13a5e0dfd599d19496091ff471e39f4))

## [0.4.0](https://github.com/frapercan/PROTEA/compare/v0.3.0...v0.4.0) (2026-05-06)


### Features

* **api:** F2B.1-3 plugin registry endpoints ([a80ef8f](https://github.com/frapercan/PROTEA/commit/a80ef8f92eeb57a1dff283a90c617e0592bd1075))
* **benchmark:** expose K as a filter axis + align presets/labels in YAML ([7559899](https://github.com/frapercan/PROTEA/commit/755989932f45bee1c12b1def23d8c5bb4e3c857e))
* **boundary:** T1.8 column invariant on export + canonical sha on inference ([5019100](https://github.com/frapercan/PROTEA/commit/50191007d93cf903d497bf1a77d696f55ff23e30))
* **ci:** D29 release-please scaffolding ([2f01616](https://github.com/frapercan/PROTEA/commit/2f016161ffe198cf23011cd74777e7a516944972))
* **ci:** D29 release-please scaffolding (Conventional Commits driving version + CHANGELOG) ([a301a28](https://github.com/frapercan/PROTEA/commit/a301a28f4b74293e11729366df4cec00cfd1623c))
* **ci:** T0.4 add security workflow (pip-audit + bandit) ([780d3cf](https://github.com/frapercan/PROTEA/commit/780d3cf87d23e505c35ffad64b989e5c42423822))
* **config:** T-CONF.2 add APILimits category ([15bc87e](https://github.com/frapercan/PROTEA/commit/15bc87e276e14419965128071b64e55c954f7c9d))
* **config:** T-CONF.2 add OperationTuning category ([54a57f8](https://github.com/frapercan/PROTEA/commit/54a57f83132af2445593d698bdbd255b44a7dc1b))
* **config:** T-CONF.2 add WorkerTuning category ([ef13fac](https://github.com/frapercan/PROTEA/commit/ef13faccace54e141065aad6d4ae32b0d02d407c))
* **config:** T-CONF.2 skeleton - QueueTuning externalised ([efcdda5](https://github.com/frapercan/PROTEA/commit/efcdda54e5bd3bba3af767e4a4d497455956b253))
* consolidate refactor — protea-reranker-lab integration (phases 1-6) ([ace4c4a](https://github.com/frapercan/PROTEA/commit/ace4c4a10bc176f2491c11a052f16cdb85606cfd))
* **domain:** introduce Aspect enum (Replace Type Code with Class) ([a534c7d](https://github.com/frapercan/PROTEA/commit/a534c7d28ba9b1dfe774372c7239938c23e32340))
* **eval:** groundtruth URI on EvaluationSet + serialise eval workers ([9bc9823](https://github.com/frapercan/PROTEA/commit/9bc9823ab55b47f81a21d2b764cff9c9d0da0309))
* **lab:** integrate protea-reranker-lab — Dataset + ArtifactStore + decoupled training ([0e0b90a](https://github.com/frapercan/PROTEA/commit/0e0b90a1b57098185d895d3d9e601644cc42d7ea))
* **operation:** T0.2 introduce make_safe_emit wrapper ([3155bbd](https://github.com/frapercan/PROTEA/commit/3155bbda5a8b328fd874d143df70552b5f1555b4))
* **predict,scoring:** enable feature flags by default; raise IEA weight 0.3→0.8 ([131f357](https://github.com/frapercan/PROTEA/commit/131f35716aa61607c72dd94e4615582db6dd3e78))
* **predict:** ancestor expansion + chunked store_predictions ([b2f4600](https://github.com/frapercan/PROTEA/commit/b2f46005a7ee6e6229eaa6810121a98a515a8af7))
* **retry:** T0.3+T0.8 retry middleware + BaseWorker extract ([2973fcf](https://github.com/frapercan/PROTEA/commit/2973fcf9613f4db5a710865cd2dea6fc4dc5cdd4))
* **scoring:** add vote_fraction signal, redesign presets, add 409 coverage guard ([8803c5c](https://github.com/frapercan/PROTEA/commit/8803c5ced6acbe29f6c063ed324cd4926c67ae0f))
* **scripts:** T0.5 add smoke.sh end-to-end check ([ca25beb](https://github.com/frapercan/PROTEA/commit/ca25beba2cfe9f4683df40a5720fbbb4623e8155))
* **secrets:** D28 sops + age scaffolding ([b51091e](https://github.com/frapercan/PROTEA/commit/b51091e347dc6a983130dbc6ebbe5d4372275aaa))
* **secrets:** D28 sops + age scaffolding (config, schema template, runbook) ([15281d6](https://github.com/frapercan/PROTEA/commit/15281d628338dd68031f0ed31d2347ea39678dc7))
* **ui:** show test_fmax and best_iteration in reranker card ([e76635a](https://github.com/frapercan/PROTEA/commit/e76635a7986057572cb3414353e7647736f0da62))


### Bug Fixes

* **cafa-eval:** forward neighbor_vote_fraction to compute_score ([e6f0ab6](https://github.com/frapercan/PROTEA/commit/e6f0ab614bb26abb19082c968bddf6a2883b726e))
* **ci:** restore main to green after ~6 weeks of red ([ccecf8a](https://github.com/frapercan/PROTEA/commit/ccecf8a2fc8328714f707efa1c77679880ce8b20))
* **eval:** cafa eval — artifact_uri fallback + GT restriction ([a76d3cf](https://github.com/frapercan/PROTEA/commit/a76d3cf86c5ff05768fdc05f54215a3312e906f7))
* **eval:** persist + thread categorical-code maps from lab → predict() ([34ff468](https://github.com/frapercan/PROTEA/commit/34ff4685d669e2f2aa11abb01469e368fe8948b1))
* **perf:** list_prediction_sets — replace correlated subquery with GROUP BY ([5d89a88](https://github.com/frapercan/PROTEA/commit/5d89a8818e7f27c3511e96d44d2d1e365b3beb83))
* **queue:** increase publisher retry to 12 attempts (~4 min broker downtime) ([e299672](https://github.com/frapercan/PROTEA/commit/e2996720099ad1c5a2da4cbeb726f031c2551823))
* **reranker-models:** tolerate cross-instance FKs in /import ([ef9d45b](https://github.com/frapercan/PROTEA/commit/ef9d45b2e7bca79b67118c8d46cb4d1faa725064))
* **reranker:** key the in-process booster cache by URI, not schema_sha ([4631bdf](https://github.com/frapercan/PROTEA/commit/4631bdf1259db902e593366a90dd1932be84ad7f))
* **scoring:** reranker-metrics endpoint — DetachedInstance + reconciled mode ([ae497db](https://github.com/frapercan/PROTEA/commit/ae497db4ed701008cdb6507c9f443f4b2c23b097))
* **scoring:** wire ArtifactStore-backed boosters into rerank.tsv + metrics ([5db5301](https://github.com/frapercan/PROTEA/commit/5db53015d58f43b4ebd7e0bf917b6b68396afd10))
* **tests:** T0.1b clear preexisting BROKEN suite ([9ecc521](https://github.com/frapercan/PROTEA/commit/9ecc521e3a553675ebee1b18b7b1a1b964c393fe))
* **train_reranker:** filter rows by per-aspect cat membership before labelling ([223299c](https://github.com/frapercan/PROTEA/commit/223299cf6acd91e565531fdd52ac1a4e3e7e4f64))
* **train:** share PCA state with predict via cache ([26b4cc6](https://github.com/frapercan/PROTEA/commit/26b4cc6cc02e05335e4cd22316f2d25343b4bb5a))
* **workers:** raise reaper hard-timeout 6h→24h; add cross-scoring launcher ([ae1a925](https://github.com/frapercan/PROTEA/commit/ae1a925f18e2e1b455b0f1ddefbf0716e0516eea))


### Performance Improvements

* **annotations:** Flyweight string-intern qualifier + evidence_code ([53bbe5c](https://github.com/frapercan/PROTEA/commit/53bbe5c13136b70adfe8f4560ec23af02d192275))
* **predictions:** reduce STORE_CHUNK_SIZE 25k to 10k for broker memory pressure ([df73f6e](https://github.com/frapercan/PROTEA/commit/df73f6e4ed1cde75eba1e195343091072ddd9ecc))


### Documentation

* **adr:** add 30 strategic ADR stubs (D1-D30) covering master plan v3 ([7db0e0d](https://github.com/frapercan/PROTEA/commit/7db0e0d792048e13c9c27344f1d3a4ef1259633b))
* **adr:** add ADR-008 for cafaeval PK coverage fix ([81260be](https://github.com/frapercan/PROTEA/commit/81260be9770bcf5df8be70d25b8a76c369ff785a))
* **config:** T-CONF.1 hardcoded params inventory ([907f104](https://github.com/frapercan/PROTEA/commit/907f104352903e9aa690903bd0f110dd4cd33bdb))
* **config:** T-CONF.3 add Tuning settings section to configuration.rst ([cb24813](https://github.com/frapercan/PROTEA/commit/cb24813a4f276d69e2e9896cba46d91f618f3e7f))
* **core:** drop autodoc reference to deleted protea.core.reranking ([fc01d3f](https://github.com/frapercan/PROTEA/commit/fc01d3fea3c75f421e6b6eb7e1228488c6ee639e))
* Doc-T11 add "5 minutes to first job" section to README ([e9ae748](https://github.com/frapercan/PROTEA/commit/e9ae748b78d29569d44c729f190bd67750f3e3c2))
* Doc-T7 add top-level plugin author guide ([37379af](https://github.com/frapercan/PROTEA/commit/37379af1281404e5e581ab3106d26e20f867f7a2))
* **planning:** add lab decoupling plan ([5b3f19d](https://github.com/frapercan/PROTEA/commit/5b3f19de83af2efa03fc261194de0a9852a613cb))
* **reference:** Doc-T3 reference/core.rst cleanup + post-F1 modules ([cffe1c3](https://github.com/frapercan/PROTEA/commit/cffe1c39e0d5a399cd844cf574e8da31f7d3c366))
* sync narrative with code — queue routing, lab decoupling, IA URL ([9caf64d](https://github.com/frapercan/PROTEA/commit/9caf64d81ba80d24f2fa6f17aa033c5993148ff6))
