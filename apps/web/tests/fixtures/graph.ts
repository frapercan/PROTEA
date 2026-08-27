// The graph exactly as GET /v1/graph answered on the record this page was
// built against, captured from the endpoint rather than invented.
//
// Written as a typed literal on purpose. A JSON import behind a cast would
// accept a payload whose shape had drifted; this stops compiling the day a
// field is renamed or a strength value the client cannot draw appears, and
// the drift surfaces here instead of on the page.
//
// The state it captures is worth reading before changing it. The frame is
// NOT declared even though every one of its fields is populated, because
// no result row seals to it; nine panels carry populations and eight
// levels each; one node is measured against nothing, five are blocked, and
// the substrate has one level instantiated out of thirteen available. Each
// of those is a case the page has to render honestly.

import type { GraphResponse } from "@/lib/graph";

export const GRAPH_FIXTURE: GraphResponse = {
  frame: {
    declared: false,
    evaluation_set_id: "c9daabc4-19bd-4c6b-8da5-c19e4bd880c4",
    window: "220->227",
    window_span: null,
    window_role: "valid",
    mode: "reconciled",
    pivot_snapshot: {
      id: "a24e7d91-1236-4a18-a3ac-aadc36222e8b",
      version: "releases/2025-07-22",
    },
    information_accretion_set: {
      id: "f91e31b2-d57b-410d-87a1-d016f5969f1f",
      regime: "lafa",
      sha256: "308fc28ef3df566658a9e3cbe28a0c72b41000e0e1e4eec6ebfca2801c38b55e",
    },
    query_set: {
      id: "0c00f7b2-e7c9-4bb5-af2b-21b662c83821",
      name: "instance-zero-220-227-valid",
      entries: 14032,
    },
    sealed_rows: 0,
    unsealed_rows: 8,
  },
  nodes: [
    {
      key: "frame",
      held: [],
      title: "Frame",
      stage: 0,
      question: "Which window, pivot and accretion regime every number is read in.",
      strength: "chosen",
      levels_instantiated: 1,
      levels_available: 1,
      varying_fields: [],
      constant_fields: [
        "window_from",
        "window_to",
        "window_role",
        "mode",
        "pivot_version",
      ],
      blocked_reason: "One accretion table is built on the declared pivot and it is the one every published result was weighted by, so the frame fixes the regime rather than leaving it open.",
      results: 8,
    },
    {
      key: "substrate",
      held: [],
      title: "Substrate",
      stage: 1,
      question: "Which representation the neighbourhood is computed in.",
      strength: "inherited",
      levels_instantiated: 1,
      levels_available: 13,
      varying_fields: [],
      constant_fields: [
        "model_name",
        "model_backend",
        "layer_indices",
        "layer_agg",
        "pooling",
        "normalize",
        "normalize_residues",
        "max_length",
        "use_chunking",
        "chunk_size",
        "chunk_overlap",
      ],
      blocked_reason: "13 representations hold stored embeddings and 1 has ever been retrieved against (esm2_650m). The rest are built and were never tried, and nothing in the frame selects the one that was.",
      results: 8,
    },
    {
      key: "bank",
      held: [],
      title: "Bank",
      stage: 2,
      question: "Which corpus the donors come from, and under which donor policy.",
      strength: "inherited",
      levels_instantiated: 1,
      levels_available: 1,
      varying_fields: [],
      constant_fields: [
        "bank_source",
        "bank_version",
        "donor_reviewed_only",
        "donor_evidence_codes",
        "donor_exclusions",
      ],
      blocked_reason: "The corpus is the window's lower endpoint, which the frame fixes. The donor policy in force is the empty one: no reviewed-only restriction, no evidence filter, no excluded reference prefixes. That is the value the field holds when nobody sets it, and corpus and policy cannot be decided apart.",
      results: 8,
    },
    {
      key: "retriever",
      held: [],
      title: "Retriever",
      stage: 3,
      question: "How candidates are drawn from the bank, and how deep.",
      strength: "inherited",
      levels_instantiated: 1,
      levels_available: 1,
      varying_fields: [],
      constant_fields: [
        "depth",
        "distance_threshold",
        "metric",
        "search_backend",
        "aspect_separated",
        "expand_to_ancestors",
      ],
      blocked_reason: "Every candidate the record can hold carries a donor accession, because the column is NOT NULL: a term that arrived without one cannot be stored here at all. 752786 candidates sit under 1 retrieval setting at depth 10. No registry of retrievers exists, so there was never a level to choose against: the setting is the one it has always been.",
      results: 8,
    },
    {
      key: "generator",
      held: [],
      title: "Generator",
      stage: 4,
      question: "Whether any candidate arrives without a donor.",
      strength: "blocked",
      levels_instantiated: 0,
      levels_available: 0,
      varying_fields: [],
      constant_fields: [],
      blocked_reason: "No candidate arrives without a donor: the candidate column naming the donor is NOT NULL, so a term that arrived without one has nowhere to be written, and the artifacts a second source would draw on are absent. interpro_annotation holds 0 rows, interpro_go_mapping 0, reranker_model 0.",
      results: 0,
    },
    {
      key: "scoring",
      held: [],
      title: "Scoring",
      stage: 5,
      question: "Which weighting turns a candidate into a score.",
      strength: "chosen",
      levels_instantiated: 8,
      levels_available: 8,
      varying_fields: [
        "formula",
        "weights",
      ],
      constant_fields: [
        "evidence_weights",
        "params",
      ],
      blocked_reason: "8 weightings scored the same candidates in the same frame, so the contrast is real and its spread is in the panels below. No floor is declared for it anywhere in the record, so the spread cannot be called a separation.",
      results: 8,
    },
    {
      key: "features",
      held: [],
      title: "Features",
      stage: 6,
      question: "Which per-candidate features enter a model.",
      strength: "blocked",
      levels_instantiated: 0,
      levels_available: 3,
      varying_fields: [],
      constant_fields: [],
      blocked_reason: "Candidates carry the feature families their run asked for (compute_alignments, compute_reranker_features, compute_taxonomy), but choosing among them is a decision only a consumer makes and there is none: reranker_model holds 0 rows.",
      results: 0,
    },
    {
      key: "reranking",
      held: [],
      title: "Re-ranking",
      stage: 7,
      question: "Whether a model reorders the candidates.",
      strength: "blocked",
      levels_instantiated: 0,
      levels_available: 0,
      varying_fields: [],
      constant_fields: [],
      blocked_reason: "No model exists to reorder with: reranker_model holds 0 rows and 0 published results name one. The candidate order is the retriever's, unchanged.",
      results: 0,
    },
    {
      key: "combination",
      held: [],
      title: "Combination",
      stage: 8,
      question: "How two or more flows are merged into one answer.",
      strength: "blocked",
      levels_instantiated: 0,
      levels_available: 1,
      varying_fields: [],
      constant_fields: [],
      blocked_reason: "1 flow is instantiated, so there is nothing to combine. Every prediction set draws from a single kind of source (goa) through a single propagation mechanism, donor transfer, which is the only one the candidate schema can express.",
      results: 0,
    },
    {
      key: "routing",
      held: [],
      title: "Routing",
      stage: 9,
      question: "Which flow answers which panel.",
      strength: "blocked",
      levels_instantiated: 0,
      levels_available: 1,
      varying_fields: [],
      constant_fields: [],
      blocked_reason: "Routing picks a flow per panel and there is 1 of them, so every panel has the same answer by construction. It is downstream of combination, which is blocked for the same reason.",
      results: 0,
    },
  ],
  panels: [
    {
      category: "NK",
      aspect: "BPO",
      units: 1509,
      detectable_effect: null,
      results: [
        {
          level: "composite_no_embedding",
          f_micro_w: 0.2652,
          tau: 0.43,
        },
        {
          level: "composite",
          f_micro_w: 0.2644,
          tau: 0.66,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.2532,
          tau: 0.48,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.2529,
          tau: 0.73,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.2346,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.2343,
          tau: 0.31,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.1691,
          tau: 0.8,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.1682,
          tau: 0.99,
        },
      ],
    },
    {
      category: "NK",
      aspect: "MFO",
      units: 1129,
      detectable_effect: null,
      results: [
        {
          level: "composite_no_embedding",
          f_micro_w: 0.4555,
          tau: 0.51,
        },
        {
          level: "composite",
          f_micro_w: 0.4552,
          tau: 0.7,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.4485,
          tau: 0.44,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.448,
          tau: 0.72,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.4102,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.4032,
          tau: 0.31,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.3269,
          tau: 0.8,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.3056,
          tau: 0.99,
        },
      ],
    },
    {
      category: "NK",
      aspect: "CCO",
      units: 1116,
      detectable_effect: null,
      results: [
        {
          level: "composite",
          f_micro_w: 0.4313,
          tau: 0.68,
        },
        {
          level: "composite_no_embedding",
          f_micro_w: 0.4312,
          tau: 0.49,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.4059,
          tau: 0.49,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.4059,
          tau: 0.74,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.3812,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.3792,
          tau: 0.41,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.2904,
          tau: 0.99,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.2869,
          tau: 0.8,
        },
      ],
    },
    {
      category: "LK",
      aspect: "BPO",
      units: 1214,
      detectable_effect: null,
      results: [
        {
          level: "composite_no_embedding",
          f_micro_w: 0.3426,
          tau: 0.49,
        },
        {
          level: "composite",
          f_micro_w: 0.342,
          tau: 0.69,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.3392,
          tau: 0.6,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.3381,
          tau: 0.8,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.2936,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.2847,
          tau: 0.31,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.1858,
          tau: 0.99,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.1812,
          tau: 0.8,
        },
      ],
    },
    {
      category: "LK",
      aspect: "MFO",
      units: 943,
      detectable_effect: null,
      results: [
        {
          level: "composite_no_embedding",
          f_micro_w: 0.382,
          tau: 0.42,
        },
        {
          level: "composite",
          f_micro_w: 0.3819,
          tau: 0.65,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.3793,
          tau: 0.33,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.3787,
          tau: 0.68,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.3493,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.3427,
          tau: 0.21,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.2414,
          tau: 0.8,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.2319,
          tau: 0.99,
        },
      ],
    },
    {
      category: "LK",
      aspect: "CCO",
      units: 821,
      detectable_effect: null,
      results: [
        {
          level: "composite",
          f_micro_w: 0.4494,
          tau: 0.72,
        },
        {
          level: "composite_no_embedding",
          f_micro_w: 0.4488,
          tau: 0.54,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.4338,
          tau: 0.74,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.4328,
          tau: 0.81,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.4052,
          tau: 0.7,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.3969,
          tau: 0.41,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.2833,
          tau: 0.8,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.2812,
          tau: 0.99,
        },
      ],
    },
    {
      category: "PK",
      aspect: "BPO",
      units: 5800,
      detectable_effect: null,
      results: [
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.1146,
          tau: 0.65,
        },
        {
          level: "composite_no_embedding",
          f_micro_w: 0.1144,
          tau: 0.48,
        },
        {
          level: "composite",
          f_micro_w: 0.114,
          tau: 0.68,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.1137,
          tau: 0.41,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.0994,
          tau: 0.61,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.0988,
          tau: 0.8,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.0662,
          tau: 0.8,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.0658,
          tau: 0.99,
        },
      ],
    },
    {
      category: "PK",
      aspect: "MFO",
      units: 3284,
      detectable_effect: null,
      results: [
        {
          level: "composite",
          f_micro_w: 0.246,
          tau: 0.68,
        },
        {
          level: "composite_no_embedding",
          f_micro_w: 0.2457,
          tau: 0.47,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.2433,
          tau: 0.59,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.2427,
          tau: 0.79,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.229,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.224,
          tau: 0.31,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.1675,
          tau: 0.99,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.167,
          tau: 0.8,
        },
      ],
    },
    {
      category: "PK",
      aspect: "CCO",
      units: 3201,
      detectable_effect: null,
      results: [
        {
          level: "composite_no_embedding",
          f_micro_w: 0.2524,
          tau: 0.49,
        },
        {
          level: "composite",
          f_micro_w: 0.2523,
          tau: 0.71,
        },
        {
          level: "alignment_only",
          f_micro_w: 0.2459,
          tau: 0.58,
        },
        {
          level: "embedding_plus_alignment",
          f_micro_w: 0.2449,
          tau: 0.77,
        },
        {
          level: "embedding_plus_vote",
          f_micro_w: 0.2367,
          tau: 0.65,
        },
        {
          level: "vote_fraction",
          f_micro_w: 0.2319,
          tau: 0.31,
        },
        {
          level: "embedding_only",
          f_micro_w: 0.1783,
          tau: 0.99,
        },
        {
          level: "evidence_veto",
          f_micro_w: 0.1696,
          tau: 0.69,
        },
      ],
    },
  ],
  blocked: [
    {
      node: "generator",
      what: "a candidate that owes nothing to a donor",
      why: "No candidate arrives without a donor: the candidate column naming the donor is NOT NULL, so a term that arrived without one has nowhere to be written, and the artifacts a second source would draw on are absent. interpro_annotation holds 0 rows, interpro_go_mapping 0, reranker_model 0.",
      precondition: "rows in interpro_annotation and interpro_go_mapping",
    },
    {
      node: "features",
      what: "a selection of features to feed a model",
      why: "Candidates carry the feature families their run asked for (compute_alignments, compute_reranker_features, compute_taxonomy), but choosing among them is a decision only a consumer makes and there is none: reranker_model holds 0 rows.",
      precondition: "a row in reranker_model to consume them",
    },
    {
      node: "reranking",
      what: "a model that reorders candidates",
      why: "No model exists to reorder with: reranker_model holds 0 rows and 0 published results name one. The candidate order is the retriever's, unchanged.",
      precondition: "a row in reranker_model",
    },
    {
      node: "combination",
      what: "a rule for merging two flows",
      why: "1 flow is instantiated, so there is nothing to combine. Every prediction set draws from a single kind of source (goa) through a single propagation mechanism, donor transfer, which is the only one the candidate schema can express.",
      precondition: "a second flow, which the generator node has to produce first",
    },
    {
      node: "routing",
      what: "a per-panel choice of flow",
      why: "Routing picks a flow per panel and there is 1 of them, so every panel has the same answer by construction. It is downstream of combination, which is blocked for the same reason.",
      precondition: "a combination of two or more flows",
    },
  ],
};

/** The same endpoint, answering for a record with nothing in it. */
export const EMPTY_GRAPH_FIXTURE: GraphResponse = {
  frame: {
    declared: false,
    evaluation_set_id: null,
    window: null,
    window_span: null,
    window_role: null,
    mode: null,
    pivot_snapshot: null,
    information_accretion_set: null,
    query_set: null,
    sealed_rows: 0,
    unsealed_rows: 0,
  },
  nodes: [],
  panels: [],
  blocked: [],
};
