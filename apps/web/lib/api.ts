export type Job = {
  id: string;
  operation: string;
  queue_name: string;
  status: string;
  parent_job_id?: string | null;
  created_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
  progress_current?: number | null;
  progress_total?: number | null;
  error_code?: string | null;
  error_message?: string | null;
};

export type JobEvent = {
  id: number;
  ts: string;
  level: "info" | "warning" | "error";
  event: string;
  message: string | null;
  fields: Record<string, any>;
};

export function baseUrl(): string {
  const u = process.env.NEXT_PUBLIC_API_URL;
  if (!u) throw new Error("NEXT_PUBLIC_API_URL is not set");
  return u.replace(/\/+$/, "");
}

async function http<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${baseUrl()}${path}`, { cache: "no-store", ...init });
  if (!res.ok) throw new Error(await res.text());
  return await res.json();
}

export function listJobs(params?: { limit?: number; status?: string; operation?: string; parent_job_id?: string }) {
  const q = new URLSearchParams();
  q.set("limit", String(params?.limit ?? 50));
  if (params?.status) q.set("status", params.status);
  if (params?.operation) q.set("operation", params.operation);
  if (params?.parent_job_id) q.set("parent_job_id", params.parent_job_id);
  return http<Job[]>(`/jobs?${q.toString()}`);
}

export function getJob(id: string) {
  return http<any>(`/jobs/${id}`);
}

export function getJobEvents(id: string, limit = 200) {
  return http<JobEvent[]>(`/jobs/${id}/events?limit=${limit}`);
}

export function createJob(body: {
  operation: string;
  queue_name: string;
  payload?: Record<string, any>;
  meta?: Record<string, any>;
}) {
  return http<{ id: string; status: string }>(`/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteJob(id: string) {
  return http<{ deleted: string }>(`/jobs/${id}`, { method: "DELETE" });
}

export function cancelJob(id: string) {
  return http<{ id: string; status: string }>(`/jobs/${id}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}",
  });
}

export type EmbeddingConfig = {
  id: string;
  model_name: string;
  model_backend: string;
  layer_indices: number[];
  layer_agg: string;
  pooling: string;
  normalize_residues: boolean;
  normalize: boolean;
  max_length: number;
  use_chunking: boolean;
  chunk_size: number;
  chunk_overlap: number;
  description?: string | null;
  created_at: string;
  embedding_count?: number;
};

export type PredictionSet = {
  id: string;
  embedding_config_id: string;
  annotation_set_id: string;
  ontology_snapshot_id: string;
  limit_per_entry: number;
  distance_threshold?: number | null;
  created_at: string;
  prediction_count?: number;
};

export type AnnotationSet = {
  id: string;
  source: string;
  source_version?: string | null;
  ontology_snapshot_id: string;
  job_id?: string | null;
  created_at: string;
  meta?: Record<string, any>;
  annotation_count?: number;
};

export type OntologySnapshot = {
  id: string;
  obo_url: string;
  obo_version: string;
  loaded_at: string;
  go_term_count?: number;
};

export type ProteinItem = {
  accession: string;
  entry_name?: string | null;
  gene_name?: string | null;
  organism?: string | null;
  taxonomy_id?: string | null;
  length?: number | null;
  reviewed?: boolean | null;
  is_canonical: boolean;
  isoform_index?: number | null;
};

export type ProteinDetail = ProteinItem & {
  canonical_accession: string;
  isoforms: string[];
  sequence_id?: number | null;
  embedding_count: number;
  go_annotation_count: number;
  metadata?: {
    function_cc?: string | null;
    ec_number?: string | null;
    catalytic_activity?: string | null;
    pathway?: string | null;
    keywords?: string | null;
    cofactor?: string | null;
    activity_regulation?: string | null;
    [key: string]: string | null | undefined;
  } | null;
};

export type ProteinStats = {
  total: number;
  canonical: number;
  isoforms: number;
  reviewed: number;
  unreviewed: number;
  with_metadata: number;
  with_embeddings: number;
  with_go_annotations: number;
};

export function getProteinStats() {
  return http<ProteinStats>(`/proteins/stats`);
}

export function listProteins(params?: {
  search?: string;
  reviewed?: boolean;
  canonical_only?: boolean;
  limit?: number;
  offset?: number;
}) {
  const q = new URLSearchParams();
  if (params?.search) q.set("search", params.search);
  if (params?.reviewed !== undefined) q.set("reviewed", String(params.reviewed));
  if (params?.canonical_only !== undefined) q.set("canonical_only", String(params.canonical_only));
  if (params?.limit !== undefined) q.set("limit", String(params.limit));
  if (params?.offset !== undefined) q.set("offset", String(params.offset));
  return http<{ total: number; offset: number; limit: number; items: ProteinItem[] }>(
    `/proteins?${q.toString()}`
  );
}

export function getProtein(accession: string) {
  return http<ProteinDetail>(`/proteins/${encodeURIComponent(accession)}`);
}

export function listEmbeddingConfigs() {
  return http<EmbeddingConfig[]>(`/embeddings/configs`);
}

export function createEmbeddingConfig(body: Omit<EmbeddingConfig, "id" | "created_at" | "embedding_count" | "use_chunking" | "chunk_size" | "chunk_overlap" | "normalize_residues"> & {
  normalize_residues?: boolean;
  use_chunking?: boolean;
  chunk_size?: number;
  chunk_overlap?: number;
}) {
  return http<EmbeddingConfig>(`/embeddings/configs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function deleteEmbeddingConfig(id: string) {
  return http<{ deleted: string }>(`/embeddings/configs/${id}`, { method: "DELETE" });
}

export function launchPredictGoTerms(body: {
  embedding_config_id: string;
  annotation_set_id: string;
  ontology_snapshot_id: string;
  limit_per_entry?: number;
  distance_threshold?: number | null;
  batch_size?: number;
  query_accessions?: string[] | null;
  query_set_id?: string | null;
  // Search backend
  search_backend?: string;
  metric?: string;
  faiss_index_type?: string;
  faiss_nlist?: number;
  faiss_nprobe?: number;
  faiss_hnsw_m?: number;
  faiss_hnsw_ef_search?: number;
  // Feature engineering
  compute_alignments?: boolean;
  compute_taxonomy?: boolean;
}) {
  return http<{ id: string; status: string }>(`/embeddings/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function listPredictionSets() {
  return http<PredictionSet[]>(`/embeddings/prediction-sets`);
}

export function getPredictionSet(id: string) {
  return http<PredictionSet & { query_set_id?: string | null }>(`/embeddings/prediction-sets/${id}`);
}

export function getPredictionSetProteins(setId: string, params?: { search?: string; limit?: number; offset?: number }) {
  const q = new URLSearchParams();
  if (params?.search) q.set("search", params.search);
  if (params?.limit !== undefined) q.set("limit", String(params.limit));
  if (params?.offset !== undefined) q.set("offset", String(params.offset));
  return http<{
    total: number; offset: number; limit: number;
    items: { accession: string; go_count: number; min_distance: number | null; annotation_count: number; match_count: number; in_db: boolean }[];
  }>(`/embeddings/prediction-sets/${setId}/proteins?${q.toString()}`);
}

export type Prediction = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  distance: number;
  ref_protein_accession: string;
  qualifier: string | null;
  evidence_code: string | null;
  // Alignment — NW
  identity_nw: number | null;
  similarity_nw: number | null;
  alignment_score_nw: number | null;
  gaps_pct_nw: number | null;
  alignment_length_nw: number | null;
  // Alignment — SW
  identity_sw: number | null;
  similarity_sw: number | null;
  alignment_score_sw: number | null;
  gaps_pct_sw: number | null;
  alignment_length_sw: number | null;
  // Lengths
  length_query: number | null;
  length_ref: number | null;
  // Taxonomy
  query_taxonomy_id: number | null;
  ref_taxonomy_id: number | null;
  taxonomic_lca: number | null;
  taxonomic_distance: number | null;
  taxonomic_common_ancestors: number | null;
  taxonomic_relation: string | null;
};

export function getProteinPredictions(setId: string, accession: string) {
  return http<Prediction[]>(`/embeddings/prediction-sets/${setId}/proteins/${encodeURIComponent(accession)}`);
}

export function getGoTermDistribution(setId: string) {
  return http<{
    by_aspect: Record<string, { go_id: string; name: string | null; count: number }[]>;
    aspect_totals: Record<string, number>;
    top_terms: { go_id: string; name: string | null; aspect: string | null; count: number }[];
  }>(`/embeddings/prediction-sets/${setId}/go-terms`);
}

export type ProteinAnnotation = {
  go_id: string;
  name: string | null;
  aspect: string | null;
  qualifier: string | null;
  evidence_code: string | null;
  assigned_by: string | null;
  db_reference: string | null;
  annotation_set_id: string;
  annotation_set_source: string;
  annotation_set_version: string | null;
};

export function getProteinAnnotations(accession: string, annotationSetId?: string) {
  const q = new URLSearchParams();
  if (annotationSetId) q.set("annotation_set_id", annotationSetId);
  return http<ProteinAnnotation[]>(`/proteins/${encodeURIComponent(accession)}/annotations?${q.toString()}`);
}

export function deletePredictionSet(id: string) {
  return http<{ deleted: string; predictions_deleted: number }>(`/embeddings/prediction-sets/${id}`, { method: "DELETE" });
}

export type GoSubgraph = {
  nodes: { id: number; go_id: string; name: string | null; aspect: string | null; is_query: boolean }[];
  edges: { source: number; target: number; relation_type: string }[];
};

export function getGoSubgraph(snapshotId: string, goIds: string[], depth = 3) {
  const q = new URLSearchParams({ go_ids: goIds.join(","), depth: String(depth) });
  return http<GoSubgraph>(`/annotations/snapshots/${snapshotId}/subgraph?${q.toString()}`);
}

export function listAnnotationSets() {
  return http<AnnotationSet[]>(`/annotations/sets`);
}

export function deleteAnnotationSet(id: string) {
  return http<{ deleted: string; annotations_deleted: number }>(`/annotations/sets/${id}`, { method: "DELETE" });
}

export function listOntologySnapshots() {
  return http<OntologySnapshot[]>(`/annotations/snapshots`);
}

export type QuerySet = {
  id: string;
  name: string;
  description?: string | null;
  entry_count: number;
  created_at: string;
  entries?: { accession: string; sequence_id: number }[];
};

export function listQuerySets() {
  return http<QuerySet[]>(`/query-sets`);
}

export function getQuerySet(id: string) {
  return http<QuerySet>(`/query-sets/${id}`);
}

export function deleteQuerySet(id: string) {
  return http<{ deleted: string }>(`/query-sets/${id}`, { method: "DELETE" });
}

export type VacuumSequencesPreview = {
  total_sequences: number;
  orphan_sequences: number;
  referenced_sequences: number;
};

export type VacuumEmbeddingsPreview = {
  total_embeddings: number;
  unindexed_embeddings: number;
  indexed_embeddings: number;
};

export function previewVacuumSequences() {
  return http<VacuumSequencesPreview>(`/maintenance/vacuum-sequences/preview`);
}

export function runVacuumSequences() {
  return http<{ deleted_sequences: number }>(`/maintenance/vacuum-sequences`, { method: "POST" });
}

export function previewVacuumEmbeddings() {
  return http<VacuumEmbeddingsPreview>(`/maintenance/vacuum-embeddings/preview`);
}

export function runVacuumEmbeddings() {
  return http<{ deleted_embeddings: number }>(`/maintenance/vacuum-embeddings`, { method: "POST" });
}

export async function createQuerySet(file: File, name: string, description?: string): Promise<QuerySet> {
  const form = new FormData();
  form.append("file", file);
  form.append("name", name);
  if (description) form.append("description", description);
  const res = await fetch(`${baseUrl()}/query-sets`, { cache: "no-store", method: "POST", body: form });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
