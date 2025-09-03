-- sql/ddl_pgvector.sql
-- Enable pgvector and add embedding columns where needed.
create extension if not exists vector;


alter table public.chunks add column if not exists embedding_384 vector(384);
alter table public.graph  add column if not exists label_embed_384 vector(384);
alter table public.kcs    add column if not exists q_embed_384 vector(384);
alter table public.images add column if not exists caption_embed_384 vector(384);
alter table public.docs   add column if not exists embed_384 vector(384);


create table if not exists public.prototypes (
  prototype_id text primary key,
  doc_id text,
  topic text,
  centroid_384 vector(384),
  size integer default 0,
  data jsonb default '{}'
);


-- Helpful indexes
create index if not exists idx_chunks_vec on public.chunks using ivfflat (embedding_384 vector_l2_ops) with (lists = 100);
create index if not exists idx_graph_label_vec on public.graph using ivfflat (label_embed_384 vector_l2_ops) with (lists = 50);
create index if not exists idx_docs_vec on public.docs using ivfflat (embed_384 vector_l2_ops) with (lists = 50);
