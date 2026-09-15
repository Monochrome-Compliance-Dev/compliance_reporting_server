-- Remove legacy Sequelize-generated indexes that exactly duplicate the
-- explicitly named PTRS Stage indexes retained by the current model.
--
-- CONCURRENTLY keeps the cleanup from blocking normal Stage readers/writers
-- while PostgreSQL removes each redundant physical index.

DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_customer_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_customer_id_ptrs_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_customer_id_ptrs_id_dataset_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_customer_id_ptrs_id_profile_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_customer_id_ptrs_id_profile_id_dataset_id_ro";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_dataset_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_profile_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_ptrs_id";
DROP INDEX CONCURRENTLY IF EXISTS "tbl_ptrs_stage_row_ptrs_id_dataset_id";
