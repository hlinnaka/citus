-- citus_finalize_upgrade_to_citus11() is a helper UDF ensures
-- the upgrade to Citus 11 is finished successfully. Upgrade to
-- Citus 11 requires all active primary worker nodes to get the
-- metadata. And, this function's job is to sync the metadata to
-- the nodes that does not already have
-- once the function finishes without any errors and returns true
-- the cluster is ready for running distributed queries from
-- the worker nodes. When debug is enabled, the function provides
-- more information to the user.
CREATE OR REPLACE FUNCTION pg_catalog.citus_finalize_upgrade_to_citus11()
  RETURNS bool
  LANGUAGE plpgsql
  AS $$
BEGIN

  ---------------------------------------------
  -- This script consists of N stages
  -- Each step is documented, and if log level
  -- is reduced to DEBUG1, each step is logged
  -- as well
  ---------------------------------------------

------------------------------------------------------------------------------------------
  -- STAGE 0: Ensure no concurrent node metadata changing operation happens while this
  -- script is running via acquiring a strong lock on the pg_dist_node
------------------------------------------------------------------------------------------
BEGIN

  LOCK TABLE pg_dist_node IN EXCLUSIVE MODE NOWAIT;

  EXCEPTION WHEN OTHERS THEN
  RAISE 'Another node metadata changing operation is in progress, try again.';
END;
------------------------------------------------------------------------------------------
  -- STAGE 1: Ensure we have the prerequisites
  -- (a) only superuser can run this script
  -- (b) cannot be executed when enable_ddl_propagation is False
  -- (c) can only be executed from the coordinator
------------------------------------------------------------------------------------------
DECLARE
  is_superuser_running boolean := False;
  enable_ddl_prop boolean:= False;
  local_group_id int := 0;
BEGIN
      SELECT rolsuper INTO is_superuser_running FROM pg_roles WHERE rolname = current_user;
      IF is_superuser_running IS NOT True THEN
                RAISE EXCEPTION 'This operation can only be initiated by superuser';
      END IF;

      SELECT current_setting('citus.enable_ddl_propagation') INTO enable_ddl_prop;
      IF enable_ddl_prop IS NOT True THEN
                RAISE EXCEPTION 'This operation cannot be completed when citus.enable_ddl_propagation is False.';
      END IF;

      SELECT groupid INTO local_group_id FROM pg_dist_local_group;

      IF local_group_id != 0 THEN
                RAISE EXCEPTION 'Operation is not allowed on this node. Connect to the coordinator and run it again.';
      ELSE
                RAISE DEBUG 'We are on the coordinator, continue to sync metadata';
      END IF;
END;


  ------------------------------------------------------------------------------------------
    -- STAGE 2: Ensure all primary nodes are active
  ------------------------------------------------------------------------------------------
  DECLARE
    primary_disabled_worker_node_count int := 0;
  BEGIN
        SELECT count(*) INTO primary_disabled_worker_node_count FROM pg_dist_node
                WHERE groupid != 0 AND noderole = 'primary' AND NOT isactive;

        IF primary_disabled_worker_node_count != 0 THEN
                  RAISE EXCEPTION 'There are inactive primary worker nodes, you need to activate the nodes first.'
                                  'Use SELECT citus_activate_node() to activate the disabled nodes';
        ELSE
                  RAISE DEBUG 'There are no disabled worker nodes, continue to sync metadata';
        END IF;
  END;

  ------------------------------------------------------------------------------------------
    -- STAGE 3: Ensure there is no connectivity issues in the cluster
  ------------------------------------------------------------------------------------------
  DECLARE
    all_nodes_can_connect_to_each_other boolean := False;
  BEGIN
       SELECT bool_and(coalesce(result, false)) INTO all_nodes_can_connect_to_each_other FROM citus_check_cluster_node_health();

        IF all_nodes_can_connect_to_each_other != True THEN
                  RAISE EXCEPTION 'There are unhealth primary nodes, you need to ensure all '
                                  'nodes are up and runnnig. Also, make sure that all nodes can connect '
                                  'to each other. Use SELECT * FROM citus_check_cluster_node_health(); '
                                  'to check the cluster health';
        ELSE
                  RAISE DEBUG 'Cluster is healthy, all nodes can connect to each other';
        END IF;
  END;

  ------------------------------------------------------------------------------------------
    -- STAGE 4: Ensure all the partitioned tables have the proper naming structure
    -- As described on https://github.com/citusdata/citus/issues/4962
    -- existing indexes on partitioned distributed tables can collide
    -- with the index names exists on the shards
    -- luckily, we know how to fix it.
    -- And, note that we should do this even if the cluster is a basic plan
    -- (e.g., single node Citus) such that when cluster scaled out, everything
    -- works as intended
    -- And, this should be done only ONCE for a cluster as it can be a pretty
    -- time consuming operation. Thus, even if the function is called multiple time,
    -- we keep track of it and do not re-execute this part if not needed.
  ------------------------------------------------------------------------------------------
  DECLARE
      partitioned_table_exists_pre_11 boolean:=False;
  BEGIN

    -- we recorded if partitioned tables exists during upgrade to Citus 11
    SELECT metadata->>'partitioned_citus_table_exists_pre_11' INTO partitioned_table_exists_pre_11
    FROM pg_dist_node_metadata;

    IF partitioned_table_exists_pre_11 IS NOT NULL AND partitioned_table_exists_pre_11 THEN

      -- this might take long depending on the number of partitions and shards...
      RAISE NOTICE 'Preparing all the existing partitioned table indexes';
      SELECT pg_catalog.fix_all_partition_shard_index_names();

      -- great, we are done with fixing the existing wrong index names
      -- so, lets remove this
      UPDATE pg_dist_node_metadata
      SET metadata=jsonb_delete(metadata, 'partitioned_citus_table_exists_pre_11');
    ELSE
        RAISE DEBUG 'There are no partitioned tables that should be fixed';
    END IF;
  END;

  ------------------------------------------------------------------------------------------
  -- STAGE 5: Return early if there are no primary worker nodes
  -- We don't strictly need this step, but it gives a nicer notice message
  ------------------------------------------------------------------------------------------
  DECLARE
    primary_worker_node_count bigint :=0;
  BEGIN
        SELECT count(*) INTO primary_worker_node_count FROM pg_dist_node WHERE groupid != 0 AND noderole = 'primary';

        IF primary_worker_node_count = 0 THEN
                  RAISE NOTICE 'There are no primary worker nodes, no need to sync metadata to any node';
                  RETURN true;
        ELSE
                  RAISE DEBUG 'There are % primary worker nodes, continue to sync metadata', primary_worker_node_count;
        END IF;
  END;

  ------------------------------------------------------------------------------------------
  -- STAGE 6: Do the actual metadata & object syncing to the worker nodes
  -- For the "already synced" metadata nodes, we do not strictly need to
  -- sync the objects & metadata, but there is no harm to do it anyway
  -- it'll only cost some execution time but makes sure that we have a
  -- a consistent metadata & objects across all the nodes
  ------------------------------------------------------------------------------------------
  DECLARE
    primary_worker_node_count_without_metadata_synced bigint :=0;
  BEGIN

        SELECT count(*) INTO primary_worker_node_count_without_metadata_synced FROM pg_dist_node WHERE NOT hasmetadata AND groupid != 0 AND noderole = 'primary';

        IF primary_worker_node_count_without_metadata_synced > 0 THEN
        RAISE NOTICE '% number of primary worker nodes has metadata synced missing, so we need to sync the whole', primary_worker_node_count_without_metadata_synced;

          SELECT
           start_metadata_sync_to_node(nodename,nodeport)
          FROM
            pg_dist_node WHERE NOT hasmetadata AND groupid != 0 AND noderole = 'primary';
        ELSE
                  RAISE DEBUG 'All of the % primary worker nodes has metadata synced, so we are done!', primary_worker_node_count_without_metadata_synced;
        END IF;
  END;

  RETURN true;
END;
$$;
COMMENT ON FUNCTION pg_catalog.citus_finalize_upgrade_to_citus11()
  IS 'finalizes upgrade to Citus';

REVOKE ALL ON FUNCTION pg_catalog.citus_finalize_upgrade_to_citus11() FROM PUBLIC;
