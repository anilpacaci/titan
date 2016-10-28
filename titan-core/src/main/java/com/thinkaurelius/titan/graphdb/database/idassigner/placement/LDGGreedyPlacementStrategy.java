package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

@PreInitializeConfigOptions
public class LDGGreedyPlacementStrategy implements IDPlacementStrategy {
	
    private static final Logger log =
            LoggerFactory.getLogger(LDGGreedyPlacementStrategy.class);
    
    public static final ConfigOption<Integer> CONCURRENT_PARTITIONS = new ConfigOption<Integer>(GraphDatabaseConfiguration.IDS_NS,
            "num-partitions","Number of partition block to allocate for placement of vertices", ConfigOption.Type.MASKABLE,10);
    
    public static final ConfigOption<Integer> TOTAL_CAPACITY = new ConfigOption<Integer>(GraphDatabaseConfiguration.IDS_NS,
            "total-capacity","Total size for all partitions", ConfigOption.Type.MASKABLE,10);

	@Override
	public int getPartition(InternalElement element) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void getPartitions(Map<InternalVertex, PartitionAssignment> vertices) {
		// TODO Auto-generated method stub

	}

	@Override
	public void injectIDManager(IDManager idManager) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean supportsBulkPlacement() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void setLocalPartitionBounds(List<PartitionIDRange> localPartitionIdRanges) {
		// TODO Auto-generated method stub

	}

	@Override
	public void exhaustedPartition(int partitionID) {
		// TODO Auto-generated method stub

	}

}
