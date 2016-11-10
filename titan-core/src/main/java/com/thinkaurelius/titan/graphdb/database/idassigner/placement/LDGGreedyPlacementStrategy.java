package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

@PreInitializeConfigOptions
public class LDGGreedyPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(LDGGreedyPlacementStrategy.class);

	public static final ConfigOption<Integer> TOTAL_CAPACITY = new ConfigOption<Integer>(
			GraphDatabaseConfiguration.CLUSTER_NS, "total-capacity",
			"Total size (number of vertices) for all partitions, only applicable for explicit graph partitioners",
			ConfigOption.Type.MASKABLE, 10);

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history",
			"Placement history Implementation for Greedy Partitioners", ConfigOption.Type.MASKABLE, "inmemory");

	public static final ConfigOption<String> IDS_PLACEMENT_HISTORY_HOSTNAME = new ConfigOption<String>(
			GraphDatabaseConfiguration.IDS_NS, "placement-history-hostname",
			"Memcached Server address for Placement History Implementation", ConfigOption.Type.MASKABLE,
			"localhost:11211");

	private final Random random = new Random();

	private int maxPartitions;
	private int totalCapacity;
	private int partitionCapacity;

	private int counter = 0;

	private List<Integer> availablePartitions;
	private int[] partitionSizes;

	private PlacementHistory placementHistory;

	public LDGGreedyPlacementStrategy(Configuration config) {
		int maxPartitions = config.get(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS);
		int totalCapacity = config.get(TOTAL_CAPACITY);

		Preconditions.checkArgument(totalCapacity > 0 && maxPartitions > 0);

		this.maxPartitions = maxPartitions;
		this.totalCapacity = totalCapacity;
		this.partitionCapacity = totalCapacity / maxPartitions;

		if (config.get(IDS_PLACEMENT_HISTORY).equals(PlacementHistory.MEMCACHED_PLACEMENT_HISTORY)) {
			String hostname = config.get(IDS_PLACEMENT_HISTORY_HOSTNAME);
			this.placementHistory = new MemcachedPlacementHistory(hostname);
		} else {
			this.placementHistory = new InMemoryPlacementHistory(totalCapacity);
		}

		availablePartitions = new ArrayList<>(maxPartitions);
		partitionSizes = new int[maxPartitions];

		// initially all partitions are available
		for (int i = 0; i < maxPartitions; i++) {
			availablePartitions.add(i);
		}
	}

	@Override
	public int getPartition(InternalElement element) {
		// XXX partition assignment without a context. Random
		return getRandomPartition();
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		// assign first 0.1% of vertices randomly
		if (this.counter < this.totalCapacity / 1000) {
			return getRandomPartition();
		}

		List<Long> neighbourList = Lists.newArrayList();
		vertex.edges(Direction.BOTH).forEachRemaining(edge -> {
			if (edge.inVertex().id().equals(vertex.id())) {
				neighbourList.add((Long) edge.outVertex().id());
			} else {
				neighbourList.add((Long) edge.inVertex().id());
			}
		});

		double[] partitionScores = new double[maxPartitions];
		int[] neighbourCount = new int[maxPartitions];

		for (Long neighbour : neighbourList) {
			Integer partition = placementHistory.getPartition(neighbour);
			if (partition != null) {
				// means that adjacent vertex previously assigned
				neighbourCount[partition]++;
			}
		}

		for (int i = 0; i < maxPartitions; i++) {
			// actual LDG formula
			partitionScores[i] = neighbourCount[i] * (1 - ((double) partitionSizes[i]) / partitionCapacity);
		}

		List<Integer> candidatePartitions = Lists.newArrayList();

		double tempMax = -1;
		for (int i : availablePartitions) {
			if (partitionScores[i] > tempMax) {
				tempMax = partitionScores[i];
				candidatePartitions.clear();
				candidatePartitions.add(i);
			} else if (partitionScores[i] == tempMax) {
				candidatePartitions.add(i);
			}
		}
		int assignedPartition = candidatePartitions.get(random.nextInt(candidatePartitions.size()));

		return assignedPartition;
	}

	@Override
	public void assignedPartition(InternalElement element, int partitionID) {
		// TODO Auto-generated method stub

	}

	@Override
	public void assignedPartition(InternalElement element, StarVertex vertex, int partitionID) {
		Preconditions.checkArgument(partitionID < maxPartitions && vertex != null);
		placementHistory.setPartition((Long) vertex.id(), partitionID);
		partitionSizes[partitionID]++;
		// check whether partition achieved its capacity
		if (partitionSizes[partitionID] >= partitionCapacity) {
			availablePartitions.remove(Integer.valueOf(partitionID));
		}

		log.warn("Vertex {} assigned to partition {}", ++counter, partitionID);
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

	/**
	 *
	 * @return one of the available partitions randomly drawn from a uniform
	 *         distribution
	 */
	public int getRandomPartition() {
		return availablePartitions.get(random.nextInt(availablePartitions.size()));
	}

}
