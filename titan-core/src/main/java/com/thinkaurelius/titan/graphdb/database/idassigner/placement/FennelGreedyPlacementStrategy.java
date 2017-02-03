package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;

@PreInitializeConfigOptions
public class FennelGreedyPlacementStrategy extends AbstractEdgeCutPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(FennelGreedyPlacementStrategy.class);

	/**
	 * Fennel Specific Parameters.
	 */
	public static final ConfigOption<Double> PARTITION_BALANCE_COEFFICINET = new ConfigOption<Double>(
			GraphDatabaseConfiguration.CLUSTER_NS, "partition-balance-coefficient",
			"Controls the nature of heuristics, higher values ignores neighbourhood count. Recommended to set between 1 and 2",
			ConfigOption.Type.MASKABLE, (double) 1.5);

	private double balanceCoefficient;

	public FennelGreedyPlacementStrategy(Configuration config) {
		super(config);
		this.balanceCoefficient = config.get(PARTITION_BALANCE_COEFFICINET);
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		// assign first 0.1% of vertices randomly
		if (this.counter < this.totalCapacity / 1000) {
			return getRandomPartition();
		}

		if (vertex == null || !this.partitioningEnabled) {
			// there is no adjacency information or partitioning is explicity
			// disabled, resort to random partitioning
			return getRandomPartition();
		}

		double[] partitionScores = new double[maxPartitions];
		int[] neighbourCount = getNeighbourCount(vertex);

		for (int i = 0; i < maxPartitions; i++) {
			// greedy objective function for Fennel Formulation
			partitionScores[i] = neighbourCount[i] - (this.balanceSlack * this.balanceCoefficient
					* Math.pow(partitionSizes[i], this.balanceCoefficient - 1));
		}

		List<Integer> candidatePartitions = Lists.newArrayList();

		// because DOUBLE.MIN_VALUE is actually smallest positive value that can be represented via double
		double tempMax = -Double.MAX_VALUE;
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

	private int[] getNeighbourCount(StarVertex vertex) {
		List<Long> neighbourList = Lists.newArrayList();
		vertex.edges(Direction.BOTH).forEachRemaining(edge -> {
			if (edge.inVertex().id().equals(vertex.id())) {
				neighbourList.add((Long) edge.outVertex().id());
			} else {
				neighbourList.add((Long) edge.inVertex().id());
			}
		});

		int[] neighbourCount = new int[maxPartitions];

		for (Long neighbour : neighbourList) {
			Integer partition = placementHistory.getPartition(neighbour);
			if (partition != null) {
				// means that adjacent vertex previously assigned
				neighbourCount[partition]++;
			}
		}

		return neighbourCount;
	}

}
