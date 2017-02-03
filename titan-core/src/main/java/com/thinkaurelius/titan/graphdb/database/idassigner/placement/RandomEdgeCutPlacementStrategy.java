package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;

@PreInitializeConfigOptions
public class RandomEdgeCutPlacementStrategy extends AbstractEdgeCutPlacementStrategy implements IDPlacementStrategy {

	private static final Logger log = LoggerFactory.getLogger(RandomEdgeCutPlacementStrategy.class);

	public RandomEdgeCutPlacementStrategy(Configuration config) {
		super(config);
	}

	@Override
	public int getPartition(InternalElement element, StarVertex vertex) {
		return getRandomPartition();
	}

}
