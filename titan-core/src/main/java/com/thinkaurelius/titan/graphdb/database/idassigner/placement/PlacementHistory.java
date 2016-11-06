package com.thinkaurelius.titan.graphdb.database.idassigner.placement;


/**
 * A utility interface used by Greedy Streaming Heuristics to store history of assignment
 * @author apacaci
 *
 */
public interface PlacementHistory {
	
	/**
	 * Partition ID of the element specifid by ID
	 * @param id
	 * @return negative number if element is not in history
	 */
	public Integer getPartition(Long id);
	
	/**
	 * Set partition ID for the element specified by id
	 * @param id
	 * @param partition
	 */
	public void setPartition(Long id, Integer partition);

}
