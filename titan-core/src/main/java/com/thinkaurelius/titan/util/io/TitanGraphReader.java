package com.thinkaurelius.titan.util.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.tinkerpop.gremlin.structure.io.GraphReader;

import com.thinkaurelius.titan.core.TitanGraph;

public interface TitanGraphReader extends GraphReader {
	
    public void readGraph(final InputStream inputStream, final TitanGraph graphToWriteTo) throws IOException;

}
