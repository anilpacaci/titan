package com.thinkaurelius.titan.util.io;

import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

import com.thinkaurelius.titan.core.TitanGraph;

public interface TitanIo<R extends TitanGraphReader.ReaderBuilder, W extends GraphWriter.WriterBuilder, M extends Mapper.Builder>
		extends Io {

	public interface Builder<I extends Io> extends Io.Builder<Io> {

		/**
		 * Vendors use this method to supply the current instance of their
		 * {@link TitanGraph} to the builder. End-users should not call this
		 * method directly.
		 */
		public Builder<? extends Io> graph(final TitanGraph g);
	}

}
