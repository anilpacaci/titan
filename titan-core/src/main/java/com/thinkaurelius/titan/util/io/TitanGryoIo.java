package com.thinkaurelius.titan.util.io;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;

import com.thinkaurelius.titan.core.TitanGraph;

/**
 * Gryo IO with Titan Graph Specific Reader Implementation
 * 
 * @author apacaci
 *
 */
public class TitanGryoIo implements TitanIo<TitanGryoReader.Builder, GryoWriter.Builder, GryoMapper.Builder> {
	private final IoRegistry registry;
	private final TitanGraph graph;

	private TitanGryoIo(final IoRegistry registry, final TitanGraph graph) {
		this.registry = registry;
		this.graph = graph;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TitanGryoReader.Builder reader() {
		return TitanGryoReader.build().mapper(mapper().create());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GryoWriter.Builder writer() {
		return GryoWriter.build().mapper(mapper().create());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GryoMapper.Builder mapper() {
		return (null == this.registry) ? GryoMapper.build() : GryoMapper.build().addRegistry(this.registry);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeGraph(final String file) throws IOException {
		try (final OutputStream out = new FileOutputStream(file)) {
			writer().create().writeGraph(out, graph);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readGraph(final String file) throws IOException {
		reader().create().readGraph(file, graph);
	}

	public static TitanIo.Builder<TitanGryoIo> build() {
		return new Builder();
	}

	public final static class Builder implements TitanIo.Builder<TitanGryoIo> {

		private IoRegistry registry = null;
		private TitanGraph graph;

		@Override
		public TitanIo.Builder<TitanGryoIo> registry(final IoRegistry registry) {
			this.registry = registry;
			return this;
		}

		@Override
		public TitanIo.Builder<TitanGryoIo> graph(final Graph g) {
			if (!(graph instanceof TitanGraph))
				throw new IllegalArgumentException("Graph should be an instance of TitanGraph for TitanIO");
			this.graph = (TitanGraph) g;
			return this;
		}

		@Override
		public TitanGryoIo create() {
			if (null == graph)
				throw new IllegalArgumentException("The graph argument was not specified");
			return new TitanGryoIo(registry, graph);
		}

		@Override
		public TitanIo.Builder<TitanGryoIo> graph(final TitanGraph g) {
			this.graph = (TitanGraph) g;
			return this;
		}
	}
}