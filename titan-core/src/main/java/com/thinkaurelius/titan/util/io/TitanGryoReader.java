package com.thinkaurelius.titan.util.io;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoWriter;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.Host;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.StarVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;

/**
 * Titan Graph specific Gryo Reader Implementation
 * 
 * @author apacaci
 *
 */
public final class TitanGryoReader implements GraphReader {

	private static final Logger log = LoggerFactory.getLogger(TitanGryoReader.class);

	private static final String ORIGINAL_ID_PROPERTY = "originalID";
	private static final String ORIGINAL_ID_INDEX_NAME = "byOriginalID";

	private final Kryo kryo;

	private final long batchSize;

	private TitanGryoReader(final long batchSize, final Mapper<Kryo> gryoMapper) {
		this.kryo = gryoMapper.createMapper();
		this.batchSize = batchSize;
	}

	public void readGraph(final String file, final TitanGraph graphToWriteTo) throws IOException {
		// an extra indexed property on original IDs
		TitanManagement management = graphToWriteTo.openManagement();
		PropertyKey originalIDProperty = management.makePropertyKey(ORIGINAL_ID_PROPERTY).dataType(Long.class)
				.cardinality(Cardinality.SINGLE).make();
		management.commit();
		// now index this property
		management = graphToWriteTo.openManagement();
		originalIDProperty = management.getPropertyKey(ORIGINAL_ID_PROPERTY);
		management.buildIndex(ORIGINAL_ID_INDEX_NAME, Vertex.class).addKey(originalIDProperty).buildCompositeIndex();
		management.commit();

		log.warn("Temporary original ID index have been created");

		try {
			ManagementSystem.awaitGraphIndexStatus(graphToWriteTo, ORIGINAL_ID_INDEX_NAME).call();
			// just make sure existing data is indexed
			management = graphToWriteTo.openManagement();
			management.updateIndex(management.getGraphIndex(ORIGINAL_ID_INDEX_NAME), SchemaAction.REINDEX).get();
			management.commit();
		} catch (InterruptedException | ExecutionException e) {
			log.error("Index creation failed, graph will not be added", e);
			return;
		}

		// first read vertices from stream
		FileInputStream inputStream = new FileInputStream(file);
		readVertices(inputStream, graphToWriteTo);

		log.warn("Vertices are read into the graph");

		// now read the edges
		inputStream = new FileInputStream(file);
		readEdges(inputStream, graphToWriteTo);

		log.warn("Edges are read into the graph");

		// now we need to drop the extra index
		management = graphToWriteTo.openManagement();
		TitanGraphIndex graphIndex = management.getGraphIndex(ORIGINAL_ID_INDEX_NAME);
		try {
			// disable index first
			management.updateIndex(graphIndex, SchemaAction.DISABLE_INDEX).get();
			management.commit();
			ManagementSystem.awaitGraphIndexStatus(graphToWriteTo, ORIGINAL_ID_INDEX_NAME).status(SchemaStatus.DISABLED)
					.call();

			// now we can remove index
			management = graphToWriteTo.openManagement();
			graphIndex = management.getGraphIndex(ORIGINAL_ID_INDEX_NAME);
			management.updateIndex(graphIndex, SchemaAction.REMOVE_INDEX).get();
			management.commit();
			log.warn("Temporary original ID index have been removed");
		} catch (InterruptedException | ExecutionException e) {
			log.error("Index removal failed, it needs to be removed manually", e);
		}
	}

	/**
	 * Read vertices into a {@link TitanGraph} from output generated by any of
	 * the {@link GryoWriter} {@code writeVertex} or {@code writeVertices}
	 * methods or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the Titan graph to write to when reading from the stream.
	 * @throws IOException
	 */
	public void readVertices(final InputStream inputStream, final TitanGraph graphToWriteTo) throws IOException {
		final AtomicLong counter = new AtomicLong(0);

		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		Input input = new Input(inputStream);

		IteratorUtils.iterate(new VertexInputIterator(input, attachable -> {
			final StarVertex starVertex = (StarGraph.StarVertex) attachable.get();
			final Vertex v = graphToWriteTo.addStarVertex(starVertex, T.label, starVertex.label());
			starVertex.properties().forEachRemaining(vp -> {
				final VertexProperty vertexProperty = graphToWriteTo.features().vertex().properties()
						.willAllowId(vp.id())
								? v.property(graphToWriteTo.features().vertex().getCardinality(vp.key()), vp.key(),
										vp.value(), T.id, vp.id())
								: v.property(graphToWriteTo.features().vertex().getCardinality(vp.key()), vp.key(),
										vp.value());
				vp.properties().forEachRemaining(p -> vertexProperty.property(p.key(), p.value()));
			});

			Long originalID = (Long) starVertex.id();

			v.property(ORIGINAL_ID_PROPERTY, originalID);

			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
			return v;
		}, null, null));

		if (supportsTx)
			graphToWriteTo.tx().commit();

		log.warn("Total number of vertices {}", counter.get());
	}

	/**
	 * Read edges into a {@link TitanGraph} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the Titan graph to write to when reading from the stream.
	 * @throws IOException
	 */
	public void readEdges(final InputStream inputStream, final TitanGraph graphToWriteTo) throws IOException {
		final AtomicLong counter = new AtomicLong(0);

		final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		Input input = new Input(inputStream);

		GraphTraversalSource traversal = graphToWriteTo.traversal();

		IteratorUtils.iterate(new VertexInputIterator(input, attachable -> {
			final StarVertex starVertex = (StarGraph.StarVertex) attachable.get();
			starVertex.edges(Direction.IN).forEachRemaining(e -> {
				// can't use a standard Attachable attach method here because we
				// have to use the cache for those
				// graphs that don't support userSuppliedIds on edges. note that
				// outVertex/inVertex methods return
				// StarAdjacentVertex whose equality should match StarVertex.

				Long outID = (Long) e.outVertex().id();
				Long inID = (Long) e.inVertex().id();

				Vertex outV = traversal.V().has(ORIGINAL_ID_PROPERTY, outID).next();
				Vertex inV = traversal.V().has(ORIGINAL_ID_PROPERTY, inID).next();

				final Edge newEdge = edgeFeatures.willAllowId(e.id()) ? outV.addEdge(e.label(), inV, T.id, e.id())
						: outV.addEdge(e.label(), inV);
				e.properties().forEachRemaining(p -> newEdge.property(p.key(), p.value()));
				if (supportsTx && counter.incrementAndGet() % batchSize == 0)
					graphToWriteTo.tx().commit();
			});

			return starVertex;
		}, null, null));

		if (supportsTx)
			graphToWriteTo.tx().commit();
	}

	/**
	 * Read data into a {@link Graph} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing an entire graph of vertices and edges as
	 *            defined by the accompanying
	 *            {@link GraphWriter#writeGraph(OutputStream, Graph)}.
	 * @param graphToWriteTo
	 *            the graph to write to when reading from the stream.
	 * @throws IOException
	 */
	@Override
	public void readGraph(final InputStream inputStream, final Graph graphToWriteTo) throws IOException {
		// dual pass - create all vertices and store to cache the ids. then
		// create edges. as long as we don't
		// have vertex labels in the output we can't do this single pass
		final Map<StarGraph.StarVertex, Vertex> cache = new HashMap<>();
		final AtomicLong counter = new AtomicLong(0);

		final Graph.Features.EdgeFeatures edgeFeatures = graphToWriteTo.features().edge();
		final boolean supportsTx = graphToWriteTo.features().graph().supportsTransactions();

		IteratorUtils.iterate(new VertexInputIterator(new Input(inputStream), attachable -> {
			final Vertex v = cache.put((StarGraph.StarVertex) attachable.get(),
					attachable.attach(Attachable.Method.create(graphToWriteTo)));
			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
			return v;
		}, null, null));
		cache.entrySet().forEach(kv -> kv.getKey().edges(Direction.IN).forEachRemaining(e -> {
			// can't use a standard Attachable attach method here because we
			// have to use the cache for those
			// graphs that don't support userSuppliedIds on edges. note that
			// outVertex/inVertex methods return
			// StarAdjacentVertex whose equality should match StarVertex.
			final Vertex cachedOutV = cache.get(e.outVertex());
			final Vertex cachedInV = cache.get(e.inVertex());
			final Edge newEdge = edgeFeatures.willAllowId(e.id())
					? cachedOutV.addEdge(e.label(), cachedInV, T.id, e.id()) : cachedOutV.addEdge(e.label(), cachedInV);
			e.properties().forEachRemaining(p -> newEdge.property(p.key(), p.value()));
			if (supportsTx && counter.incrementAndGet() % batchSize == 0)
				graphToWriteTo.tx().commit();
		}));

		if (supportsTx)
			graphToWriteTo.tx().commit();
	}

	/**
	 * Read {@link Vertex} objects from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Vertex} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)}
	 *            or {@link GraphWriter#writeVertices(OutputStream, Iterator)}
	 *            methods.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 * @param attachEdgesOfThisDirection
	 *            only edges of this direction are passed to the
	 *            {@code edgeMaker}.
	 */
	@Override
	public Iterator<Vertex> readVertices(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
			final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection)
			throws IOException {
		return new VertexInputIterator(new Input(inputStream), vertexAttachMethod, attachEdgesOfThisDirection,
				edgeAttachMethod);
	}

	/**
	 * Read a {@link Vertex} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least a single vertex as defined by the
	 *            accompanying
	 *            {@link GraphWriter#writeVertex(OutputStream, Vertex)}.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 */
	@Override
	public Vertex readVertex(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod) throws IOException {
		return readVertex(inputStream, vertexAttachMethod, null, null);
	}

	/**
	 * Read a {@link Vertex} from output generated by any of the
	 * {@link GryoWriter} {@code writeVertex} or {@code writeVertices} methods
	 * or by {@link GryoWriter#writeGraph(OutputStream, Graph)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Vertex} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeVertices(OutputStream, Iterator, Direction)}
	 *            method.
	 * @param vertexAttachMethod
	 *            a function that creates re-attaches a {@link Vertex} to a
	 *            {@link Host} object.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 * @param attachEdgesOfThisDirection
	 *            only edges of this direction are passed to the
	 *            {@code edgeMaker}.
	 */
	@Override
	public Vertex readVertex(final InputStream inputStream,
			final Function<Attachable<Vertex>, Vertex> vertexAttachMethod,
			final Function<Attachable<Edge>, Edge> edgeAttachMethod, final Direction attachEdgesOfThisDirection)
			throws IOException {
		final Input input = new Input(inputStream);
		return readVertexInternal(vertexAttachMethod, edgeAttachMethod, attachEdgesOfThisDirection, input);
	}

	/**
	 * Read an {@link Edge} from output generated by
	 * {@link GryoWriter#writeEdge(OutputStream, Edge)} or via an {@link Edge}
	 * passed to {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Edge} as defined by
	 *            the accompanying
	 *            {@link GraphWriter#writeEdge(OutputStream, Edge)} method.
	 * @param edgeAttachMethod
	 *            a function that creates re-attaches a {@link Edge} to a
	 *            {@link Host} object.
	 */
	@Override
	public Edge readEdge(final InputStream inputStream, final Function<Attachable<Edge>, Edge> edgeAttachMethod)
			throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<Edge> attachable = kryo.readObject(input, DetachedEdge.class);
		return edgeAttachMethod.apply(attachable);
	}

	/**
	 * Read a {@link VertexProperty} from output generated by
	 * {@link GryoWriter#writeVertexProperty(OutputStream, VertexProperty)} or
	 * via an {@link VertexProperty} passed to
	 * {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link VertexProperty} as
	 *            written by the accompanying
	 *            {@link GraphWriter#writeVertexProperty(OutputStream, VertexProperty)}
	 *            method.
	 * @param vertexPropertyAttachMethod
	 *            a function that creates re-attaches a {@link VertexProperty}
	 *            to a {@link Host} object.
	 */
	@Override
	public VertexProperty readVertexProperty(final InputStream inputStream,
			final Function<Attachable<VertexProperty>, VertexProperty> vertexPropertyAttachMethod) throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<VertexProperty> attachable = kryo.readObject(input, DetachedVertexProperty.class);
		return vertexPropertyAttachMethod.apply(attachable);
	}

	/**
	 * Read a {@link Property} from output generated by
	 * {@link GryoWriter#writeProperty(OutputStream, Property)} or via an
	 * {@link Property} passed to
	 * {@link GryoWriter#writeObject(OutputStream, Object)}.
	 *
	 * @param inputStream
	 *            a stream containing at least one {@link Property} as written
	 *            by the accompanying
	 *            {@link GraphWriter#writeProperty(OutputStream, Property)}
	 *            method.
	 * @param propertyAttachMethod
	 *            a function that creates re-attaches a {@link Property} to a
	 *            {@link Host} object.
	 */
	@Override
	public Property readProperty(final InputStream inputStream,
			final Function<Attachable<Property>, Property> propertyAttachMethod) throws IOException {
		final Input input = new Input(inputStream);
		readHeader(input);
		final Attachable<Property> attachable = kryo.readObject(input, DetachedProperty.class);
		return propertyAttachMethod.apply(attachable);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <C> C readObject(final InputStream inputStream, final Class<? extends C> clazz) throws IOException {
		return clazz.cast(this.kryo.readClassAndObject(new Input(inputStream)));
	}

	private Vertex readVertexInternal(final Function<Attachable<Vertex>, Vertex> vertexMaker,
			final Function<Attachable<Edge>, Edge> edgeMaker, final Direction d, final Input input) throws IOException {
		readHeader(input);
		final StarGraph starGraph = kryo.readObject(input, StarGraph.class);

		// read the terminator
		kryo.readClassAndObject(input);

		final Vertex v = vertexMaker.apply(starGraph.getStarVertex());
		if (edgeMaker != null)
			starGraph.getStarVertex().edges(d).forEachRemaining(e -> edgeMaker.apply((Attachable<Edge>) e));
		return v;
	}

	private void readHeader(final Input input) throws IOException {
		if (!Arrays.equals(GryoMapper.GIO, input.readBytes(3)))
			throw new IOException("Invalid format - first three bytes of header do not match expected value");

		// skip the next 13 bytes - for future use
		input.readBytes(13);
	}

	public static Builder build() {
		return new Builder();
	}

	public final static class Builder implements ReaderBuilder<TitanGryoReader> {

		private long batchSize = 10000;
		/**
		 * Always use the most recent gryo version by default
		 */
		private Mapper<Kryo> gryoMapper = GryoMapper.build().create();

		private Builder() {
		}

		/**
		 * Number of mutations to perform before a commit is executed when using
		 * {@link TitanGryoReader#readGraph(InputStream, Graph)}.
		 */
		public Builder batchSize(final long batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Supply a mapper {@link GryoMapper} instance to use as the serializer
		 * for the {@code KryoWriter}.
		 */
		public Builder mapper(final Mapper<Kryo> gryoMapper) {
			this.gryoMapper = gryoMapper;
			return this;
		}

		public TitanGryoReader create() {
			return new TitanGryoReader(batchSize, this.gryoMapper);
		}

	}

	private class VertexInputIterator implements Iterator<Vertex> {
		private final Input input;
		private final Function<Attachable<Vertex>, Vertex> vertexMaker;
		private final Direction d;
		private final Function<Attachable<Edge>, Edge> edgeMaker;

		public VertexInputIterator(final Input input, final Function<Attachable<Vertex>, Vertex> vertexMaker,
				final Direction d, final Function<Attachable<Edge>, Edge> edgeMaker) {
			this.input = input;
			this.d = d;
			this.edgeMaker = edgeMaker;
			this.vertexMaker = vertexMaker;
		}

		@Override
		public boolean hasNext() {
			return !input.eof();
		}

		@Override
		public Vertex next() {
			try {
				return readVertexInternal(vertexMaker, edgeMaker, d, input);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}
	}
}
