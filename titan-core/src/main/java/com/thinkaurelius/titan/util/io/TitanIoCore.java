package com.thinkaurelius.titan.util.io;

import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

public class TitanIoCore {
	
	private TitanIoCore() {}


    /**
     * Creates a Titan-aware Gryo-based Reader {@link org.apache.tinkerpop.gremlin.structure.io.Io.Builder}.
     */
    public static TitanIo.Builder<TitanGryoIo> gryo() {
        return TitanGryoIo.build();
    }

    public static Io.Builder createIoBuilder(final String graphFormat) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        final Class<Io.Builder> ioBuilderClass = (Class<Io.Builder>) Class.forName(graphFormat);
        final Io.Builder ioBuilder = ioBuilderClass.newInstance();
        return ioBuilder;
    }

}
