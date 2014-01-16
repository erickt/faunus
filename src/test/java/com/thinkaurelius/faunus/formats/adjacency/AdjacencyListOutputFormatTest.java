package com.thinkaurelius.faunus.formats.adjacency;

import com.thinkaurelius.faunus.tinkerpop.gremlin.Imports;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AdjacencyListOutputFormatTest {

    @Test
    public void testInGremlinImports() {
        assertTrue(Imports.getImports().contains(AdjacencyFileOutputFormat.class.getPackage().getName() + ".*"));
    }
}
