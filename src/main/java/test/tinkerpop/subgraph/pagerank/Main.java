
package test.tinkerpop.subgraph.pagerank;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

@SuppressWarnings("unchecked")
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String NUMBER = "number";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final String EDGE = "edge";
    private static final String PAGE_RANK = "pageRank";
    private static final Map<String, Supplier<Graph>> GRAPHS = new HashMap<String, Supplier<Graph>>() {{
        put("TinkerGraph", Main::openTinkerGraph);
        put("JanusGraph", Main::openJanusGraph);
    }};

    private static Graph openTinkerGraph() {
        return TinkerGraph.open();
    }

    private static Graph openJanusGraph() {
        JanusGraph graph = JanusGraphFactory.open(Main.class.getResource("/janusgraph-berkeley.properties").getFile());

        JanusGraphManagement management = graph.openManagement();

        if (management.containsPropertyKey(NUMBER)) {
            return graph;
        }
        PropertyKey key = management.makePropertyKey(NUMBER).dataType(Integer.class).make();
        management.buildIndex("keyIndex", Vertex.class)
                .addKey(key)
                .buildCompositeIndex();
        management.commit();
        return graph;
    }

    public static void main(String... args) {
        GRAPHS.forEach(Main::runPageRankForGraph);
    }

    private static void runPageRankForGraph(String name, Supplier<Graph> graphSupplier) {
        log.info(String.format("Using: %s graph.", name));

        try (Graph graph = graphSupplier.get();
             GraphTraversalSource g = graph.traversal()) {
            loadGraph(graph);

            Map<String, Collection> idsOnlySubgraphMap = getSubgraphIds(g);

            calculatePageRankAndPrintTopRanked(idsOnlySubgraphMap, graph);
            clearGraph(g);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void calculatePageRankAndPrintTopRanked(Map<String, Collection> subGraphIdsMap, Graph graph) {
        Computer computer = Computer.compute()
                .vertices(__.hasId(P.within(subGraphIdsMap.get(VERTICES))))
                .edges(__.bothE().hasId(P.within(subGraphIdsMap.get(EDGES))))
                .result(GraphComputer.ResultGraph.ORIGINAL);
        GraphTraversalSource g = graph.traversal().withComputer(computer);
        printTopRanked(g);
    }

    private static void printTopRanked(GraphTraversalSource g) {
        StringBuilder result = new StringBuilder();

        g
                .V()
                .pageRank()
                .by(__.bothE())
                .by(PAGE_RANK)
                .times(1)
                .order().by(PAGE_RANK, Order.decr)
                .limit(20)
                .valueMap(NUMBER, PAGE_RANK)
                .forEachRemaining(kv -> result.append(kv.toString()).append("\n"));
        result.append("V: ").append(g.V().count().next()).append("\n")
                .append("E: ").append(g.E().count().next()).append("\n");
        log.info(result.toString());
    }

    private static Map<String, Collection> getSubgraphIds(GraphTraversalSource g) {
        return (Map) g
                .V()
                .has(NUMBER, 204984)
                .emit()
                .repeat(__.bothE().dedup().store(EDGES).by(T.id).otherV())
                .times(2)
                .dedup()
                .aggregate(VERTICES).by(T.id)
                .bothE()
                .where(P.without(EDGES)).by(T.id).by()
                .as(EDGE)
                .otherV()
                .where(P.within(VERTICES)).by(T.id).by()
                .select(EDGE)
                .store(EDGES).by(T.id)
                .cap(VERTICES, EDGES)
                .next();
    }

    private static void clearGraph(GraphTraversalSource g) {
        g.E().drop().iterate();
        g.V().drop().iterate();
        if (g.getGraph().features().graph().supportsTransactions()) {
            g.tx().commit();
        }
    }

    private static void loadGraph(Graph graph) throws IOException {
        InputStream is = Main.class.getResourceAsStream("/graph.graphml");
        GraphMLReader.build()
                .create()
                .readGraph(is, graph);
    }
}