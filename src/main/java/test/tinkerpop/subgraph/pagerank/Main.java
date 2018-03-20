package test.tinkerpop.subgraph.pagerank;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unchecked")
public class Main {
    private static final String NUMBER = "number";
    private static final String VERTICES = "vertices";
    private static final String EDGES = "edges";
    private static final String EDGE = "edge";
    private static final String PAGE_RANK = "pageRank";

    public static void main(String... args) throws Exception {
        try (Graph graph = TinkerGraph.open();
             GraphTraversalSource g = graph.traversal()) {
            loadGraph(graph);

            Map<String, Collection> subGraphMap = getSubgraph(g);

            pageRankForInMemorySubGraph(subGraphMap);
            pageRankForOLAPWithFilter(subGraphMap, g);
        }
    }

    private static void pageRankForOLAPWithFilter(Map<String, Collection> subGraphMap, GraphTraversalSource g) {
        TraversalStrategy strategy = SubgraphStrategy.build()
                .vertices(__.is(P.within(subGraphMap.get(VERTICES))))
                .create();

        g = g.withStrategies(strategy);
        printTopRanked(g);
    }

    private static void pageRankForInMemorySubGraph(Map<String, Collection> subGraphMap) throws Exception {
        try (Graph inMemoryGraph = TinkerGraph.open()) {
            GraphTraversalSource inMemoryG = inMemoryGraph
                    .traversal();

            // Load vertices.
            subGraphMap.get(VERTICES)
                    .stream()
                    .distinct()
                    .forEach(obj -> {
                        Vertex vertex = (Vertex) obj;
                        Vertex inMemoryVertex = inMemoryGraph.addVertex(
                                T.id, vertex.id(),
                                T.label, vertex.label()
                        );

                        vertex.properties()
                                .forEachRemaining(
                                        property -> inMemoryVertex.property(property.key(), property.value()));
                    });

            // Load edges.
            subGraphMap.get(EDGES)
                    .stream()
                    .distinct()
                    .forEach(obj -> {
                        Edge edge = (Edge) obj;
                        Iterator<Vertex> outIn = inMemoryGraph.vertices(
                                edge.outVertex().id(),
                                edge.inVertex().id()
                        );
                        Edge inMemoryEdge = outIn.next()
                                .addEdge(edge.label(), outIn.next());

                        edge.properties()
                                .forEachRemaining(
                                        property -> inMemoryEdge.property(property.key(), property.value()));
                    });

            // Page rank.
            printTopRanked(inMemoryG);
        }
    }

    private static void printTopRanked(GraphTraversalSource g) {
        List results = g
                .withComputer()
                .V()
                .pageRank()
                .by(__.bothE())
                .by(PAGE_RANK)
                .order().by(PAGE_RANK, Order.decr)
                .valueMap(NUMBER, PAGE_RANK)
                .next(20);
        System.out.println(results);
    }

    private static Map<String, Collection> getSubgraph(GraphTraversalSource g) {
        return (Map) g
                .V()
                .has(NUMBER, 204984)
                .emit()
                .repeat(__.bothE().dedup().store(EDGES).otherV())
                .times(2)
                .dedup()
                .aggregate(VERTICES)
                .bothE()
                .where(P.without(EDGES))
                .as(EDGE)
                .otherV()
                .where(P.within(VERTICES))
                .select(EDGE)
                .store(EDGES)
                .cap(VERTICES, EDGES)
                .next();
    }

    private static void loadGraph(Graph graph) throws IOException {
        InputStream is = Main.class.getResourceAsStream("/graph.graphml");
        GraphMLReader.build()
                .create()
                .readGraph(is, graph);
    }
}
