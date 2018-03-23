
package test.tinkerpop.subgraph.pagerank;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            Map<String, Collection> idsOnlySubgraphMap = getSubgraphIdsOnly(g);

            pageRankForInMemorySubGraph(subGraphMap);
            pageRankForOLAPWithFilter(subGraphMap, graph);
            pageRankForOLAPWithFilterIdsOnly(idsOnlySubgraphMap, graph);
        }
    }

    private static void pageRankForOLAPWithFilter(Map<String, Collection> subGraphMap, Graph graph) {
        final List<String> vids = (List<String>) subGraphMap.get(VERTICES).stream()
                .map(v -> ((Vertex) v).id()).collect(Collectors.toList());
        final List<String> eids = (List<String>) subGraphMap.get(EDGES).stream()
                .map(e -> ((Edge) e).id()).collect(Collectors.toList());
        final Computer computer = Computer.compute()
                .vertices(__.hasId(P.within(vids)))
                .edges(__.bothE().hasId(P.within(eids)))
                .result(GraphComputer.ResultGraph.ORIGINAL);
        final GraphTraversalSource g = graph.traversal().withComputer(computer);
        printTopRanked(g);
    }

    private static void pageRankForOLAPWithFilterIdsOnly(Map<String, Collection> subGraphIdsMap, Graph graph) {
        final Computer computer = Computer.compute()
                .vertices(__.hasId(P.within(subGraphIdsMap.get(VERTICES))))
                .edges(__.bothE().hasId(P.within(subGraphIdsMap.get(EDGES))))
                .result(GraphComputer.ResultGraph.ORIGINAL);
        final GraphTraversalSource g = graph.traversal().withComputer(computer);
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
                        Edge inMemoryEdge = inMemoryG.V(edge.outVertex().id())
                                .addE(edge.label()).to(__.V().hasId(edge.inVertex().id())).next();

                        edge.properties()
                                .forEachRemaining(
                                        property -> inMemoryEdge.property(property.key(), property.value()));
                    });

            // Page rank.
            printTopRanked(inMemoryG.withComputer());
        }
    }

    private static void printTopRanked(GraphTraversalSource g) {
        g
                .V()
                .pageRank()
                .by(__.bothE())
                .by(PAGE_RANK)
                .times(1)
                .order().by(PAGE_RANK, Order.decr)
                .limit(20)
                .valueMap(NUMBER, PAGE_RANK)
                .forEachRemaining(System.out::println);
        System.out.println("V: " + g.V().count().next());
        System.out.println("E: " + g.E().count().next());
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

    private static Map<String, Collection> getSubgraphIdsOnly(GraphTraversalSource g) {
        return (Map) g
                .V()
                .has(NUMBER, 204984)
                .emit()
                .repeat(__.bothE().dedup().store(EDGES).by(T.id).otherV())
                .times(2)
                .dedup()
                .aggregate(VERTICES).by(T.id)
                .bothE()
                .where(P.without(EDGES))
                .as(EDGE)
                .otherV()
                .where(__.hasId(P.within(VERTICES)))
                .select(EDGE)
                .store(EDGES).by(T.id)
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