package ecnu.dase.psf.concurrencycontrol;

import ecnu.dase.psf.common.Vertex;

import java.util.*;

/**
 * Using Tarjan's SCC algorithm to
 * find all strongly connected components (SCC) for a directed graph
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/11 14:04
 */
public class TarjanSCC {
    /**
     * Auxiliary stack for dfs
     */
    private Stack<Vertex> stack;
    /**
     * Store all found SCC
     */
    private List<DirectedGraph> scc;

    /**
     * Access order
     */
    private int ts;

    public TarjanSCC() {
        stack = new Stack<>();
        scc = new ArrayList<>();
        ts = 0;
    }

    public void runTarjan(DirectedGraph graph) {
        Collection<Vertex> vertices = graph.getVertices().values();
        for(Vertex v : vertices) {
            if(!v.isVisited()) {
                tarjan(v);
            }
        }
    }

    /**
     * Any scc is a subtree of the original DFS tree,
     * after finding all roots of these subtrees, we can easily
     * construct all SCCs using auxiliary stack.
     * @param vertex each node
     */
    private void tarjan(Vertex vertex) {
        /**
         * root of the scc
         */
        boolean isRoot = true;
        vertex.setLowLink(ts++);
        vertex.visit();
        stack.push(vertex);

        Iterator<Vertex> it = vertex.getNeighborIterator();
        while(it.hasNext()) {
            Vertex nextV = it.next();
            if(!nextV.isVisited()) {
                tarjan(nextV);
            }
            if(vertex.getLowLink() > nextV.getLowLink()) {
                vertex.setLowLink(nextV.getLowLink());
                isRoot = false;
            }
        }
        //pop one that has lower ts than vertex from stack
        if(isRoot) {
            Map<Integer, Vertex> component = new HashMap<>();
            while(true) {
                Vertex v = stack.pop();
                component.put(v.getvId_(), v);
                if(v.getvId_() == vertex.getvId_()) {
                    break;
                }
            }
            DirectedGraph dg = new DirectedGraph(component);
            scc.add(dg);
        }
    }

    public void printSCC() {
        for(DirectedGraph component : scc) {
            System.out.println(component.getVertices().values());
        }
    }

    public List<DirectedGraph> getScc() {
        return scc;
    }

    public void setScc(List<DirectedGraph> scc) {
        this.scc = scc;
    }
}
