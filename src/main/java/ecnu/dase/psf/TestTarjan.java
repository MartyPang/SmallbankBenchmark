package ecnu.dase.psf;

import ecnu.dase.psf.concurrencycontrol.ConflictGraph;
import ecnu.dase.psf.concurrencycontrol.TarjanSCC;

import java.util.Stack;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/11 15:37
 */
public class TestTarjan {
    public static void main(String[] args) {
        //construct a directed graph
        ConflictGraph cg = new ConflictGraph();
        cg.addVertex(1);
        cg.addVertex(2);
        cg.addVertex(3);
        cg.addVertex(4);
        cg.addVertex(5);
        cg.addVertex(6);
        cg.addVertex(7);

        cg.addEdge(1, 2);
        cg.addEdge(2, 4);
        cg.addEdge(2, 5);
        cg.addEdge(3, 5);
        //cg.addEdge(4, 1);
        cg.addEdge(4, 5);
        cg.addEdge(4, 6);
        cg.addEdge(5, 7);
        cg.addEdge(6, 1);
        cg.addEdge(7, 3);

        //cg.printGraph();
        TarjanSCC tj = new TarjanSCC();
        tj.runTarjan(cg);
        tj.printSCC();

        //test topological sort
        Stack<Integer> st = cg.getTopologicalSort();
        while(!st.empty()) {
            System.out.print(st.pop()+" ");
        }
    }
}
