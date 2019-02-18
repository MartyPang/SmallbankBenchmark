package ecnu.dase.psf.storage;

import ecnu.dase.psf.common.Edge;
import ecnu.dase.psf.common.Item;
import ecnu.dase.psf.common.Vertex;
import ecnu.dase.psf.concurrencycontrol.DirectedGraph;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/18 9:22
 */
public class HybridDB {
    private DB db_;
    private DirectedGraph tdg_;

    ThreadLocalRandom localR = ThreadLocalRandom.current();

    public HybridDB(DB db, DirectedGraph tdg) {
        db_ = db;
        tdg_ = tdg;
    }

    public Item getState(int tranId, String table, int acc) {
        Item value = null;
        //check consistent read set of tranId
        Vertex v = tdg_.getVertices().get(tranId);
        List<Integer> processors = v.getIncomingEdge();
        boolean found = false;
        String key = table + "_" + acc;
        //traverse all R
        for(int fromVid : processors) {
            Vertex u = tdg_.getVertices().get(fromVid);
            Edge e = u.getEdgeById(tranId);
            if(e != null) {
                Map<String, Item> R = e.getConsistentR();
                value = R.get(key);
                if(value != null) {
                    found = true;
                    break;
                }
            }
        }
        //if not in R,
        //then get state from local db
        if(!found) {
            value = db_.getState(table, acc);
        }
        else {
            int internal = localR.nextInt()%1500 + 1500;
            for(int i = 0;i<20;++i){
                for(int j = 0;j<internal;++j){
                    isPrime(i*j);
                }
            }
        }
        return value;
    }

    public void putState(int writtenBy, String table, int acc, int value) {
        db_.putState(writtenBy, table, acc, value);
    }


    public void updateR(int tranId, Map<String, Item> writeSet) {
        Vertex v = tdg_.getVertices().get(tranId);
        List<Edge> edges = v.getEdgeList_();
        for(Edge e : edges) {
            e.resetR();
            for(String key : writeSet.keySet()) {
                Item value = writeSet.get(key);
                e.updateConsistentReadset(key, value);
            }
        }
    }

    public boolean isPrime(int a) {
        boolean flag = true;
        if (a < 2) {// 素数不小于2
            return false;
        } else {
            for (int i = 2; i <= Math.sqrt(a); i++) {
                if (a % i == 0) {// 若能被整除，则说明不是素数，返回false
                    flag = false;
                   // break;// 跳出循环
                }
            }
        }
        return flag;
    }

    public DB getDb_() {
        return db_;
    }

    public void setDb_(DB db_) {
        this.db_ = db_;
    }

    public DirectedGraph getTdg_() {
        return tdg_;
    }

    public void setTdg_(DirectedGraph tdg_) {
        this.tdg_ = tdg_;
    }
}
