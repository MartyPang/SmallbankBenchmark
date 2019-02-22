package ecnu.dase.psf;

import ecnu.dase.psf.smallbank.WorkloadGenerator;
import ecnu.dase.psf.storage.DB;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/12 15:28
 */
public class TestRandom {
    public static void main(String[] args) {
//        int max = 30;
//        int min = 10;
//        Random rand = new Random();
//        while(true) {
//            System.out.println(rand.nextInt(max-min+1)+min);
//        }

        DB db = new DB(1000, 10);
        WorkloadGenerator generator = new WorkloadGenerator(db, 400, 1000, 10, 0.99);
        generator.testZipf();
    }
}
