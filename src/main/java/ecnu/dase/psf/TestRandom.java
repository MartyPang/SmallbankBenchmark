package ecnu.dase.psf;

import java.util.Random;

/**
 * @author MartyPang
 * @version 1.0
 * @date 2019/2/12 15:28
 */
public class TestRandom {
    public static void main(String[] args) {
        int max = 30;
        int min = 10;
        Random rand = new Random();
        while(true) {
            System.out.println(rand.nextInt(max-min+1)+min);
        }
    }
}
