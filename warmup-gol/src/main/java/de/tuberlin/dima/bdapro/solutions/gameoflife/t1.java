package de.tuberlin.dima.bdapro.solutions.gameoflife;

/**
 * Created by zhanwang on 29/10/17.
 */
public class t1 {
    public static void main(String[] args) throws Exception {
        int a = new GameOfLifeTaskImpl().solve("",100,100,5);
        System.out.println(a);
    }
}
