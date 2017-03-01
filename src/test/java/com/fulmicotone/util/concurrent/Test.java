package com.fulmicotone.util.concurrent;

import org.junit.Assert;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * Created by dino on 27/02/2017.
 */
public class Test {

    private void doEach(int millis, Consumer<Integer> doFn){

        final int[] idx=new int[]{0};
        /**each second put random uid in the queue for 5 times then the tree will be cut**/
        new Timer()
                .scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        doFn.accept(  idx[0]);
                        idx[0]++;
                    }
                }, 0l, millis);
    }

//   @org.junit.Test
    public void forceShutdownWholeSystemCase() throws Exception {

        LimeTree tree=new LimeTree();
        List<String> output=new ArrayList<>();

        final  BlockingQueue<String> fruitWireQueue = tree
                .<String>newLime()
                .setAct((e, t,c) -> System.out.println(e))
                .create().getWire();


        /**each second put random uid in the queue for 5 times then the tree will be cut**/
        doEach(500,(i)->{
            try{
                 String value=UUID.randomUUID().toString();
                fruitWireQueue.put(value);
                 output.add(value);
                 if(i==4){  tree.clearCutBrutal(); }
            } catch (InterruptedException e) { e.printStackTrace();}
        });


        tree.clearCutAwaitActs();//wait all acts finished

        Assert.assertTrue("size was:"+output.size(),output.size()==5);
    }

    //@org.junit.Test
    public void shutdownForTreeInactivityCase(){
        List<String> output=new ArrayList<>();

        LimeTree tree=new LimeTree();

        /**LIME A DEFINITION**/
        BlockingQueue<String> wireA = tree
                .<String>newLime()
                .setAct((element, contextTree, MeConsumer) -> {
                    try {

                            //get queue of Lime with key B and put element + _A suffix
                           contextTree.<String>getWireBy("B").put(element+"_A");

                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                })
                .create("A").getWire();

        /**LIME B DEFINITION**/
         tree
                .<String>newLime()
                .setAct((e, t, c) -> output.add(e+"_B"))
                .create("B");

        /** put in A lime queue an element random
         * each 500 millis after ten time the adding
         * become slow and the monitor terminate the whole system
         */
        doEach(500,(i)-> {
             try {
                     if(i>10){ Thread.sleep(3000);}
                     wireA.add(UUID.randomUUID().toString());
             } catch (InterruptedException e) { e.printStackTrace();}

         });

         tree.clearCutOnTreeInactivity(2000 );
         tree.clearCutAwaitActs();
         Assert.assertTrue("unexpected size",output.size()==11);
    }

  //  @org.junit.Test
    public void shutdownForWiltingAllLimecase(){

        List<String> output=new ArrayList<>();
        LimeTree tree=new LimeTree();

       BlockingQueue<String> wire = tree.newLime()
                .<String>setAct((pill, context, lime) -> {output.add((String) pill);})
                .setWilting(500)
                .create()
                .getWire();

        BlockingQueue<String> wire2 = tree.newLime()
                .<String>setAct((g, ctx, l) -> output.add((String) g))
                .setWilting(500)
                .create()
                .getWire();

        doEach(100,(i)-> {
            if(i>1){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            wire.add(UUID.randomUUID().toString());
        });

        doEach(100,(i)-> {
            if(i>2){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            wire2.add(UUID.randomUUID().toString());
        });
        tree.clearCutAwaitActs();
        output.stream().forEach(System.out::println);
      Assert.assertTrue("unexpected number of records:"+output.size(),output.size()==5);

    }

    @org.junit.Test
    public void endlessLime(){


        List<String> output=new ArrayList<>();

        LimeTree tree=new LimeTree();

        BlockingQueue<String> wireA= tree.newLime()
                .<String>setAct((pill, context, lime) -> {

            if(pill.equals("fire exception")){ throw new RuntimeException("sf");}
            System.out.println("pill:"+pill);
        })
                .setWilting(2000)
                .setLiveAye(true)
                .create()
                .getWire();

/*

        BlockingQueue<String> wireB= tree.newLime()
                .<String>setAct((pill, context, lime) -> {
                    if(pill.equals("fire exception")){ throw new RuntimeException("sf");}
                    System.out.println("pill:"+pill);
                })
                .setWilting(1000)
                .setLiveAye(true)
                .create()
                .getWire();
*/


        doEach(500,(i)-> {
            System.out.println("add pill");
            if(i==3){wireA.add("fire exception");}
            wireA.add(UUID.randomUUID().toString());
        });



        tree.clearCutAwaitActs();


    }


//funzione che verra eseguita allo shutdown dell'intero albero
    //in questo modo saraà possibile preservare le code
    public void onendwholesystem(){}

}
