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

    /**
     * Brutal shutdown test
     * @throws Exception
     */
    @org.junit.Test
    public void forceShutdownWholeSystemCase() throws Exception {

       EventsCounter ec = new EventsCounter();
       LimeTree tree=new LimeTree();
       tree.addListener(ec);
        List<String> output=new ArrayList<>();

        final  BlockingQueue<String> fruitWireQueue = tree
                .<String>newLime()
                .setAct((e, t,c) -> {
            System.out.println(e);



        })
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

       ec
     .testAssert(
             5,
             1,
             0,
             0,
             0,
             0,
             0,
             0,
             1);

    }

    /**
     * Shutdown for inactivity
     */
    @org.junit.Test
    public void shutdownForTreeInactivityCase(){

        EventsCounter ec = new EventsCounter();
        LimeTree tree=new LimeTree();
        tree.addListener(ec);
        List<String> output=new ArrayList<>();

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
        doEach(200,(i)-> {
             try {
                     if(i>5){ Thread.sleep(2000);}
                     wireA.add(UUID.randomUUID().toString());
             } catch (InterruptedException e) { e.printStackTrace();}

         });

         tree.setTimeoutForInactivity(500 );

         tree.clearCutAwaitActs();


        ec.testAssert(-1,
                        2,
                        0,
                        0,
                        2,
                        0,
                        1,
                        0,
                        0);

    }

    /**
     * Shutdown lime wilting
     */
    @org.junit.Test
    public void shutdownForWiltingAllLimecase(){

        EventsCounter ec = new EventsCounter();
        LimeTree tree=new LimeTree();
        tree.addListener(ec);
        List<String> output=new ArrayList<>();


       BlockingQueue<String> wire = tree.newLime()
                .<String>setAct((pill, context, lime) -> output.add((String) pill))
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

        ec
                .testAssert(5,
                        2,
                        0,
                        0,
                        0,
                        2,
                        0,
                        0,
                        1);

    }


    /**
     * Live Aye Test multiconsumer
     */
    @org.junit.Test
    public void endlessLimeMultipartner(){
        EventsCounter ec = new EventsCounter();
        LimeTree tree=new LimeTree();
        tree.addListener(ec);
        List<String> output=new ArrayList<>();




        BlockingQueue<String> wireA= tree.newLime()
                .<String>setAct((pill, context, lime) -> {
            if(pill.equals("fire exception")){ throw new RuntimeException("sf"); }
                    output.add((String) pill);
        })
                .setWilting(2300)
                .setLiveAye(true)
                .create("A")
                .getWire();

        BlockingQueue<String> wireB= tree.newLime()
                .<String>setAct((pill, context, lime) -> {
                    if(pill.equals("fire exception")){ throw new RuntimeException("sf");}
                    output.add((String) pill);
                    System.out.println("b:"+pill);
                })
                .setWilting(1500)

                .setLiveAye(true)

                .create("B")

                .getWire();

        doEach(500,(i)-> {

            if(i==2){wireB.add("fire exception"); return ;}

            if(i==3){wireA.add("fire exception"); return;}

            if(i>5){
                try {
                    Thread.sleep(1500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if(i>10){
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            wireB.add(UUID.randomUUID().toString());
            wireA.add(UUID.randomUUID().toString());
        });

        tree.clearCutAwaitActs();



        ec
                .testAssert(-1,
                        4, // two lime and two recommit
                        0,
                        2,
                        0,
                        2,
                        0,
                        0,
                        1);
    }

    /**
     * Live Aye single consumer
     */

    @org.junit.Test
    public void endlessSingleLime(){


        EventsCounter ec = new EventsCounter();

        LimeTree tree=new LimeTree();

        tree.addListener(ec);

        List<String> output=new ArrayList<>();



       BlockingQueue<String> wireA= tree.newLime()
               .<String>setAct((pill, context, lime) -> {
                   if(pill.equals("fire exception")){ throw new RuntimeException("sf"); }
                   output.add((String) pill);
                   System.out.println(pill);
               })
               .setLiveAye(true)
               .create("A")
               .getWire();


        doEach(1000,(i)-> {

            if(i==2){ wireA.add("fire exception"); return;}

            if(i==5){tree.clearCutBrutal();}

            wireA.add(UUID.randomUUID().toString());
        });


        tree.clearCutAwaitActs();

       Assert.assertTrue("unexpected output size:"+output.size(),output.size()==4);


        ec
                .testAssert(5,
                        2,//one first commit one raiseAgain
                        0,
                        1,
                        1,
                        0,
                        0,
                        0,
                        1);


    }

    /**
     * DEATH WITH PILL
     */
    @org.junit.Test
    public void pillDeath(){
        EventsCounter ec = new EventsCounter();
        LimeTree tree=new LimeTree();
        tree.addListener(ec);
        BlockingQueue<Item> wireA= tree.newLime()
                .<Item>setAct((pill, context, lime) -> System.out.println(pill.toString()))
                .setLiveAye(true)
                .create("A")
                .getWire();

        doEach(300,(i)-> {
            if(i==5){
               Item item=new Item();
               item.venom=true;
                wireA.add(item);
                return;}
            wireA.add(new Item());
        });

        tree.clearCutAwaitActs();


        ec
                .testAssert(5,
                        1,
                        1,
                        0,
                        0,
                        0,
                        0,
                        0,
                        1);

    }




}
