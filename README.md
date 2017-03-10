## **Simple Example**
### Tree with one Lime that  consumes String through Act function that simply print it.


    LimeTree tree=new LimeTree();
    
    tree.<String>newLime().setAct((item, context,consumer) ->   System.out.println(e);})
    tree.clearCutAwaitActs();//blocks main thread until tree or all fruits finish

## **Minimum Item Speed Flow**
### Consumer finish if waits  more than 500 millis for an item.

    LimeTree tree=new LimeTree();
    
    tree.<String>newLime().setWilting(500).setAct((item, context,consumer) ->   System.out.println(e);})
    tree.clearCutAwaitActs();//blocks main thread until tree or all fruits finish



## **Minimum Tree Item Speed Flow**
### Consumers finish if the common Items stream is slower than  500 millis.

    LimeTree tree=new LimeTree();
    tree.setTimeoutForInactivity(500)
    
    tree.<String>newLime().setAct((item, context,consumer) ->   System.out.println(e);})
    tree.<String>newLime().setAct((item, context,consumer) ->   /*i'm another consumer*/);})
    tree.clearCutAwaitActs();//blocks main thread until tree or all fruits finish


## **Consumers Interaction**
### In this example is shown how two consumers can dialogue.


        /**LIME A DEFINITION**/
        BlockingQueue<String> wireA = tree
                .<String>newLime() .setAct((item, treeContex, MeConsumer) -> {
                    try {
                            //editing the String item adding "_A" suffix and put it in a B consumer's queue
                           contextTree.<String>getWireBy("B").put(item+"_A");
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    }
                })
                .create("A")
                .getWire();

        /**LIME B DEFINITION**/
         tree
                .<String>newLime() .setAct((item, treeContext, Consumer) -> system.out.print(item)
                .create("B");


         wireA.put("test);
         tree.clearCutAwaitActs();//blocks main thread until tree or all fruits finish





