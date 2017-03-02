package com.fulmicotone.util.concurrent;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class LimeTree implements  ITree{

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final Map<String, Lime> fruitsO2Map = new LinkedHashMap<>();
    private ExecutorService treeLocalExecutorService = Executors.newCachedThreadPool();
    private BlockingQueue<Pill> treeActivitiesQueue = new LinkedBlockingQueue<>();
    private volatile long cutOnInactivityForMillis = -1;
    private boolean activitiesMonitorIsActive=false;
    private Phaser phaser=new Phaser();


    public LimeTree() {}

    public <E> BlockingQueue<E> getWireBy(String key) {

        return Optional.ofNullable(fruitsO2Map.get(key))
                .map(l -> l.queue)
                .get();
    }

    public boolean submitUncommitedFruits() {

        commitFruits(fruitsO2Map.values()
                .stream()
                .filter(c -> c.isCommitted == false)
                .collect(Collectors.toList())
                .toArray(new Lime[0]));
        return true;
    }

    /**SHUTDOWN METHOD Start **/

    /**
     * set variable that indicates to
     * every fruits that we want to stop whole system
     */
    public void clearCutBrutal() {this.treeLocalExecutorService.shutdownNow();}

    /**
     * @return close the tree executorService when every fruits isFinished
     */
    public void clearCutAwaitActs(long wait) {


        try {

            if(wait==-1){
                phaser.awaitAdvanceInterruptibly(0);

            }else {

                phaser.awaitAdvanceInterruptibly(0, wait, TimeUnit.MILLISECONDS);
            }

        } catch (InterruptedException e) { log.error("something is gone wrong");}
        catch (TimeoutException e) {
            log.info("time reserved to system expired!! force shutdown");

        }
        this.clearCutBrutal();
       }


    /**
     * @return close the tree executorService when every fruits isFinished
     */
    public void clearCutAwaitActs() {
        clearCutAwaitActs(-1);
    }

    /**
     * @return stop whole system after millis of inactivities
     */
    public void setTimeoutForInactivity(long inactivityTime) {
        this.cutOnInactivityForMillis = inactivityTime;
        tryCommitActivitiesMonitor();
    }

    /**
     * SHUTDOWN METHOD End
     **/

    public <E> Lime.FruitBuilder<E> newLime() {
        return new Lime.FruitBuilder(this);
    }

    private void tryCommitActivitiesMonitor() {

        if (this.cutOnInactivityForMillis != -1 &&//stop if inactivity is set
                this.activitiesMonitorIsActive == false)//monitor isn't already active
        {
            log.debug("activating activities monitor!");
            this.treeLocalExecutorService.submit(new ActivitiesMonitor(this));
            this.activitiesMonitorIsActive = true;
            log.debug("activities monitor active!");

        }

    }


    private boolean addFruit(Lime f) {

        log.debug("add fruit with key: {} ", f.key);
        log.debug( String.format(
                "{'key':%s ," +
                        "'consumeAsync':%s," +
                        "'wilting':%s," +
                        "'liveAye':%s," +
                        "'autoCommit':%s}",
                f.key,
                f.consumeAsync,
                f.wiltingInMillis,
                f.liveAye,
                f.autoCommit));
        Lime previus = this.fruitsO2Map.put(f.key, f);
        log.debug("after add limes in tree are: {} ", fruitsO2Map.size());
        if (f.autoCommit) {
            log.debug("commit fruit with key: {} ", f.key);
            commitFruits(f);
        }
        return previus != null;
    }


    private void commitFruits(Lime... fruits) {

        Arrays.asList(fruits)
                .stream()
                .forEach(this.treeLocalExecutorService::submit);
        tryCommitActivitiesMonitor();

    }


    private void riseAgain(Lime dead) {
        Lime raised = new Lime<>();
        raised.limeTree=dead.limeTree;
        raised.act = dead.act;
        raised.queue = dead.queue;
        raised.liveAye = dead.liveAye;
        raised.consumeAsync = dead.consumeAsync;
        raised.autoCommit = true;
        raised.wiltingInMillis = dead.wiltingInMillis;
        raised.key = dead.key;
        raised.isRaised=true;
        addFruit(raised);

    }


    private class ActivitiesMonitor implements Runnable {

        private final LimeTree tree;

        public ActivitiesMonitor(LimeTree tree) {
            this.tree = tree;
        }

        @Override
        public void run() {

                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        Pill pill = treeActivitiesQueue
                                .poll(tree.cutOnInactivityForMillis, TimeUnit.MILLISECONDS);
                        if (pill == null) {
                            log.info("monitor catch inactivity!!");
                            this.tree.clearCutBrutal();
                        }
                    }
                } catch (Exception exception) {
                    log.debug("exception in Monitor runnable " + exception.toString());
                    tryCommitActivitiesMonitor();
                }
            }
    }


    /**
     * fruit is a consumer runnuble
     * able to restart in case of error
     * to commit the output on another fruit task queue
     * terminate after millis of inactivity
     * consume the queue in async way
     */
    public static class Lime<E> implements Runnable,ILime<E> {

        private Logger log = LoggerFactory.getLogger(this.getClass());
        private LimeTree limeTree;
        private Act act;
        private BlockingQueue<E> queue;
        private ExecutorService foreEachExecutor;
        //config
        private boolean liveAye;
        private boolean autoCommit = false;
        private boolean consumeAsync;
        private long wiltingInMillis;
        //private
        private boolean isCommitted = false;
        private boolean isFinished = false;
        private  boolean isRaised=false;
        private String key;


        private Lime() {}

        public BlockingQueue<E> getWire() {
            return this.queue;
        }

        public void run() {

            log.info(String.format("Consumer %s start!", this.toString()));
            boolean exit = false;
            this.isCommitted = true;
            if(this.isRaised==false) {this.limeTree.phaser.register();}
            boolean raising=false;
            //executor
            if (this.consumeAsync &&
                    this.foreEachExecutor == null) {
                this.foreEachExecutor = Executors.newSingleThreadExecutor();
            }

            try {
                while (!Thread.currentThread().isInterrupted() && exit == false) {

                    final E pill;

                    if (wiltingInMillis == -1) {
                        //waiting until element become available
                        pill = this.queue.take();
                    } else {

                        //waiting until the patience finish¡
                        pill = this.queue
                                .poll(this.wiltingInMillis, TimeUnit.MILLISECONDS);
                    }

                    if (limeTree.activitiesMonitorIsActive) {limeTree.treeActivitiesQueue.put(new Pill());}

                    if (!(exit = pill == null)) {
                        if (this.foreEachExecutor != null) {
                            CompletableFuture
                                    .runAsync(() -> act.forEach(pill, limeTree, this), this.foreEachExecutor);
                        } else {
                            act.forEach(pill, limeTree, this);
                        }

                    }

                }

            } catch (InterruptedException ex1){
                log.info("{} has been interrupted",this.toString());

            } catch (Exception ex) {
                //check if must restart
                log.error("error on forEach {} ", ex.toString());
                if (this.liveAye) {
                    log.info(String.format("Consumer %s death and riseAgain miracle!", this.toString()));
                    limeTree.riseAgain(this);
                    raising=true;
                }
            }

            if(raising==false){
                log.info("deregister"+this.toString());
                this.limeTree.phaser.arriveAndDeregister();
            }
            this.isFinished = true;
            if(exit){log.info(String.format("Consumer %s wilted and fallen!", this.toString()));}
            log.info(String.format("Consumer %s end!", this.toString()));
        }

        /**
         * set tree
         * act
         * patience
         * liveAye
         * wireQueue
         * consume async
         */
        public static class FruitBuilder<E> {

            private LimeTree treeProducer;
            private BlockingQueue<E> wireQueue;
            private boolean liveAye;
            private Act<E> act;
            private boolean consumeAsync = false;
            private long patienceBeforeWilting = -1;
            private boolean autocommit = true;

            private FruitBuilder(LimeTree tree) {
                this.treeProducer = tree;
            }

            /**
             * set the wireQueue consumed by the fruit
             *
             * @param wireQueue
             * @param <E>
             * @return
             */
            public <E extends BlockingQueue> FruitBuilder setWire(E wireQueue) {

                this.wireQueue = wireQueue;
                return this;
            }

            /**
             * set if on error the fruit will be recommitted in the executors
             *
             * @param deathless
             * @return
             */
            public FruitBuilder setLiveAye(boolean deathless) {
                this.liveAye = deathless;
                return this;
            }

            /**
             * set the time after witch the fruit die
             **/
            public FruitBuilder setWilting(long millis) {
                this.patienceBeforeWilting = millis;
                return this;
            }

            public FruitBuilder setAutoCommit(boolean autocommit) {
                this.autocommit = autocommit;
                return this;
            }

            /**
             * set the operations each time
             **/
            public FruitBuilder setAct(Act<E> act) {
                this.act = act;
                return this;
            }

            /**
             * se if inside for each will be in other thread
             *
             * @param async
             * @return
             */
            public FruitBuilder setConsumeAsync(boolean async) {
                this.consumeAsync = async;
                return this;
            }

            public FruitBuilder setProducerTree(LimeTree treeProducer) {
                this.treeProducer = treeProducer;
                return this;
            }

            public Lime create() {
                return this.create(null);
            }

            public Lime create(String key) {
                Objects.requireNonNull(this.act);
                Lime limeToAdd = new Lime();
                key = key == null ? UUID.randomUUID().toString() : key;
                limeToAdd.act = this.act;
                limeToAdd.queue = this.wireQueue == null ? new LinkedTransferQueue() : this.wireQueue;
                limeToAdd.limeTree = this.treeProducer;
                limeToAdd.liveAye = this.liveAye;
                limeToAdd.wiltingInMillis = this.patienceBeforeWilting;
                limeToAdd.consumeAsync = this.consumeAsync;
                limeToAdd.autoCommit = this.autocommit;
                limeToAdd.key = key;
                treeProducer.addFruit(limeToAdd);
                return limeToAdd;
            }
        }
        @Override
        public String toString() {
            return String
                    .format("Lime:{ key:'%s'}",
                            this.key);
        }
    }
}











