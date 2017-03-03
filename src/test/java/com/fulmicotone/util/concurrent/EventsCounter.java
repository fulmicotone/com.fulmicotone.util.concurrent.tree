package com.fulmicotone.util.concurrent;

import org.junit.Assert;

/**
 * Created by dino on 03/03/2017.
 */
public class EventsCounter implements  TreeLifeCycleListener {

    int pillsIncoming=0;
    int limeCommit=0;
    int limeEndPillReason=0;
    int limeEndTimeout =0;
    int limeEndError=0;
    int limeEndTreeDeath=0;
    int treeShutdownTimeout=0;
    int treeShutdownRegular=0;
    int treeShutdownError =0;



    @Override public <T> void onPillIncome(LimeTree ctx, LimeTree.Lime lime, T pill) { pillsIncoming++;}

    @Override public void onLimeCommit(LimeTree ctx, LimeTree.Lime lime) { limeCommit++;}

    @Override
    public void onLimeFinish(LimeTree ctx, LimeTree.Lime lime, ELimeEnd reason, Throwable... exceptions) {

        switch (reason){
            case DEATH_PILL:
                limeEndPillReason++;
                break;
            case ERROR:
                limeEndError++;
                break;
            case WILTING:
                limeEndTimeout++;
                break;

            case TREE_DEATH:
                this.limeEndTreeDeath++;
                break;
        }
    }

    @Override
    public void onTreeShutdown(LimeTree tree, ELimeTreeEnd reason, Throwable... exceptions) {

        switch (reason){
            case REGULAR:
                treeShutdownRegular++;
                break;
            case ERROR:
                treeShutdownError++;
                break;
            case INACTIVITY:
                treeShutdownTimeout++;
                break;
        }

    }




    public void testAssert(int pillsIncoming,
                           int limeCommit,
                           int limeEndForPill,
                           int limeEndForError,
                           int limeEndForTreeDeath,
                           int limeEndWilting,
                           int treeShutdownInact,
                           int treeShutdownError,
                           int treeShutdownRegular){

       if(pillsIncoming!=-1) Assert.assertTrue("pillsIncoming Unexpected:"+this.pillsIncoming,pillsIncoming==this.pillsIncoming);
        Assert.assertTrue("limeCommit Unexpected:"+this.limeCommit,limeCommit==this.limeCommit);
        Assert.assertTrue("limeEndForTreeDeath Unexpected:"+this.limeEndTreeDeath,this.limeEndTreeDeath==limeEndForTreeDeath);
        Assert.assertTrue("limeEndForPill Unexpected:"+this.limeEndPillReason,this.limeEndPillReason==limeEndForPill);
        Assert.assertTrue("limeEndForError Unexpected:"+this.limeEndError,this.limeEndError==limeEndForError);
        Assert.assertTrue("limeEndWilting Unexpected:"+this.limeEndTimeout,this.limeEndTimeout==limeEndWilting);
        Assert.assertTrue("treeShutdownInact Unexpected:"+this.treeShutdownTimeout,this.treeShutdownTimeout==treeShutdownInact);
        Assert.assertTrue("treeShutdownError Unexpected:"+this.treeShutdownError,this.treeShutdownError ==treeShutdownError);
        Assert.assertTrue("treeShutdownRegular Unexpected:"+this.treeShutdownRegular,this.treeShutdownRegular==treeShutdownRegular);



    }







}
