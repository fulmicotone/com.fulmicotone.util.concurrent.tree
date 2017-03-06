package com.fulmicotone.util.concurrent;

/**
 * Created by dino on 03/03/2017.
 */
public interface TreeLifeCycleListener {

     <T> void  onPillIncome(LimeTree ctx,LimeTree.Lime lime,T pill);//k
     void onLimeCommit(LimeTree ctx,LimeTree.Lime lime);
     void onLimeFinish(LimeTree ctx, LimeTree.Lime lime, ELimeEnd reason, Throwable... exceptions);//k
     void onTreeShutdown(LimeTree tree, ELimeTreeEnd reason, Throwable ... exceptions);
}
