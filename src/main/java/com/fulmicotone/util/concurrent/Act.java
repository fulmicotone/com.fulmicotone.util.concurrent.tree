package com.fulmicotone.util.concurrent;

/**
 * Created by dino on 24/02/2017.
 */

@FunctionalInterface
public interface Act<E> {

    void forEach(E pill,LimeTree context,LimeTree.Lime consumer);

}
