package com.fulmicotone.util.concurrent;

import java.util.concurrent.BlockingQueue;

/**
 * this interface allows to limit methods exposed  inside the act
 */
public interface ILime<E> {

     BlockingQueue<E> getWire();

}


