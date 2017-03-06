package com.fulmicotone.util.concurrent;

/**
 * Created by dino on 27/02/2017.
 */
public class Pill {

    protected  boolean venom=false;

    public Pill(){}

    public Pill(boolean venom){ this.venom=venom;}

    public static Pill OF_DEATH=new Pill(true);
}
