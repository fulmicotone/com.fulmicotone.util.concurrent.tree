package com.fulmicotone.util.concurrent;

import java.util.function.Function;

/**
 * Created by dino on 28/02/2017.
 */
public class FnLimeDesc implements Function<LimeTree.Lime,String> {
    @Override
    public String apply(LimeTree.Lime lime) {
        return String.format(
                "{'key':%s ," +
                "'consumeAsync':%s," +
                "'wilting':%s," +
                "'liveAye':%s," +
                "'autoCommit':%s}",
                lime.key,
                lime.consumeAsync,
                lime.wiltingInMillis,
                lime.liveAye,
                lime.autoCommit);
    }
}
