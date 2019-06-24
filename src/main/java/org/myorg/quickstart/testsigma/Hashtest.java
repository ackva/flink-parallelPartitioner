package org.myorg.quickstart.testsigma;

import org.apache.flink.util.MathUtils;

public class Hashtest {

    public static void main(String[] args) {

        System.out.println("761081,488266");
        System.out.println(MathUtils.murmurHash(761081));
        System.out.println(MathUtils.murmurHash(488266));
        System.out.println(MathUtils.murmurHash(488266)*MathUtils.murmurHash(761081));
        System.out.println("600960,233822");
        System.out.println(MathUtils.murmurHash(600960));
        System.out.println(MathUtils.murmurHash(233822));
        System.out.println(MathUtils.murmurHash(600960)*MathUtils.murmurHash(233822));

    }
}
