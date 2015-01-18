package com.datastax.driver.core;

import java.math.BigInteger;

import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class TokenRangeTest {
    // The tests in this class don't depend on the kind of factory used, so use Murmur3 everywhere
    Token.Factory factory = Token.getFactory("Murmur3Partitioner");

    @Test(groups = "unit")
    public void should_check_intersection() {
        // NB - to make the test more visual, we use watch face numbers
        assertThat(tokenRange(3, 9))
            .doesNotIntersect(tokenRange(11, 1))
            .doesNotIntersect(tokenRange(1, 2))
            .doesNotIntersect(tokenRange(11, 3))
            .doesNotIntersect(tokenRange(2, 3))
            .doesNotIntersect(tokenRange(3, 3))
            .intersects(tokenRange(2, 6))
            .intersects(tokenRange(2, 10))
            .intersects(tokenRange(6, 10))
            .intersects(tokenRange(4, 8))
            .intersects(tokenRange(3, 9))
            .doesNotIntersect(tokenRange(9, 10))
            .doesNotIntersect(tokenRange(10, 11))
        ;
        assertThat(tokenRange(9, 3))
            .doesNotIntersect(tokenRange(5, 7))
            .doesNotIntersect(tokenRange(7, 8))
            .doesNotIntersect(tokenRange(5, 9))
            .doesNotIntersect(tokenRange(8, 9))
            .doesNotIntersect(tokenRange(9, 9))
            .intersects(tokenRange(8, 2))
            .intersects(tokenRange(8, 4))
            .intersects(tokenRange(2, 4))
            .intersects(tokenRange(10, 2))
            .intersects(tokenRange(9, 3))
            .doesNotIntersect(tokenRange(3, 4))
            .doesNotIntersect(tokenRange(4, 5))
        ;
        assertThat(tokenRange(3, 3)).doesNotIntersect(tokenRange(3, 3));
    }

    @Test(groups = "unit")
    public void should_merge_with_other_range() {
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(2, 3))).isEqualTo(tokenRange(2, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(2, 4))).isEqualTo(tokenRange(2, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(11, 3))).isEqualTo(tokenRange(11, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(11, 4))).isEqualTo(tokenRange(11, 9));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(4, 8))).isEqualTo(tokenRange(3, 9));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(3, 9))).isEqualTo(tokenRange(3, 9));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 11))).isEqualTo(tokenRange(3, 11));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(8, 11))).isEqualTo(tokenRange(3, 11));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 1))).isEqualTo(tokenRange(3, 1));
        assertThat(tokenRange(3, 9).mergeWith(tokenRange(8, 1))).isEqualTo(tokenRange(3, 1));

        assertThat(tokenRange(3, 9).mergeWith(tokenRange(9, 3))).isEqualTo(tokenRange(3, 3));


        assertThat(tokenRange(9, 3).mergeWith(tokenRange(8, 9))).isEqualTo(tokenRange(8, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(8, 10))).isEqualTo(tokenRange(8, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(4, 9))).isEqualTo(tokenRange(4, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(4, 10))).isEqualTo(tokenRange(4, 3));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(10, 2))).isEqualTo(tokenRange(9, 3));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(9, 3))).isEqualTo(tokenRange(9, 3));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 5))).isEqualTo(tokenRange(9, 5));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(2, 5))).isEqualTo(tokenRange(9, 5));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 7))).isEqualTo(tokenRange(9, 7));
        assertThat(tokenRange(9, 3).mergeWith(tokenRange(2, 7))).isEqualTo(tokenRange(9, 7));

        assertThat(tokenRange(9, 3).mergeWith(tokenRange(3, 9))).isEqualTo(tokenRange(9, 9));
    }

    private TokenRange tokenRange(int start, int end) {
        return new TokenRange(factory.newToken(BigInteger.valueOf(start)), factory.newToken(BigInteger.valueOf(end)), factory);
    }
}