// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/analysis/IsNullPredicateTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

public class IsNullPredicateTest {
    @Mocked
    Analyzer analyzer;

    @Test
    public void testNullParam() {
        IsNullPredicate isNullPredicate = new IsNullPredicate(new NullLiteral(), false);

        try {
            isNullPredicate.analyzeImpl(analyzer);
        } catch (AnalysisException e) {
            Assert.fail();
        }
        Assert.assertEquals(isNullPredicate.getFn(), IsNullPredicate.isNullFN);

        IsNullPredicate isNotNullPredicate = new IsNullPredicate(new NullLiteral(), true);

        try {
            isNotNullPredicate.analyzeImpl(analyzer);
        } catch (AnalysisException e) {
            Assert.fail();
        }
        Assert.assertEquals(isNotNullPredicate.getFn(), IsNullPredicate.isNotNullFN);
    }

    @Test
    public void testNormal() {
        try {
            Expr isNull = new IsNullPredicate(new BoolLiteral(true), false);
            Assert.assertTrue(isNull.getResultValue() instanceof BoolLiteral);
            Assert.assertFalse(((BoolLiteral) isNull.getResultValue()).getValue());

            isNull = new IsNullPredicate(new NullLiteral(), false);
            Assert.assertTrue(isNull.getResultValue() instanceof BoolLiteral);
            Assert.assertTrue(((BoolLiteral) isNull.getResultValue()).getValue());

            isNull = new IsNullPredicate(new BoolLiteral(true), true);
            Assert.assertTrue(isNull.getResultValue() instanceof BoolLiteral);
            Assert.assertTrue(((BoolLiteral) isNull.getResultValue()).getValue());

            isNull = new IsNullPredicate(new NullLiteral(), true);
            Assert.assertTrue(isNull.getResultValue() instanceof BoolLiteral);
            Assert.assertFalse(((BoolLiteral) isNull.getResultValue()).getValue());
        } catch (AnalysisException e) {
            Assert.fail();
        }
    }
}
