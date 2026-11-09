package com.manas.kafkaplatform.k8splatform.admission;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The reference oracle: for each of the 40 seeded misconfigurations, an independently stated
 * expected outcome ({@link SeededCases}) is checked against what the validator actually does.
 * The honesty check behind the "40 of 40" claim: it is not enough that a request is rejected,
 * the rejection must name the rule it was designed to violate.
 */
class ValidationReferenceOracleTest {

    @TestFactory
    Stream<DynamicTest> everySeededCaseIsRejectedByTheExpectedRule() {
        List<SeededCase> cases = SeededCases.all();
        assertEquals(40, cases.size(), "the seeded case table itself must contain exactly 40 cases");
        return cases.stream().map(c -> DynamicTest.dynamicTest(c.id() + ": " + c.description(), () -> {
            AdmissionRejectedException ex = assertThrows(AdmissionRejectedException.class,
                    () -> ClusterRequestValidator.validate(c.request()),
                    () -> c.id() + " (" + c.description() + ") was accepted but should have failed " + c.expectedRule().ruleId());

            if (ex.rule() != c.expectedRule()) {
                fail(c.id() + " (" + c.description() + ") failed rule " + ex.rule().ruleId()
                        + " but the reference oracle expected " + c.expectedRule().ruleId());
            }
        }));
    }
}
