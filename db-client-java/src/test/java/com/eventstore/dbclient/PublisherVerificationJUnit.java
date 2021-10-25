package com.eventstore.dbclient;

import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.SkipException;

import java.lang.reflect.Method;

import static org.junit.Assert.assertNotNull;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class PublisherVerificationJUnit<T> extends PublisherVerification<T> {

    public PublisherVerificationJUnit(TestEnvironment env) {
        super(env);
    }

    public PublisherVerificationJUnit(TestEnvironment env, long publisherReferenceGCTimeoutMillis) {
        super(env, publisherReferenceGCTimeoutMillis);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        mapTestNgExceptions(super::setUp);
    }

    @Test
    public void testAllTestNGTestsAreOverridden() throws Exception {
        for (Method method : PublisherVerificationJUnit.class.getMethods())
            if (method.getAnnotation(org.testng.annotations.Test.class) != null)
                assertNotNull("TestNG test must be annotated with junit @Test: " + method, method.getAnnotation(Test.class));
    }

    @Override
    @Test
    public void required_createPublisher1MustProduceAStreamOfExactly1Element() throws Throwable {
        mapTestNgExceptions(super::required_createPublisher1MustProduceAStreamOfExactly1Element);
    }

    @Override
    @Test
    public void required_createPublisher3MustProduceAStreamOfExactly3Elements() throws Throwable {
        mapTestNgExceptions(super::required_createPublisher3MustProduceAStreamOfExactly3Elements);
    }

    @Override
    @Test
    public void required_validate_maxElementsFromPublisher() throws Exception {
        mapTestNgExceptions(super::required_validate_maxElementsFromPublisher);
    }

    @Override
    @Test
    public void required_validate_boundedDepthOfOnNextAndRequestRecursion() throws Exception {
        mapTestNgExceptions(super::required_validate_boundedDepthOfOnNextAndRequestRecursion);
    }

    @Override
    @Test
    public void required_spec101_subscriptionRequestMustResultInTheCorrectNumberOfProducedElements() throws Throwable {
        mapTestNgExceptions(super::required_spec101_subscriptionRequestMustResultInTheCorrectNumberOfProducedElements);
    }

    @Override
    @Test
    public void required_spec102_maySignalLessThanRequestedAndTerminateSubscription() throws Throwable {
        mapTestNgExceptions(super::required_spec102_maySignalLessThanRequestedAndTerminateSubscription);
    }

    @Override
    @Test
    public void stochastic_spec103_mustSignalOnMethodsSequentially() throws Throwable {
        mapTestNgExceptions(super::stochastic_spec103_mustSignalOnMethodsSequentially);
    }

    @Override
    @Test
    public void optional_spec104_mustSignalOnErrorWhenFails() throws Throwable {
        mapTestNgExceptions(super::optional_spec104_mustSignalOnErrorWhenFails);
    }

    @Override
    @Test
    public void required_spec105_mustSignalOnCompleteWhenFiniteStreamTerminates() throws Throwable {
        mapTestNgExceptions(super::required_spec105_mustSignalOnCompleteWhenFiniteStreamTerminates);
    }

    @Override
    @Test
    public void optional_spec105_emptyStreamMustTerminateBySignallingOnComplete() throws Throwable {
        mapTestNgExceptions(super::optional_spec105_emptyStreamMustTerminateBySignallingOnComplete);
    }

    @Override
    @Test
    public void untested_spec106_mustConsiderSubscriptionCancelledAfterOnErrorOrOnCompleteHasBeenCalled() throws Throwable {
        mapTestNgExceptions(super::untested_spec106_mustConsiderSubscriptionCancelledAfterOnErrorOrOnCompleteHasBeenCalled);
    }

    @Override
    @Test
    public void required_spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled() throws Throwable {
        mapTestNgExceptions(super::required_spec107_mustNotEmitFurtherSignalsOnceOnCompleteHasBeenSignalled);
    }

    @Override
    @Test
    public void untested_spec107_mustNotEmitFurtherSignalsOnceOnErrorHasBeenSignalled() throws Throwable {
        mapTestNgExceptions(super::untested_spec107_mustNotEmitFurtherSignalsOnceOnErrorHasBeenSignalled);
    }

    @Override
    @Test
    public void untested_spec108_possiblyCanceledSubscriptionShouldNotReceiveOnErrorOrOnCompleteSignals() throws Throwable {
        mapTestNgExceptions(super::untested_spec108_possiblyCanceledSubscriptionShouldNotReceiveOnErrorOrOnCompleteSignals);
    }

    @Override
    @Test
    public void untested_spec109_subscribeShouldNotThrowNonFatalThrowable() throws Throwable {
        mapTestNgExceptions(super::untested_spec109_subscribeShouldNotThrowNonFatalThrowable);
    }

    @Override
    @Test
    public void required_spec109_subscribeThrowNPEOnNullSubscriber() throws Throwable {
        mapTestNgExceptions(super::required_spec109_subscribeThrowNPEOnNullSubscriber);
    }

    @Override
    @Test
    public void required_spec109_mustIssueOnSubscribeForNonNullSubscriber() throws Throwable {
        mapTestNgExceptions(super::required_spec109_mustIssueOnSubscribeForNonNullSubscriber);
    }

    @Override
    @Test
    public void required_spec109_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorAfterOnSubscribe() throws Throwable {
        mapTestNgExceptions(super::required_spec109_mayRejectCallsToSubscribeIfPublisherIsUnableOrUnwillingToServeThemRejectionMustTriggerOnErrorAfterOnSubscribe);
    }

    @Override
    @Test
    public void untested_spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice() throws Throwable {
        mapTestNgExceptions(super::untested_spec110_rejectASubscriptionRequestIfTheSameSubscriberSubscribesTwice);
    }

    @Override
    @Test
    public void optional_spec111_maySupportMultiSubscribe() throws Throwable {
        mapTestNgExceptions(super::optional_spec111_maySupportMultiSubscribe);
    }

    @Override
    @Test
    public void optional_spec111_registeredSubscribersMustReceiveOnNextOrOnCompleteSignals() throws Throwable {
        mapTestNgExceptions(super::optional_spec111_registeredSubscribersMustReceiveOnNextOrOnCompleteSignals);
    }

    @Override
    @Test
    public void optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingOneByOne() throws Throwable {
        mapTestNgExceptions(super::optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingOneByOne);
    }

    @Override
    @Test
    public void optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfront() throws Throwable {
        mapTestNgExceptions(super::optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfront);
    }

    @Override
    @Test
    public void optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfrontAndCompleteAsExpected() throws Throwable {
        mapTestNgExceptions(super::optional_spec111_multicast_mustProduceTheSameElementsInTheSameSequenceToAllOfItsSubscribersWhenRequestingManyUpfrontAndCompleteAsExpected);
    }

    @Override
    @Test
    public void required_spec302_mustAllowSynchronousRequestCallsFromOnNextAndOnSubscribe() throws Throwable {
        mapTestNgExceptions(super::required_spec302_mustAllowSynchronousRequestCallsFromOnNextAndOnSubscribe);
    }

    @Override
    @Test
    public void required_spec303_mustNotAllowUnboundedRecursion() throws Throwable {
        mapTestNgExceptions(super::required_spec303_mustNotAllowUnboundedRecursion);
    }

    @Override
    @Test
    public void untested_spec304_requestShouldNotPerformHeavyComputations() throws Exception {
        mapTestNgExceptions(super::untested_spec304_requestShouldNotPerformHeavyComputations);
    }

    @Override
    @Test
    public void untested_spec305_cancelMustNotSynchronouslyPerformHeavyComputation() throws Exception {
        mapTestNgExceptions(super::untested_spec305_cancelMustNotSynchronouslyPerformHeavyComputation);
    }

    @Override
    @Test
    public void required_spec306_afterSubscriptionIsCancelledRequestMustBeNops() throws Throwable {
        mapTestNgExceptions(super::required_spec306_afterSubscriptionIsCancelledRequestMustBeNops);
    }

    @Override
    @Test
    public void required_spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops() throws Throwable {
        mapTestNgExceptions(super::required_spec307_afterSubscriptionIsCancelledAdditionalCancelationsMustBeNops);
    }

    @Override
    @Test
    public void required_spec309_requestZeroMustSignalIllegalArgumentException() throws Throwable {
        mapTestNgExceptions(super::required_spec309_requestZeroMustSignalIllegalArgumentException);
    }

    @Override
    @Test
    public void required_spec309_requestNegativeNumberMustSignalIllegalArgumentException() throws Throwable {
        mapTestNgExceptions(super::required_spec309_requestNegativeNumberMustSignalIllegalArgumentException);
    }

    @Override
    @Test
    public void optional_spec309_requestNegativeNumberMaySignalIllegalArgumentExceptionWithSpecificMessage() throws Throwable {
        mapTestNgExceptions(super::optional_spec309_requestNegativeNumberMaySignalIllegalArgumentExceptionWithSpecificMessage);
    }

    @Override
    @Test
    public void required_spec312_cancelMustMakeThePublisherToEventuallyStopSignaling() throws Throwable {
        mapTestNgExceptions(super::required_spec312_cancelMustMakeThePublisherToEventuallyStopSignaling);
    }

    @Override
    @Test
    public void required_spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber() throws Throwable {
        mapTestNgExceptions(super::required_spec313_cancelMustMakeThePublisherEventuallyDropAllReferencesToTheSubscriber);
    }

    @Override
    @Test
    public void required_spec317_mustSupportAPendingElementCountUpToLongMaxValue() throws Throwable {
        mapTestNgExceptions(super::required_spec317_mustSupportAPendingElementCountUpToLongMaxValue);
    }

    @Override
    @Test
    public void required_spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue() throws Throwable {
        mapTestNgExceptions(super::required_spec317_mustSupportACumulativePendingElementCountUpToLongMaxValue);
    }

    @Override
    @Test
    public void required_spec317_mustNotSignalOnErrorWhenPendingAboveLongMaxValue() throws Throwable {
        mapTestNgExceptions(super::required_spec317_mustNotSignalOnErrorWhenPendingAboveLongMaxValue);
    }

    private static final <T extends Throwable> void mapTestNgExceptions(ThrowingRunnable<T> runnable) throws T {
        try {
            runnable.run();
        }
        catch (SkipException e) {
            throw new AssumptionViolatedException("TestNG SkipException", e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable<T extends Throwable> {
        void run() throws T;
    }
}