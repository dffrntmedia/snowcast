package com.noctarius.snowcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

public class DffrntLabSnowcastUseCaseTest {
    @Test
    public void snowcastSequencerLifecycleTest() throws InterruptedException {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        Snowcast snowcast = SnowcastSystem.snowcast(hazelcastInstance);

        Calendar calendar = GregorianCalendar.getInstance();

        calendar.set(2018, Calendar.JANUARY, 1, 0, 0, 0);

        SnowcastEpoch epoch = SnowcastEpoch.byInstant(calendar.toInstant());

        SnowcastSequencer sequencer = snowcast.createSequencer("dffrntlLabSequencer", epoch);

        sequencer.next();

        snowcast.destroySequencer(sequencer);
    }
}
