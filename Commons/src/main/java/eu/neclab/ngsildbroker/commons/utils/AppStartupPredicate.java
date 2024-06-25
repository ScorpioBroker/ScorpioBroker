package eu.neclab.ngsildbroker.commons.utils;

import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled.SkipPredicate;
import io.quarkus.scheduler.ScheduledExecution;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Singleton;

@Singleton
public class AppStartupPredicate implements SkipPredicate {

    private boolean appStarted = false;

    void onStart(@Observes StartupEvent event) {
        appStarted = true;
    }

    public boolean test(ScheduledExecution execution) {
        return appStarted;
    }
}
