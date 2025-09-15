package eu.neclab.ngsildbroker.commons.serialization.messaging;

import org.flywaydb.core.internal.exception.sqlExceptions.FlywaySqlNoIntegratedAuthException;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = {

        FlywaySqlNoIntegratedAuthException.class
},
        // We need to register the methods to avoid the NoSuchMethodException
        methods = true, registerFullHierarchy = true)
public class AdditionNativeConfig {

}
