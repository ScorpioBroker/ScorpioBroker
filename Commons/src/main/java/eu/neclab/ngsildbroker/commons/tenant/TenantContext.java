package eu.neclab.ngsildbroker.commons.tenant;

public class TenantContext {
	
    private static ThreadLocal<String> currentTenant = new ThreadLocal<>();

    public static void setCurrentTenant(String tenantidvalue) {
        currentTenant.set(tenantidvalue);
    }

    public static String getCurrentTenant() {
        return currentTenant.get();
    }
}