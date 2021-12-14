package eu.neclab.ngsildbroker.commons.interfaces;

import java.net.URI;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceQueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Query;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:06
 */
public interface ContextRegistryInterface extends SubscriptionManager, CSourceQueryHandler {

	/**
	 * 
	 * @param id
	 */
	public void deleteCSourceRegistration(URI id);

	/**
	 * 
	 * @param query
	 */
	public CSourceQueryResult query(Query query);

	/**
	 * 
	 * @param id
	 */
	public Subscription querySubscription(URI id);

	/**
	 * 
	 * @param source
	 */
	public URI registerCSource(CSourceRegistration source);

	/**
	 * 
	 * @param subscription
	 */
	public URI subscribe(Subscription subscription);

	/**
	 * 
	 * @param id
	 */
	public void unsubscribe(URI id);

	/**
	 * 
	 * @param id
	 * @param update
	 */
	public void updateCSourceRegistry(URI id, CSourceRegistration update);

	/**
	 * 
	 * @param subscription
	 */
	public Subscription updateSubscription(Subscription subscription);

}