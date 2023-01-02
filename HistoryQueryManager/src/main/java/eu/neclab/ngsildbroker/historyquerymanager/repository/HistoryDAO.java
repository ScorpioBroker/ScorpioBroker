package eu.neclab.ngsildbroker.historyquerymanager.repository;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.storage.ClientManager;

@Singleton
public class HistoryDAO {

	@Inject
	ClientManager clientManager;

	
}