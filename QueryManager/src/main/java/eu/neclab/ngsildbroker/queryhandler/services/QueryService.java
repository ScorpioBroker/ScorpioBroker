package eu.neclab.ngsildbroker.queryhandler.services;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.CSourceDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;

@Singleton
public class QueryService extends BaseQueryService {

	@Inject
	QueryDAO queryDAO;

	@Inject
	CSourceDAO cSourceDAO;

	@Override
	protected StorageDAO getQueryDAO() {
		return queryDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		return cSourceDAO;
	}

}