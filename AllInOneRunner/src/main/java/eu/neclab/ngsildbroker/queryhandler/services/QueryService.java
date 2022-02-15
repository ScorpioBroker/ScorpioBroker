package eu.neclab.ngsildbroker.queryhandler.services;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryCSourceDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;

@ApplicationScoped
public class QueryService extends BaseQueryService {

	@Inject
	QueryDAO queryDAO;

	@Inject
	QueryCSourceDAO cSourceDAO;

	@Override
	protected StorageDAO getQueryDAO() {
		return queryDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		return cSourceDAO;
	}

}