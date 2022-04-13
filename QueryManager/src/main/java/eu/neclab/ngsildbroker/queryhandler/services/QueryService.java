package eu.neclab.ngsildbroker.queryhandler.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.CSourceDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;

@Service
public class QueryService extends BaseQueryService {

	@Autowired
	QueryDAO queryDAO;

	@Autowired
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