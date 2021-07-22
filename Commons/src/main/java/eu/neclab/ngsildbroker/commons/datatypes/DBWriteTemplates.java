package eu.neclab.ngsildbroker.commons.datatypes;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class DBWriteTemplates {
	
	
	private JdbcTemplate writerJdbcTemplateWithTransaction;
	private TransactionTemplate writerTransactionTemplate;
	private JdbcTemplate writerJdbcTemplate;
	
	public DBWriteTemplates() {
		
	}
	public DBWriteTemplates(JdbcTemplate writerJdbcTemplateWithTransaction,
			TransactionTemplate writerTransactionTemplate, JdbcTemplate writerJdbcTemplate) {
		super();
		this.writerJdbcTemplateWithTransaction = writerJdbcTemplateWithTransaction;
		this.writerTransactionTemplate = writerTransactionTemplate;
		this.writerJdbcTemplate = writerJdbcTemplate;
	}
	public JdbcTemplate getWriterJdbcTemplateWithTransaction() {
		return writerJdbcTemplateWithTransaction;
	}
	public void setWriterJdbcTemplateWithTransaction(JdbcTemplate writerJdbcTemplateWithTransaction) {
		this.writerJdbcTemplateWithTransaction = writerJdbcTemplateWithTransaction;
	}
	public TransactionTemplate getWriterTransactionTemplate() {
		return writerTransactionTemplate;
	}
	public void setWriterTransactionTemplate(TransactionTemplate writerTransactionTemplate) {
		this.writerTransactionTemplate = writerTransactionTemplate;
	}
	public JdbcTemplate getWriterJdbcTemplate() {
		return writerJdbcTemplate;
	}
	public void setWriterJdbcTemplate(JdbcTemplate writerJdbcTemplate) {
		this.writerJdbcTemplate = writerJdbcTemplate;
	}
	
	

}
