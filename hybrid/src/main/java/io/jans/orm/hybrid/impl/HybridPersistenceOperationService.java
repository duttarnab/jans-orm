package io.jans.orm.hybrid.impl;

import java.util.List;

import io.jans.orm.exception.extension.PersistenceExtension;
import io.jans.orm.operation.PersistenceOperationService;

/**
 * Hybrid Operation Service
 *
 * @author Yuriy Movchan Date: 05/13/2018
 */
public class HybridPersistenceOperationService implements PersistenceOperationService {

	private List<PersistenceOperationService> persistenceOperationServices;

	public HybridPersistenceOperationService(List<PersistenceOperationService> persistenceOperationServices) {
		this.persistenceOperationServices = persistenceOperationServices;
	}

	@Override
	public boolean isConnected() {
		for(PersistenceOperationService persistenceOperationService : persistenceOperationServices) {
			if (!persistenceOperationService.isConnected()) {
				return false;
			}
		}

		return true;
	}

	public List<PersistenceOperationService> getPersistenceOperationServices() {
		return persistenceOperationServices;
	}

	@Override
	public void setPersistenceExtension(PersistenceExtension persistenceExtension) {
		for(PersistenceOperationService persistenceOperationService : persistenceOperationServices) {
			persistenceOperationService.setPersistenceExtension(persistenceExtension);
		}
	}

}