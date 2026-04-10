package io.mosip.registartion.processor.abis.middleware.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.mosip.registration.processor.packet.storage.entity.AbisRequestEntity;
import io.mosip.registration.processor.packet.storage.repository.BasePacketRepository;

/**
 * Commits ABIS request status in a separate transaction so SENT/FAILED is visible to other
 * threads (e.g. fast inbound responses) without waiting for a larger outer transaction to finish.
 */
@Component
public class AbisRequestSendStatusCommitUtility {

	@Autowired
	private BasePacketRepository<AbisRequestEntity, String> abisRequestRepository;

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void commitStatus(AbisRequestEntity abisReqEntity) {
		abisRequestRepository.saveAndFlush(abisReqEntity);
	}
}
