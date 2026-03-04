package io.mosip.registration.processor.stages.utils;

import io.mosip.kernel.biometrics.commons.CbeffValidator;
import io.mosip.kernel.biometrics.entities.BIR;
import io.mosip.kernel.biometrics.entities.BiometricRecord;
import io.mosip.kernel.cbeffutil.container.impl.CbeffContainerImpl;
import io.mosip.kernel.core.logger.spi.Logger;
import io.mosip.registration.processor.core.constant.LoggerFileConstant;
import io.mosip.registration.processor.core.logger.RegProcessorLogger;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URL;

@Component
public class BiometricsXSDValidator {

    private static final Logger regProcLogger = RegProcessorLogger.getLogger(BiometricsXSDValidator.class);

    @Value("${mosip.kernel.xsdstorage-uri}")
    private String configServerFileStorageURL;
    
    @Value("${mosip.kernel.xsdfile}")
    private String schemaFileName;
    
    private byte[] xsd = null;

    public void validateXSD(BiometricRecord biometricRecord ) throws Exception  {
        long startMs = System.currentTimeMillis();
        if(xsd==null) {
            try (InputStream inputStream = new URL(configServerFileStorageURL + schemaFileName).openStream()) {
                xsd =  IOUtils.toByteArray(inputStream);
            }
        }
            CbeffContainerImpl cbeffContainer = new CbeffContainerImpl();
			BIR bir = cbeffContainer.createBIRType(biometricRecord.getSegments());
        CbeffValidator.createXMLBytes(bir, xsd);//validates XSD
        regProcLogger.debug(LoggerFileConstant.SESSIONID.toString(), LoggerFileConstant.REGISTRATIONID.toString(), "",
                "[PACKET_VALIDATOR_TIMING] BiometricsXSDValidator.validateXSD completed in " + (System.currentTimeMillis() - startMs) + " ms");
    } 

	
}
