/*-
 * #%L
 * athena-udfs
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.udfs;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.amazonaws.services.sagemaker.AmazonSageMaker;
import com.amazonaws.services.sagemaker.AmazonSageMakerClientBuilder;
import com.amazonaws.services.sagemaker.model.*;

public class AthenaUDFHandlerTest
{
  
    private AthenaUDFHandler m_athenaUDFHandler;
    private CreateTrainingJobResult m_traningResult;

    @Before
    public void setup()
    {
        AmazonSageMaker sagemaker = mock(AmazonSageMaker.class);
        m_athenaUDFHandler = new AthenaUDFHandler(sagemaker, "my-s3bucket", "my-train-dataset.csv", 
            "811284229777.dkr.ecr.us-east-1.amazonaws.com", "ml.c5.xlarge", 1, 30, "3","0.1","0.5","auc","binary:logistic","2.0","100");
        CreateTrainingJobRequest request = m_athenaUDFHandler.getTrainingJobRequest();
        when(sagemaker.createTrainingJob(request)).thenReturn(m_traningResult);
    }

    @Test
    public void testSageMakerXGBoost()
    {
        //String s3bucket, String s3key, String containerpath, String instancetype, int instancecount, int instancevolumesize, 
        //String maxdepth, String eta, String subsample, String evalmetric, String objective, String scaleposweight, String numround)
        Assert.assertNotNull(System.getenv("SAGEMAKER_ROLE_ARN"));
        System.out.println("****************************************" + System.getenv("SAGEMAKER_ROLE_ARN"));
        String output = m_athenaUDFHandler.trainxgboost("my-s3bucket", "my-train-dataset.csv", 
            "811284229777.dkr.ecr.us-east-1.amazonaws.com", "ml.c5.xlarge", 1, 30, "3","0.1","0.5","auc","binary:logistic","2.0","100");
        Assert.assertNotNull(output);
    }
}
