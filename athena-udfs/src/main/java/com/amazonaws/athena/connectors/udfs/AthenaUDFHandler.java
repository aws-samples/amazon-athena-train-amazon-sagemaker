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

import com.amazonaws.athena.connector.lambda.handlers.UserDefinedFunctionHandler;
import com.google.common.annotations.VisibleForTesting;
import com.amazonaws.services.sagemaker.AmazonSageMaker;
import com.amazonaws.services.sagemaker.AmazonSageMakerClientBuilder;
import com.amazonaws.services.sagemaker.model.*;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AthenaUDFHandler
        extends UserDefinedFunctionHandler
{
    private static final String SOURCE_TYPE = "athena_xgboost_udf";
    private static final String TRAINING_MODE = "File";

    private static final String S3_DISTRIBUTION_TYPE = "ShardedByS3Key";
    private static final String S3_DATA_TYPE = "S3Prefix";
    private static final String S3_PREFIX = "s3://";

    private static final String TRAINING_CHANNEL_NAME = "train";
    private static final String TRAINING_CHANNEL_CONTENT_TYPE = "text/csv;label_size=0";

    private static final String MODEL_PREFIX = "/models/";

    private static final int TRAINING_JOB_MAX_RUNTIME_IN_MINUTES = 20;
    private static final String SAGEMAKER_ROLE_ARN = "SAGEMAKER_ROLE_ARN";
    private static final String ALGORITHM = "xgboost";
    
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    private String m_timeStamp;
    private String m_s3Bucket;
    private String m_s3Key;
    private String m_containerPath;
    private String m_instanceType;
    private int m_instanceCount;
    private int m_instanceVolumeSize;
    private String m_maxDepth;
    private String m_eta;
    private String m_subSample;
    private String m_evalMetric;
    private String m_objective;
    private String m_scalePosWeight;
    private String m_numRound;
    private final AmazonSageMaker m_sagemaker;

    public AthenaUDFHandler()
    {
        super(SOURCE_TYPE);
        m_sagemaker = AmazonSageMakerClientBuilder.standard().build();
    }

    @VisibleForTesting
    AthenaUDFHandler(AmazonSageMaker sagemaker,String s3bucket, String s3key, String containerpath, String instancetype, Integer instancecount, Integer instancevolumesize, 
        String maxdepth, String eta, String subsample, String evalmetric, String objective, String scaleposweight, String numround)
    {
        super(SOURCE_TYPE);
        m_sagemaker = sagemaker;
        m_timeStamp = sdf.format(new Timestamp(System.currentTimeMillis()));
        m_s3Bucket = s3bucket;
        m_s3Key = s3key;
        m_containerPath = containerpath;
        m_instanceType = instancetype;
        m_instanceCount = instancecount;
        m_instanceVolumeSize = instancevolumesize;
        m_maxDepth = maxdepth;
        m_eta = eta;
        m_subSample = subsample;
        m_evalMetric = evalmetric;
        m_objective = objective;
        m_scalePosWeight = scaleposweight;
        m_numRound = numround;
    }

    /**
     * IMPORTANT: Amazon Athena UDF function must have only lowercase and the method signature (name and argument types)
     **/
    public String trainxgboost(
        String s3bucket, String s3key, String containerpath, String instancetype, Integer instancecount, Integer instancevolumesize, 
        String maxdepth, String eta, String subsample, String evalmetric, String objective, String scaleposweight, String numround)
    {
        m_timeStamp = sdf.format(new Timestamp(System.currentTimeMillis()));
        m_s3Bucket = s3bucket;
        m_s3Key = s3key;
        m_containerPath = containerpath;
        m_instanceType = instancetype;
        m_instanceCount = instancecount;
        m_instanceVolumeSize = instancevolumesize;
        m_maxDepth = maxdepth;
        m_eta = eta;
        m_subSample = subsample;
        m_evalMetric = evalmetric;
        m_objective = objective;
        m_scalePosWeight = scaleposweight;
        m_numRound = numround;
        
        CreateTrainingJobRequest request = getTrainingJobRequest();
        log.info("Starting sagemaker training job: " + request.getTrainingJobName());
        log.info("Full training job request is: " + request);
        m_sagemaker.createTrainingJob(request);

        return request.toString();
        //return "running on the slim";
    }
    /*
    public String printVariables()
    {
        return m_timeStamp + "\n" + m_s3Bucket + "\n" + m_s3Key + "\n" + m_containerPath + "\n" + m_instanceType + "\n" + m_instanceCount + "\n" 
        + m_instanceVolumeSize + "\n" + m_maxDepth + "\n" + m_eta + "\n" + m_subSample + "\n" + m_evalMetric + "\n" + m_objective + "\n" 
        + m_scalePosWeight + "\n" + m_numRound + "\n";
    }
    */
    public CreateTrainingJobRequest getTrainingJobRequest() {
        return new CreateTrainingJobRequest()
            .withAlgorithmSpecification(getAlgorithmSpecification())
            .withHyperParameters(getHyperparameters())
            .withInputDataConfig(getTrainingChannel())
            .withOutputDataConfig(getOutputDataConfig())
            .withResourceConfig(getResourceConfig())
            .withRoleArn(getSagemakerRoleArn())
            .withStoppingCondition(getStoppingCondition())
            .withTrainingJobName(ALGORITHM + "-" + m_timeStamp);
    }
    private AlgorithmSpecification getAlgorithmSpecification() {
        return new AlgorithmSpecification()
            .withTrainingImage(m_containerPath + "/"+ ALGORITHM + ":1")
            .withTrainingInputMode(TRAINING_MODE);
    }
    private HashMap<String, String> getHyperparameters() {
        HashMap<String, String> hyperParameters = new HashMap<>();
        hyperParameters.put("max_depth", m_maxDepth);
        hyperParameters.put("eta", m_eta);
        hyperParameters.put("subsample", m_subSample);
        hyperParameters.put("eval_metric", m_evalMetric);
        hyperParameters.put("objective", m_objective);
        hyperParameters.put("scale_pos_weight", m_scalePosWeight);
        hyperParameters.put("num_round", m_numRound);
        return hyperParameters;
    }
    private Channel getTrainingChannel() {
        S3DataSource s3DataSource = new S3DataSource()
            .withS3DataDistributionType(S3_DISTRIBUTION_TYPE)
            .withS3DataType(S3_DATA_TYPE)
            .withS3Uri(S3_PREFIX + m_s3Bucket + "/" + m_s3Key);
        DataSource dataSource = new DataSource()
            .withS3DataSource(s3DataSource);
        return new Channel()
            .withChannelName(TRAINING_CHANNEL_NAME)
            .withContentType(TRAINING_CHANNEL_CONTENT_TYPE)
            .withDataSource(dataSource);
    }
    private OutputDataConfig getOutputDataConfig() {
        return new OutputDataConfig()
                .withS3OutputPath(S3_PREFIX + m_s3Bucket + MODEL_PREFIX);
    }
    private ResourceConfig getResourceConfig() {
        return new ResourceConfig()
                .withInstanceCount(m_instanceCount)
                .withInstanceType(m_instanceType)
                .withVolumeSizeInGB(m_instanceVolumeSize);
    }
    private String getSagemakerRoleArn() {
        return System.getenv(SAGEMAKER_ROLE_ARN);
    }
    private StoppingCondition getStoppingCondition() {
        int maxRuntimeInSeconds = (int) TimeUnit.MINUTES.toSeconds(TRAINING_JOB_MAX_RUNTIME_IN_MINUTES);
        return new StoppingCondition()
            .withMaxRuntimeInSeconds(maxRuntimeInSeconds);
    }
}
