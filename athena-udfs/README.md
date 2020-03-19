# Amazon Athena UDF Connector with Amazon SageMaker

This connector extends Amazon Athena's capability to integrate with Amazon SageMaker by adding customizable UDFs via Lambda.

**To enable this Preview feature you need to create an Athena workgroup named AmazonAthenaPreviewFunctionality and run any queries attempting to federate to this connector, use a UDF, or SageMaker inference from that workgroup.**

## Supported UDFs

1. "trainxgboost": Train a model with XGBoost

Example query:

`USING FUNCTION trainxgboost(s3bucket VARCHAR, s3key VARCHAR, containerpath VARCHAR, instancetype VARCHAR, instancecount int, instancevolumesize int, maxdepth VARCHAR, eta VARCHAR, subsample VARCHAR, evalmetric VARCHAR, objective VARCHAR, scaleposweight VARCHAR, numround VARCHAR) RETURNS VARCHAR TYPE LAMBDA_INVOKE WITH (lambda_name = 'athena-udf') SELECT trainxgboost('my-s3bucket', 'my-train-dataset.csv', '811284229777.dkr.ecr.us-east-1.amazonaws.com','ml.c5.xlarge',1,30,'3','0.1','0.5','auc','binary:logistic','2.0','100');`


### Deploying The Connector

To use this connector in your queries, navigate to AWS Serverless Application Repository and deploy a pre-built version of this connector. Alternatively, you can build and deploy this connector from source follow the below steps or use the more detailed tutorial in the athena-example module:

1. From the athena-federation-sdk dir, run `mvn clean install` if you haven't already.
2. From the athena-udfs dir, run `mvn clean install`.
3. From the athena-udfs dir, run  `../tools/publish.sh S3_BUCKET_NAME athena-udfs` to publish the connector to your private AWS Serverless Application Repository. The S3_BUCKET in the command is where a copy of the connector's code will be stored for Serverless Application Repository to retrieve it. This will allow users with permission to do so, the ability to deploy instances of the connector via 1-Click form. Then navigate to [Serverless Application Repository](https://aws.amazon.com/serverless/serverlessrepo)
4. Try using your UDF(s) in a query.
5. If tests run too long during compilation - run `mvn clean install -DskipTests`
6. Ensure to increment SemanticVersion in athena-udfs.yam file to update the application version in SAM repository. Also delete the cloudformation stack before deploying new version of SAM application.
## License

This project is licensed under the Apache-2.0 License.