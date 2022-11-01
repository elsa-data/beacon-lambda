import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import {
  Architecture,
  Code,
  DockerImageCode,
  DockerImageFunction,
  Function,
  Runtime,
} from "aws-cdk-lib/aws-lambda";
import { RustFunction } from "rust.aws-cdk-lambda";
import {
  ManagedPolicy,
  PolicyStatement,
  Role,
  ServicePrincipal,
} from "aws-cdk-lib/aws-iam";
import { Duration } from "aws-cdk-lib";

export class ElsaDataBeaconLambdaStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const lambdaRole = new Role(this, "Role", {
      assumedBy: new ServicePrincipal("lambda.amazonaws.com"),
      description: "Lambda execution role for " + id,
    });

    const s3BucketPolicy = new PolicyStatement({
      actions: ["s3:List*", "s3:Get*"],
      resources: ["arn:aws:s3:::umccr-10*"],
    });

    lambdaRole.addManagedPolicy(
      ManagedPolicy.fromAwsManagedPolicyName(
        "service-role/AWSLambdaBasicExecutionRole"
      )
    );
    lambdaRole.addToPolicy(s3BucketPolicy);

    const beacon = new DockerImageFunction(this, "BeaconHandler", {
      code: DockerImageCode.fromImageAsset("application/lambda/beacon", {}),
      timeout: Duration.seconds(60),
      functionName: "elsa-data-beacon",
      architecture: Architecture.ARM_64,
      role: lambdaRole
    });
  }
}
