#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import { ElsaDataBeaconLambdaStack } from "../application/elsa-data-beacon-lambda-stack";

const app = new cdk.App();

new ElsaDataBeaconLambdaStack(app, "ElaDataBeaconLambdaStack", {
  // we absolutely only want this installed in UMCCR dev for the moment
  env: { account: "843407916570", region: "ap-southeast-2" },
});
