#!/usr/bin/env bash
# Deploy trade-flux-polygon to AWS Elastic Beanstalk (eu-west-1).
# Requires: AWS CLI, env vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION.
# Set DB_PASSWORD (and optionally other overrides) before running.

set -e
APP_NAME="trade-flux-polygon"
ENV_NAME="trade-flux-polygon-env"
REGION="${AWS_DEFAULT_REGION:-eu-west-1}"
S3_BUCKET="elasticbeanstalk-${REGION}-$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo 'YOUR_ACCOUNT')"
VERSION_LABEL="v-$(date +%Y%m%d%H%M%S)"

# Required: DB password for RDS
export DB_PASSWORD="${DB_PASSWORD:?Set DB_PASSWORD}"

echo "Building..."
mvn -q package -DskipTests

echo "Creating deployment zip..."
rm -f deploy.zip
zip -q deploy.zip Procfile
zip -q deploy.zip -j target/trade-flux-polygon-1.0.0-beanstalk.jar
zip -q deploy.zip -r .ebextensions

echo "Ensuring S3 bucket and app..."
aws s3 mb "s3://${S3_BUCKET}" --region "$REGION" 2>/dev/null || true
aws elasticbeanstalk create-application --application-name "$APP_NAME" --region "$REGION" 2>/dev/null || true

echo "Uploading $VERSION_LABEL..."
aws s3 cp deploy.zip "s3://${S3_BUCKET}/trade-flux-polygon/${VERSION_LABEL}.zip" --region "$REGION"
aws elasticbeanstalk create-application-version \
  --application-name "$APP_NAME" \
  --version-label "$VERSION_LABEL" \
  --source-bundle "S3Bucket=${S3_BUCKET},S3Key=trade-flux-polygon/${VERSION_LABEL}.zip" \
  --region "$REGION"

# Instance profile required for EC2 in the environment (run scripts/eb-ensure-instance-profile.sh once)
IAM_INSTANCE_PROFILE="${EB_INSTANCE_PROFILE:-aws-elasticbeanstalk-ec2-role}"

# Solution stack: Corretto 17 (no Tomcat) on Amazon Linux 2023
STACK=$(aws elasticbeanstalk list-available-solution-stacks --region "$REGION" --query "SolutionStacks[?contains(@, 'running Corretto 17') && !contains(@, 'Tomcat')] | [0]" --output text)
echo "Using stack: $STACK"

echo "Creating or updating environment..."
ENV_STATUS=$(aws elasticbeanstalk describe-environments --application-name "$APP_NAME" --environment-names "$ENV_NAME" --region "$REGION" --query "Environments[0].Status" --output text 2>/dev/null || echo "None")
if [ "$ENV_STATUS" = "Terminated" ] || [ "$ENV_STATUS" = "Terminating" ]; then
  echo "Environment $ENV_NAME exists but is $ENV_STATUS. Delete it in the AWS console and re-run to create a new one."
  exit 1
fi
if [ "$ENV_STATUS" = "None" ] || [ -z "$ENV_STATUS" ]; then
  aws elasticbeanstalk create-environment \
    --application-name "$APP_NAME" \
    --environment-name "$ENV_NAME" \
    --solution-stack-name "$STACK" \
    --version-label "$VERSION_LABEL" \
    --option-settings \
      "Namespace=aws:autoscaling:launchconfiguration,OptionName=IamInstanceProfile,Value=${IAM_INSTANCE_PROFILE}" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_HOST,Value=tradeflux.c1soc4wywjz6.eu-west-1.rds.amazonaws.com" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_NAME,Value=tradeflux" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_USER,Value=postgres" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_PASSWORD,Value=${DB_PASSWORD}" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=PORT,Value=5020" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=POLYGON_SUBGRAPH_START_DATE,Value=2026-03-12" \
    --region "$REGION"
else
  echo "Updating existing environment (status: $ENV_STATUS)..."
  aws elasticbeanstalk update-environment \
    --environment-name "$ENV_NAME" \
    --version-label "$VERSION_LABEL" \
    --option-settings \
      "Namespace=aws:autoscaling:launchconfiguration,OptionName=IamInstanceProfile,Value=${IAM_INSTANCE_PROFILE}" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_HOST,Value=tradeflux.c1soc4wywjz6.eu-west-1.rds.amazonaws.com" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_NAME,Value=tradeflux" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_USER,Value=postgres" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=DB_PASSWORD,Value=${DB_PASSWORD}" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=PORT,Value=5020" \
      "Namespace=aws:elasticbeanstalk:application:environment,OptionName=POLYGON_SUBGRAPH_START_DATE,Value=2026-03-12" \
    --region "$REGION"
fi

echo "Deploy triggered. Check status: aws elasticbeanstalk describe-environments --application-name $APP_NAME --environment-names $ENV_NAME --region $REGION"
rm -f deploy.zip
